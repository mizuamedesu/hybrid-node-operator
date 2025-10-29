import logging
import os
import asyncio
from datetime import datetime, timezone
import kopf

from operator.state import get_state_manager
from operator.k8s.client import get_k8s_client
from operator.gcp.compute import get_gcp_client

logger = logging.getLogger(__name__)

RECONCILIATION_INTERVAL = int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "60"))
ONPREM_RECOVERY_WAIT_SECONDS = int(os.getenv("ONPREM_RECOVERY_WAIT_MINUTES", "10")) * 60
GAMESERVER_MAX_WAIT_SECONDS = int(os.getenv("GAMESERVER_MAX_WAIT_HOURS", "3")) * 3600


@kopf.timer("", "v1", "nodes", interval=RECONCILIATION_INTERVAL, idle=30)
async def reconcile_failovers(**kwargs):
    """定期的なフェイルオーバー状態のReconciliation"""
    logger.debug("Running reconciliation")

    state_manager = get_state_manager()
    k8s_client = get_k8s_client()
    gcp_client = get_gcp_client()

    await _apply_taints_to_recovered_nodes(state_manager, k8s_client)
    await _cleanup_ready_vms(state_manager, k8s_client, gcp_client)


async def _apply_taints_to_recovered_nodes(state_manager, k8s_client):
    """復旧したオンプレノードに対応する臨時GCPノードにTaint適用"""
    nodes_ready_for_taint = state_manager.get_nodes_ready_for_taint(
        ONPREM_RECOVERY_WAIT_SECONDS
    )

    for node_name in nodes_ready_for_taint:
        state = state_manager.get_state(node_name)
        if not state or not state.gcp_vm_name:
            continue

        if not k8s_client.is_node_ready(node_name):
            logger.warning(f"Onprem node {node_name} is no longer Ready, skipping taint")
            state.recovery_detected_at = None
            continue

        logger.info(f"Applying taint to temporary node {state.gcp_vm_name}", extra={
            "onprem_node": node_name,
            "gcp_node": state.gcp_vm_name,
            "event": "applying_taint"
        })

        success = k8s_client.add_node_taint(
            node_name=state.gcp_vm_name,
            key="temporary-node",
            value="draining",
            effect="NoSchedule"
        )

        if success:
            state_manager.update_taint_applied(node_name)
            logger.info(f"Taint applied to {state.gcp_vm_name}", extra={
                "onprem_node": node_name,
                "gcp_node": state.gcp_vm_name,
                "event": "taint_applied"
            })
        else:
            logger.error(f"Failed to apply taint to {state.gcp_vm_name}")


async def _cleanup_ready_vms(state_manager, k8s_client, gcp_client):
    """GameServerが0になった臨時GCP VMを削除"""
    nodes_ready_for_cleanup = state_manager.get_nodes_ready_for_cleanup()

    for onprem_node_name in nodes_ready_for_cleanup:
        state = state_manager.get_state(onprem_node_name)
        if not state or not state.gcp_vm_name:
            continue

        gcp_node_name = state.gcp_vm_name

        gcp_node = k8s_client.get_node_by_name(gcp_node_name)
        if not gcp_node:
            logger.info(f"Temporary node {gcp_node_name} already removed from cluster")
            if gcp_client.instance_exists(gcp_node_name):
                gcp_client.delete_instance(gcp_node_name)
            state_manager.remove_node(onprem_node_name)
            continue

        gameserver_count = k8s_client.count_gameserver_pods_on_node(gcp_node_name)

        logger.debug(f"Temporary node {gcp_node_name} has {gameserver_count} GameServer pods")

        if gameserver_count == 0:
            logger.info(f"Deleting temporary node and VM: {gcp_node_name}", extra={
                "onprem_node": onprem_node_name,
                "gcp_node": gcp_node_name,
                "event": "deleting_temporary_vm"
            })

            k8s_client.drain_node(gcp_node_name)
            await asyncio.sleep(10)

            k8s_client.delete_node(gcp_node_name)

            success = gcp_client.delete_instance(gcp_node_name)

            if success:
                logger.info(f"Successfully deleted VM {gcp_node_name}", extra={
                    "onprem_node": onprem_node_name,
                    "gcp_node": gcp_node_name,
                    "event": "temporary_vm_deleted"
                })

                state_manager.remove_node(onprem_node_name)
            else:
                logger.error(f"Failed to delete VM {gcp_node_name}")

        else:
            if state.taint_applied:
                taint_time = datetime.fromisoformat(state.recovery_detected_at)
                elapsed = (datetime.now(timezone.utc) - taint_time).total_seconds()

                if elapsed > GAMESERVER_MAX_WAIT_SECONDS:
                    logger.error(
                        f"Temporary node {gcp_node_name} still has {gameserver_count} "
                        f"GameServer pods after {elapsed}s (max: {GAMESERVER_MAX_WAIT_SECONDS}s)",
                        extra={
                            "onprem_node": onprem_node_name,
                            "gcp_node": gcp_node_name,
                            "gameserver_count": gameserver_count,
                            "elapsed_seconds": elapsed,
                            "event": "gameserver_drain_timeout"
                        }
                    )
                else:
                    logger.info(f"Waiting for {gameserver_count} GameServer(s) to drain from {gcp_node_name}")


@kopf.on.startup()
async def on_startup(**kwargs):
    """Operator起動時の状態再構築"""
    logger.info("Operator starting up, reconstructing state...")

    state_manager = get_state_manager()
    k8s_client = get_k8s_client()
    gcp_client = get_gcp_client()

    onprem_nodes = k8s_client.list_nodes_by_label("node-type=onpremise")

    for node in onprem_nodes:
        node_name = node.metadata.name
        is_ready = k8s_client.is_node_ready(node_name)

        if not is_ready:
            logger.info(f"Found NotReady onprem node during startup: {node_name}")

            gcp_nodes = k8s_client.list_nodes_by_label("node-type=gcp-temporary")

            matching_vm = None
            for gcp_node in gcp_nodes:
                gcp_node_name = gcp_node.metadata.name
                if node_name.lower().replace("_", "-") in gcp_node_name:
                    matching_vm = gcp_node_name
                    break

            if matching_vm:
                logger.info(f"Found existing temporary VM {matching_vm} for {node_name}")

                state = state_manager.add_failed_node(node_name)
                state_manager.update_vm_created(node_name, matching_vm)

                gcp_node = k8s_client.get_node_by_name(matching_vm)
                if gcp_node and gcp_node.spec.taints:
                    for taint in gcp_node.spec.taints:
                        if taint.key == "temporary-node":
                            state_manager.update_taint_applied(node_name)
                            break

            else:
                logger.info(f"No existing VM found for {node_name}, will create one")

    logger.info("Startup state reconstruction complete")


@kopf.on.cleanup()
async def on_cleanup(**kwargs):
    """Operator終了処理"""
    logger.info("Operator shutting down gracefully")
