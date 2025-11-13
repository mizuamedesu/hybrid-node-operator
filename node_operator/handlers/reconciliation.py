import logging
import asyncio
import os
import kopf

from node_operator.k8s.client import get_k8s_client
from node_operator.gcp.compute import get_gcp_client
from node_operator.crd.nodefailover import get_nodefailover_crd, NodeFailoverPhase, ConditionType

logger = logging.getLogger(__name__)

RECONCILIATION_INTERVAL = int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "60"))
GAMESERVER_MAX_WAIT_SECONDS = int(os.getenv("GAMESERVER_MAX_WAIT_HOURS", "3")) * 3600

_reconciliation_task = None


@kopf.on.startup()
async def on_startup(**kwargs):
    """Operator起動時の初期化処理"""
    logger.info("Operator starting up with CRD-based state management...")

    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    # 既存の状態を再構築
    await reconstruct_state(k8s_client, crd)

    # リコンシリエーションループを起動
    global _reconciliation_task
    _reconciliation_task = asyncio.create_task(_reconciliation_loop())
    logger.info("Started reconciliation background task")


@kopf.on.cleanup()
async def on_cleanup(**kwargs):
    """Operator終了処理"""
    logger.info("Operator shutting down gracefully")


async def reconstruct_state(k8s_client, crd):
    """起動時に既存のVMとNodeFailoverリソースの状態を確認"""
    logger.info("Reconstructing state from existing resources...")

    gcp_client = get_gcp_client()

    # オンプレノードを全て確認
    onprem_nodes = k8s_client.list_nodes_by_label("node-type=onpremise")
    logger.info(f"Found {len(onprem_nodes)} onprem nodes to check")

    for node in onprem_nodes:
        node_name = node.metadata.name
        is_ready = k8s_client.is_node_ready(node_name)

        logger.info(f"Checking onprem node {node_name}: ready={is_ready}")

        # 既存のNodeFailoverリソースを確認
        existing = crd.get(node_name)

        if existing:
            logger.info(f"Found existing NodeFailover resource for {node_name}")

            status = existing.get("status", {})
            phase = status.get("phase")
            gcp_vm_name = status.get("gcpVmName")

            # Phaseに応じた処理
            if phase == NodeFailoverPhase.COMPLETED:
                logger.info(f"NodeFailover for {node_name} is already completed, skipping")
                continue

            # GCP VMの存在確認
            if gcp_vm_name:
                if not gcp_client.instance_exists(gcp_vm_name):
                    logger.warning(f"GCP VM {gcp_vm_name} not found, but NodeFailover exists")
                else:
                    logger.info(f"GCP VM {gcp_vm_name} exists for {node_name}")

            # オンプレノードがNotReadyで、taintが未適用の場合
            if not is_ready and phase == NodeFailoverPhase.ACTIVE:
                taint_condition = crd.get_condition(node_name, ConditionType.TAINT_APPLIED)
                if not taint_condition or taint_condition.get("status") != "True":
                    logger.info(f"Applying out-of-service taint to {node_name} during reconstruction")
                    success = k8s_client.apply_out_of_service_taint(node_name)
                    if success:
                        crd.set_condition(node_name, ConditionType.TAINT_APPLIED, "True")

            # オンプレノードが復旧済みで、まだRecovering phaseでない場合
            if is_ready and phase not in [NodeFailoverPhase.RECOVERING, NodeFailoverPhase.DRAINING, NodeFailoverPhase.COMPLETED]:
                logger.info(f"Onprem node {node_name} is Ready, updating to Recovering phase")
                from datetime import datetime, timezone
                crd.update_status(
                    node_name,
                    phase=NodeFailoverPhase.RECOVERING,
                    recovery_detected_at=datetime.now(timezone.utc).isoformat()
                )
                crd.set_condition(node_name, ConditionType.ONPREM_RECOVERED, "True")

        else:
            # NodeFailoverリソースがないが、VMが存在する場合
            sanitized = node_name.lower().replace("_", "-")
            sanitized = "".join(c for c in sanitized if c.isalnum() or c == "-")
            if not sanitized[0].isalpha():
                sanitized = "node-" + sanitized
            vm_prefix = f"gcp-temp-{sanitized}"

            instances = gcp_client.list_instances()
            matching_vm = None
            for instance in instances:
                if instance.name.startswith(vm_prefix):
                    matching_vm = instance.name
                    break

            if matching_vm:
                logger.info(f"Found orphaned VM {matching_vm} for {node_name}, creating NodeFailover resource")

                # NodeFailoverリソースを作成
                copy_label_keys = os.getenv("GCP_NODE_COPY_LABELS", "").split(",")
                copy_label_keys = [key.strip() for key in copy_label_keys if key.strip()]
                all_labels = k8s_client.get_node_custom_labels(node_name)
                target_labels = {k: v for k, v in all_labels.items() if k in copy_label_keys}

                crd.create(node_name, labels=target_labels)

                # Statusを更新
                crd.update_status(node_name, phase=NodeFailoverPhase.ACTIVE, gcp_vm_name=matching_vm)
                crd.set_condition(node_name, ConditionType.VM_CREATED, "True")
                crd.set_condition(node_name, ConditionType.NODE_JOINED, "True")

                # NotReadyならtaintを適用
                if not is_ready:
                    logger.info(f"Applying out-of-service taint to NotReady node {node_name}")
                    success = k8s_client.apply_out_of_service_taint(node_name)
                    if success:
                        crd.set_condition(node_name, ConditionType.TAINT_APPLIED, "True")

            elif not is_ready:
                # VMもNodeFailoverリソースもないが、ノードはNotReady
                logger.info(f"NotReady node {node_name} without VM, scheduling failover")

                copy_label_keys = os.getenv("GCP_NODE_COPY_LABELS", "").split(",")
                copy_label_keys = [key.strip() for key in copy_label_keys if key.strip()]
                all_labels = k8s_client.get_node_custom_labels(node_name)
                target_labels = {k: v for k, v in all_labels.items() if k in copy_label_keys}

                crd.create(node_name, labels=target_labels)

    logger.info("State reconstruction complete")


async def _reconciliation_loop():
    """定期的なリコンシリエーション処理"""
    while True:
        try:
            logger.info("Running periodic reconciliation")

            k8s_client = get_k8s_client()
            gcp_client = get_gcp_client()
            crd = get_nodefailover_crd(k8s_client.custom_objects_api)

            await _cleanup_draining_vms(k8s_client, gcp_client, crd)

            logger.info("Periodic reconciliation completed")

        except Exception as e:
            logger.error(f"Error during reconciliation: {e}", exc_info=True)

        await asyncio.sleep(RECONCILIATION_INTERVAL)


async def _cleanup_draining_vms(k8s_client, gcp_client, crd):
    """Draining phaseのVMでGameServer数が0になったものを削除"""
    all_resources = crd.list_all()

    for resource in all_resources:
        status = resource.get("status", {})
        phase = status.get("phase")

        if phase != NodeFailoverPhase.DRAINING:
            continue

        spec = resource.get("spec", {})
        onprem_node_name = spec.get("onpremNodeName")
        gcp_vm_name = status.get("gcpVmName")

        if not gcp_vm_name:
            continue

        # GCPノードのGameServer数を確認
        gcp_node = k8s_client.get_node_by_name(gcp_vm_name)
        if not gcp_node:
            logger.info(f"Temporary node {gcp_vm_name} already removed from cluster")
            if gcp_client.instance_exists(gcp_vm_name):
                await gcp_client.delete_instance(gcp_vm_name)
            crd.update_status(onprem_node_name, phase=NodeFailoverPhase.COMPLETED)
            continue

        gameserver_count = k8s_client.count_gameserver_pods_on_node(gcp_vm_name)

        logger.debug(f"Temporary node {gcp_vm_name} has {gameserver_count} GameServer pods")

        if gameserver_count == 0:
            logger.info(f"Deleting temporary node and VM: {gcp_vm_name}")

            crd.set_condition(onprem_node_name, ConditionType.GAMESERVERS_DRAINED, "True")

            # ノードをdrain
            k8s_client.drain_node(gcp_vm_name)
            await asyncio.sleep(10)

            # K8sノードを削除
            k8s_client.delete_node(gcp_vm_name)

            # GCP VMを削除
            if gcp_client.instance_exists(gcp_vm_name):
                success = await gcp_client.delete_instance(gcp_vm_name)

                if success:
                    logger.info(f"Successfully deleted VM {gcp_vm_name}")
                    crd.update_status(onprem_node_name, phase=NodeFailoverPhase.COMPLETED)
                else:
                    logger.error(f"Failed to delete VM {gcp_vm_name}")
            else:
                logger.info(f"VM {gcp_vm_name} already deleted")
                crd.update_status(onprem_node_name, phase=NodeFailoverPhase.COMPLETED)

        else:
            # まだGameServerが残っている
            recovery_detected_at = status.get("recoveryDetectedAt")
            if recovery_detected_at:
                from datetime import datetime, timezone
                recovery_time = datetime.fromisoformat(recovery_detected_at.replace("Z", "+00:00"))
                elapsed = (datetime.now(timezone.utc) - recovery_time).total_seconds()

                if elapsed > GAMESERVER_MAX_WAIT_SECONDS:
                    logger.error(
                        f"Temporary node {gcp_vm_name} still has {gameserver_count} "
                        f"GameServer pods after {elapsed}s (max: {GAMESERVER_MAX_WAIT_SECONDS}s)"
                    )
                else:
                    logger.info(f"Waiting for {gameserver_count} GameServer(s) to drain from {gcp_vm_name}")
