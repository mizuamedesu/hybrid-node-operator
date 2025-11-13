import logging
import asyncio
import os
from typing import Dict, Any
import kopf

from node_operator.state import get_state_manager
from node_operator.k8s.client import get_k8s_client
from node_operator.k8s.token import get_token_generator
from node_operator.k8s.lock import DistributedLock
from node_operator.gcp.compute import get_gcp_client
from node_operator.gcp.cloud_init import generate_startup_script, generate_vm_name

logger = logging.getLogger(__name__)

NODE_FLAPPING_GRACE_SECONDS = int(os.getenv("NODE_FLAPPING_GRACE_SECONDS", "30"))
MAX_VM_CREATION_ATTEMPTS = int(os.getenv("MAX_VM_CREATION_ATTEMPTS", "3"))


@kopf.on.event("", "v1", "nodes")
async def debug_all_node_events(event: Dict[str, Any], **kwargs):
    node = event.get("object", {})
    node_name = node.get("metadata", {}).get("name")
    event_type = event.get("type")
    labels = node.get("metadata", {}).get("labels", {})
    
    logger.info(f"DEBUG: All node event", extra={
        "node_name": node_name,
        "event_type": event_type,
        "labels": labels,
        "has_onpremise_label": "node-type" in labels and labels.get("node-type") == "onpremise"
    })


@kopf.on.event("", "v1", "nodes", labels={"node-type": "onpremise"})
async def on_node_event(event: Dict[str, Any], **kwargs):
    logger.info("NODE EVENT HANDLER TRIGGERED")
    
    node = event.get("object", {})
    node_name = node.get("metadata", {}).get("name")
    event_type = event.get("type")

    logger.info(f"Raw event data", extra={
        "event_type": event_type,
        "node_name": node_name,
        "node_labels": node.get("metadata", {}).get("labels", {}),
        "node_status": node.get("status", {}).get("conditions", [])
    })

    if not node_name:
        logger.warning("Received node event without node name")
        return

    is_ready = _check_node_ready(node)

    logger.info(f"Node event received", extra={
        "node_name": node_name,
        "event_type": event_type,
        "is_ready": is_ready
    })

    state_manager = get_state_manager()
    current_state = state_manager.get_state(node_name)

    if not is_ready:
        if current_state is None:
            logger.warning(f"Onprem node failure detected: {node_name}", extra={
                "node_name": node_name,
                "event": "onprem_node_failure_detected"
            })

            logger.info(f"Waiting {NODE_FLAPPING_GRACE_SECONDS}s before creating VM (anti-flapping)")
            await asyncio.sleep(NODE_FLAPPING_GRACE_SECONDS)

            k8s_client = get_k8s_client()
            still_not_ready = not k8s_client.is_node_ready(node_name)

            if still_not_ready:
                state_manager.add_failed_node(node_name)
                asyncio.create_task(create_failover_vm(node_name))
            else:
                logger.info(f"Node {node_name} recovered during grace period, skipping VM creation")
        else:
            # 既にstateが存在する場合
            if current_state.recovery_detected_at:
                # 一度復旧した後に再度NotReadyになった（真の再障害）
                logger.warning(f"Onprem node {node_name} failed again after recovery", extra={
                    "node_name": node_name,
                    "gcp_vm_name": current_state.gcp_vm_name,
                    "event": "onprem_node_re_failure"
                })

                # 古いVMが残っていれば削除
                if current_state.gcp_vm_name:
                    logger.info(f"Cleaning up old VM {current_state.gcp_vm_name}")
                    gcp_client = get_gcp_client()
                    k8s_client = get_k8s_client()

                    # Kubernetesノードを削除
                    if k8s_client.get_node_by_name(current_state.gcp_vm_name):
                        k8s_client.delete_node(current_state.gcp_vm_name)

                    # GCP VMを削除
                    if gcp_client.instance_exists(current_state.gcp_vm_name):
                        await gcp_client.delete_instance(current_state.gcp_vm_name)

                # stateをクリーンアップして新規VM作成
                state_manager.remove_node(node_name)

                logger.info(f"Waiting {NODE_FLAPPING_GRACE_SECONDS}s before creating new VM")
                await asyncio.sleep(NODE_FLAPPING_GRACE_SECONDS)

                k8s_client = get_k8s_client()
                still_not_ready = not k8s_client.is_node_ready(node_name)

                if still_not_ready:
                    state_manager.add_failed_node(node_name)
                    asyncio.create_task(create_failover_vm(node_name))
                else:
                    logger.info(f"Node {node_name} recovered during grace period, skipping VM creation")
            else:
                # VM作成中または作成直後（復旧検知前）の重複イベント
                logger.info(f"Ignoring duplicate NotReady event for {node_name} (VM already being created)")

    else:
        if current_state is not None and not current_state.recovery_detected_at:
            logger.info(f"Onprem node recovery detected: {node_name}", extra={
                "node_name": node_name,
                "event": "onprem_node_recovery_detected"
            })

            state_manager.update_recovery_detected(node_name)

            if current_state.taint_applied:
                k8s_client = get_k8s_client()
                success = k8s_client.remove_node_taint(node_name, "node.kubernetes.io/out-of-service")
                if success:
                    logger.info(f"Removed out-of-service taint from recovered node {node_name}")
                else:
                    logger.warning(f"Failed to remove out-of-service taint from {node_name}")


def _check_node_ready(node: Dict[str, Any]) -> bool:
    status = node.get("status", {})
    conditions = status.get("conditions", [])

    for condition in conditions:
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"

    return False


async def create_failover_vm(node_name: str):
    """フェイルオーバー用GCP VM作成"""
    state_manager = get_state_manager()
    state = state_manager.get_state(node_name)

    if not state:
        logger.error(f"Cannot create VM: no state found for node {node_name}")
        return

    if state.gcp_vm_created:
        logger.warning(f"VM already created for node {node_name}, skipping")
        return

    if state.vm_creation_attempts >= MAX_VM_CREATION_ATTEMPTS:
        logger.error(f"Max VM creation attempts ({MAX_VM_CREATION_ATTEMPTS}) reached for node {node_name}", extra={
            "node_name": node_name,
            "attempts": state.vm_creation_attempts
        })
        return

    # 分散ロックを取得（複数Operator Pod対策）
    k8s_client = get_k8s_client()
    lock = DistributedLock(k8s_client.core_v1, k8s_client.coordination_v1)
    lock_resource = f"vm-create-{node_name}"

    if not lock.acquire_lock(lock_resource, timeout_seconds=60):
        logger.warning(f"Could not acquire lock for VM creation of {node_name}, another operator may be handling it")
        return

    try:
        # ロック取得後、再度状態をチェック
        state = state_manager.get_state(node_name)
        if not state:
            logger.error(f"Cannot create VM: no state found for node {node_name}")
            return

        if state.gcp_vm_created:
            logger.info(f"VM already created for node {node_name} (detected after lock), skipping")
            return

        state_manager.increment_vm_creation_attempts(node_name)

        gcp_client = get_gcp_client()

        # VM作成前に、同じノード用の既存VMがないかチェック（追加の安全確認）
        sanitized_node_name = node_name.lower().replace("_", "-")
        sanitized_node_name = "".join(c for c in sanitized_node_name if c.isalnum() or c == "-")
        if not sanitized_node_name[0].isalpha():
            sanitized_node_name = "node-" + sanitized_node_name
        vm_prefix = f"gcp-temp-{sanitized_node_name}"

        instances = gcp_client.list_instances()
        existing_vm = None
        for instance in instances:
            if instance.name.startswith(vm_prefix):
                existing_vm = instance.name
                logger.info(f"Found existing VM {existing_vm} for node {node_name}, skipping creation")
                state_manager.update_vm_created(node_name, existing_vm)
                return

        vm_name = generate_vm_name(node_name)

        logger.info(f"Creating GCP VM for failed node {node_name}", extra={
            "node_name": node_name,
            "vm_name": vm_name,
            "attempt": state.vm_creation_attempts
        })

        if gcp_client.instance_exists(vm_name):
            logger.info(f"VM {vm_name} already exists, marking as created")
            state_manager.update_vm_created(node_name, vm_name)
            return

        k8s_client = get_k8s_client()

        copy_label_keys = os.getenv("GCP_NODE_COPY_LABELS", "").split(",")
        copy_label_keys = [key.strip() for key in copy_label_keys if key.strip()]

        all_labels = k8s_client.get_node_custom_labels(node_name)
        onprem_labels = {k: v for k, v in all_labels.items() if k in copy_label_keys}
        logger.info(f"Retrieved labels from node {node_name} to copy: {onprem_labels}")

        token_generator = get_token_generator(k8s_client.core_v1)

        token = token_generator.create_bootstrap_token(ttl_seconds=1800)
        if not token:
            raise Exception("Failed to create bootstrap token")

        ca_hash = k8s_client.get_ca_cert_hash()
        if not ca_hash:
            raise Exception("Failed to get CA certificate hash")

        api_server = os.getenv("K8S_API_SERVER")
        if not api_server:
            raise Exception("K8S_API_SERVER environment variable not set")

        startup_script = generate_startup_script(
            api_server_endpoint=api_server,
            token=token,
            ca_cert_hash=ca_hash
        )

        labels = {
            "onprem-node": node_name.lower().replace("_", "-"),
            "created-at": str(int(state.failed_at.timestamp()) if hasattr(state.failed_at, 'timestamp') else 0)
        }

        success = await gcp_client.create_instance(
            instance_name=vm_name,
            startup_script=startup_script,
            labels=labels
        )

        if success:
            logger.info(f"Successfully created VM {vm_name} for node {node_name}", extra={
                "node_name": node_name,
                "vm_name": vm_name,
                "event": "gcp_vm_created"
            })

            state_manager.update_vm_created(node_name, vm_name)
            asyncio.create_task(_wait_and_label_node(vm_name, node_name, onprem_labels))
        else:
            raise Exception("VM creation failed (check GCP API logs)")

    except Exception as e:
        error_msg = f"Failed to create VM for node {node_name}: {e}"
        logger.error(error_msg, extra={
            "node_name": node_name,
            "error": str(e),
            "event": "gcp_vm_creation_failed"
        })

        state_manager.set_error(node_name, error_msg)

        if state.vm_creation_attempts < MAX_VM_CREATION_ATTEMPTS:
            delay = min(2 ** state.vm_creation_attempts * 60, 300)
            logger.info(f"Retrying VM creation in {delay}s...")
            await asyncio.sleep(delay)
            await create_failover_vm(node_name)

    finally:
        # VM作成処理終了時にロックを解放
        lock.release_lock(lock_resource)


async def _wait_and_label_node(vm_name: str, onprem_node_name: str, onprem_labels: Dict[str, str]):
    """ノード参加待機、ラベル付与、out-of-service taint付与"""
    k8s_client = get_k8s_client()
    state_manager = get_state_manager()

    logger.info(f"Waiting for node {vm_name} to join the cluster...")

    joined = await k8s_client.wait_for_node_join(vm_name, timeout_seconds=300)

    if joined:
        labels = {
            "node-type": "gcp-temporary",
            "node-location": "gcp"
        }

        labels.update(onprem_labels)

        success = k8s_client.patch_node_labels(vm_name, labels)

        if success:
            logger.info(f"Successfully labeled node {vm_name}", extra={
                "node_name": vm_name,
                "labels": labels
            })
        else:
            logger.error(f"Failed to label node {vm_name}")

        grace_period = 300
        logger.info(f"Waiting {grace_period}s grace period for potential node recovery...")
        await asyncio.sleep(grace_period)

        onprem_still_not_ready = not k8s_client.is_node_ready(onprem_node_name)

        if onprem_still_not_ready:
            logger.warning(f"Onprem node {onprem_node_name} still NotReady after grace period, applying out-of-service taint")

            success = k8s_client.apply_out_of_service_taint(onprem_node_name)

            if success:
                state_manager.update_taint_applied(onprem_node_name)
                logger.info(f"Applied out-of-service taint to {onprem_node_name}", extra={
                    "onprem_node": onprem_node_name,
                    "gcp_node": vm_name,
                    "event": "out_of_service_taint_applied"
                })

                await asyncio.sleep(60)

                gameserver_count = k8s_client.count_gameserver_pods_on_node(onprem_node_name)
                logger.info(f"GameServers remaining on {onprem_node_name}: {gameserver_count}")
            else:
                logger.error(f"Failed to apply out-of-service taint to {onprem_node_name}")
        else:
            logger.info(f"Onprem node {onprem_node_name} recovered during grace period, skipping taint")

    else:
        logger.error(f"Node {vm_name} did not join within timeout", extra={
            "vm_name": vm_name,
            "event": "node_join_timeout"
        })

        logger.warning(f"Deleting failed VM {vm_name}")
        gcp_client = get_gcp_client()
        if gcp_client.instance_exists(vm_name):
            if await gcp_client.delete_instance(vm_name):
                logger.info(f"Deleted failed VM {vm_name}")
            else:
                logger.error(f"Failed to delete VM {vm_name}")
        else:
            logger.info(f"VM {vm_name} already deleted")
