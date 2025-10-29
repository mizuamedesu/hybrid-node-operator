import logging
import asyncio
import os
from typing import Dict, Any
import kopf

from ..state import get_state_manager
from ..k8s.client import get_k8s_client
from ..k8s.token import get_token_generator
from ..gcp.compute import get_gcp_client
from ..gcp.cloud_init import generate_startup_script, generate_vm_name

logger = logging.getLogger(__name__)

NODE_FLAPPING_GRACE_SECONDS = int(os.getenv("NODE_FLAPPING_GRACE_SECONDS", "30"))
MAX_VM_CREATION_ATTEMPTS = int(os.getenv("MAX_VM_CREATION_ATTEMPTS", "3"))


@kopf.on.event("", "v1", "nodes", labels={"node-type": "onpremise"})
async def on_node_event(event: Dict[str, Any], **kwargs):
    node = event.get("object", {})
    node_name = node.get("metadata", {}).get("name")
    event_type = event.get("type")

    if not node_name:
        logger.warning("Received node event without node name")
        return

    is_ready = _check_node_ready(node)

    logger.debug(f"Node event received", extra={
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
                asyncio.create_task(_create_failover_vm(node_name))
            else:
                logger.info(f"Node {node_name} recovered during grace period, skipping VM creation")

    else:
        if current_state is not None and not current_state.recovery_detected_at:
            logger.info(f"Onprem node recovery detected: {node_name}", extra={
                "node_name": node_name,
                "event": "onprem_node_recovery_detected"
            })

            state_manager.update_recovery_detected(node_name)


def _check_node_ready(node: Dict[str, Any]) -> bool:
    status = node.get("status", {})
    conditions = status.get("conditions", [])

    for condition in conditions:
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"

    return False


async def _create_failover_vm(node_name: str):
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

    state_manager.increment_vm_creation_attempts(node_name)

    try:
        vm_name = generate_vm_name(node_name)

        logger.info(f"Creating GCP VM for failed node {node_name}", extra={
            "node_name": node_name,
            "vm_name": vm_name,
            "attempt": state.vm_creation_attempts
        })

        gcp_client = get_gcp_client()

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

        success = gcp_client.create_instance(
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
            asyncio.create_task(_wait_and_label_node(vm_name, onprem_labels))

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
            await _create_failover_vm(node_name)


async def _wait_and_label_node(vm_name: str, onprem_labels: Dict[str, str]):
    """ノード参加待機とラベル付与"""
    k8s_client = get_k8s_client()

    logger.info(f"Waiting for node {vm_name} to join the cluster...")

    joined = k8s_client.wait_for_node_join(vm_name, timeout_seconds=300)

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
    else:
        logger.error(f"Node {vm_name} did not join within timeout", extra={
            "vm_name": vm_name,
            "event": "node_join_timeout"
        })
