import logging
import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Any
import kopf

from node_operator.k8s.client import get_k8s_client
from node_operator.k8s.token import get_token_generator
from node_operator.gcp.compute import get_gcp_client
from node_operator.gcp.cloud_init import generate_startup_script, generate_vm_name
from node_operator.crd.nodefailover import (
    get_nodefailover_crd,
    NodeFailoverPhase,
    ConditionType
)

logger = logging.getLogger(__name__)

NODE_FLAPPING_GRACE_SECONDS = int(os.getenv("NODE_FLAPPING_GRACE_SECONDS", "30"))
MAX_VM_CREATION_ATTEMPTS = int(os.getenv("MAX_VM_CREATION_ATTEMPTS", "3"))


@kopf.on.create("failover.operator.io", "v1", "nodefailovers")
async def on_nodefailover_create(spec, name, status, **kwargs):
    """NodeFailoverリソースが作成された時の処理"""
    logger.info(f"NodeFailover resource created: {name}")

    onprem_node_name = spec.get("onpremNodeName")
    target_labels = spec.get("targetNodeLabels", {})

    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    # まずgrace periodを待つ
    logger.info(f"Waiting {NODE_FLAPPING_GRACE_SECONDS}s before creating VM")
    await asyncio.sleep(NODE_FLAPPING_GRACE_SECONDS)

    # ノードがまだNotReadyか確認
    is_ready = k8s_client.is_node_ready(onprem_node_name)
    if is_ready:
        logger.info(f"Node {onprem_node_name} recovered during grace period, marking as completed")
        crd.update_status(onprem_node_name, phase=NodeFailoverPhase.COMPLETED)
        return

    # VM作成を開始
    await create_failover_vm(onprem_node_name, target_labels)


@kopf.on.field("failover.operator.io", "v1", "nodefailovers", field="status.phase")
async def on_phase_change(old, new, spec, name, **kwargs):
    """Phaseが変更された時の処理"""
    logger.info(f"NodeFailover {name} phase changed: {old} -> {new}")

    if new == NodeFailoverPhase.ACTIVE:
        # VMが稼働状態になった後、grace periodを待ってtaintを適用
        onprem_node_name = spec.get("onpremNodeName")
        await handle_active_phase(onprem_node_name)

    elif new == NodeFailoverPhase.RECOVERING:
        # オンプレノードが復旧した後、taintを削除してDraining phaseに移行
        onprem_node_name = spec.get("onpremNodeName")
        await handle_recovering_phase(onprem_node_name)


async def create_failover_vm(node_name: str, target_labels: Dict[str, str]):
    """フェイルオーバー用GCP VM作成"""
    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    resource = crd.get(node_name)
    if not resource:
        logger.error(f"NodeFailover resource not found for {node_name}")
        return

    status = resource.get("status", {})
    attempts = status.get("vmCreationAttempts", 0)

    if attempts >= MAX_VM_CREATION_ATTEMPTS:
        logger.error(f"Max VM creation attempts ({MAX_VM_CREATION_ATTEMPTS}) reached for node {node_name}")
        crd.update_status(
            node_name,
            last_error=f"Max attempts ({MAX_VM_CREATION_ATTEMPTS}) reached"
        )
        return

    # Attemptをインクリメント
    crd.update_status(node_name, vm_creation_attempts=attempts + 1, phase=NodeFailoverPhase.CREATING)

    try:
        gcp_client = get_gcp_client()

        # 既存VMをチェック（他のOperator Podが作成済みかもしれない）
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
                logger.info(f"Found existing VM {existing_vm} for node {node_name}")
                crd.update_status(node_name, gcp_vm_name=existing_vm)
                crd.set_condition(node_name, ConditionType.VM_CREATED, "True", message="VM already exists")
                # ノード参加待機タスクを起動
                asyncio.create_task(wait_for_node_join(existing_vm, node_name, target_labels))
                return

        vm_name = generate_vm_name(node_name)

        logger.info(f"Creating GCP VM for failed node {node_name}", extra={
            "node_name": node_name,
            "vm_name": vm_name,
            "attempt": attempts + 1
        })

        # Startup script生成
        copy_label_keys = os.getenv("GCP_NODE_COPY_LABELS", "").split(",")
        copy_label_keys = [key.strip() for key in copy_label_keys if key.strip()]

        all_labels = k8s_client.get_node_custom_labels(node_name)
        onprem_labels = {k: v for k, v in all_labels.items() if k in copy_label_keys}
        target_labels.update(onprem_labels)

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
            "created-at": resource.get("status", {}).get("failedAt", "")[:10].replace("-", "")
        }

        success = await gcp_client.create_instance(
            instance_name=vm_name,
            startup_script=startup_script,
            labels=labels
        )

        if success:
            logger.info(f"Successfully created VM {vm_name} for node {node_name}")
            crd.update_status(node_name, gcp_vm_name=vm_name)
            crd.set_condition(node_name, ConditionType.VM_CREATED, "True", message="VM created successfully")

            # ノード参加待機タスクを起動
            asyncio.create_task(wait_for_node_join(vm_name, node_name, target_labels))
        else:
            raise Exception("VM creation failed")

    except Exception as e:
        error_msg = f"Failed to create VM for node {node_name}: {e}"
        logger.error(error_msg, exc_info=True)

        crd.update_status(node_name, last_error=error_msg)
        crd.set_condition(
            node_name,
            ConditionType.VM_CREATED,
            "False",
            reason="CreationFailed",
            message=str(e)
        )

        # リトライ
        resource = crd.get(node_name)
        if resource:
            attempts = resource.get("status", {}).get("vmCreationAttempts", 0)
            if attempts < MAX_VM_CREATION_ATTEMPTS:
                delay = min(2 ** attempts * 60, 300)
                logger.info(f"Retrying VM creation in {delay}s...")
                await asyncio.sleep(delay)
                await create_failover_vm(node_name, target_labels)


async def wait_for_node_join(vm_name: str, onprem_node_name: str, labels: Dict[str, str]):
    """ノード参加待機、ラベル付与"""
    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    logger.info(f"Waiting for node {vm_name} to join the cluster...")

    joined = await k8s_client.wait_for_node_join(vm_name, timeout_seconds=300)

    if joined:
        node_labels = {
            "node-type": "gcp-temporary",
            "node-location": "gcp"
        }
        node_labels.update(labels)

        success = k8s_client.patch_node_labels(vm_name, node_labels)

        if success:
            logger.info(f"Successfully labeled node {vm_name}")
            crd.set_condition(onprem_node_name, ConditionType.NODE_JOINED, "True", message="Node joined and labeled")
            crd.update_status(onprem_node_name, phase=NodeFailoverPhase.ACTIVE)
        else:
            logger.error(f"Failed to label node {vm_name}")
            crd.set_condition(onprem_node_name, ConditionType.NODE_JOINED, "False", reason="LabelingFailed")
    else:
        logger.error(f"Node {vm_name} did not join within timeout")
        crd.set_condition(onprem_node_name, ConditionType.NODE_JOINED, "False", reason="JoinTimeout")


async def handle_active_phase(onprem_node_name: str):
    """ACTIVE phaseの処理: grace period後にtaintを適用"""
    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    grace_period = 300
    logger.info(f"Waiting {grace_period}s grace period for potential node recovery...")
    await asyncio.sleep(grace_period)

    # ノードがまだNotReadyか確認
    is_ready = k8s_client.is_node_ready(onprem_node_name)

    if is_ready:
        logger.info(f"Onprem node {onprem_node_name} recovered during grace period")
        crd.update_status(
            onprem_node_name,
            phase=NodeFailoverPhase.RECOVERING,
            recovery_detected_at=datetime.now(timezone.utc).isoformat()
        )
        crd.set_condition(onprem_node_name, ConditionType.ONPREM_RECOVERED, "True")
    else:
        logger.warning(f"Onprem node {onprem_node_name} still NotReady, applying out-of-service taint")

        success = k8s_client.apply_out_of_service_taint(onprem_node_name)

        if success:
            crd.set_condition(onprem_node_name, ConditionType.TAINT_APPLIED, "True", message="Out-of-service taint applied")
            logger.info(f"Applied out-of-service taint to {onprem_node_name}")
        else:
            logger.error(f"Failed to apply out-of-service taint to {onprem_node_name}")
            crd.set_condition(onprem_node_name, ConditionType.TAINT_APPLIED, "False", reason="TaintFailed")


async def handle_recovering_phase(onprem_node_name: str):
    """RECOVERING phaseの処理: taintを削除してDraining phaseへ"""
    k8s_client = get_k8s_client()
    crd = get_nodefailover_crd(k8s_client.custom_objects_api)

    # out-of-service taintを削除
    success = k8s_client.remove_node_taint(onprem_node_name, "node.kubernetes.io/out-of-service")
    if success:
        logger.info(f"Removed out-of-service taint from recovered node {onprem_node_name}")

    # Draining phaseに移行
    crd.update_status(onprem_node_name, phase=NodeFailoverPhase.DRAINING)
