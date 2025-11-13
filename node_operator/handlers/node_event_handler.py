import logging
from typing import Dict, Any
import kopf

from node_operator.k8s.client import get_k8s_client
from node_operator.crd.nodefailover import get_nodefailover_crd

logger = logging.getLogger(__name__)


@kopf.on.event("", "v1", "nodes", labels={"node-type": "onpremise"})
async def on_node_event(event: Dict[str, Any], **kwargs):
    """オンプレノードのイベント監視: NotReady検出時にNodeFailoverリソースを作成"""
    node = event.get("object", {})
    node_name = node.get("metadata", {}).get("name")
    event_type = event.get("type")

    if not node_name:
        return

    is_ready = _check_node_ready(node)

    logger.debug(f"Node event: {node_name}, type={event_type}, ready={is_ready}")

    if not is_ready:
        # NotReadyノード検出
        k8s_client = get_k8s_client()
        crd = get_nodefailover_crd(k8s_client.custom_objects_api)

        # 既存のNodeFailoverリソースがあるか確認
        existing = crd.get(node_name)

        if existing:
            status = existing.get("status", {})
            phase = status.get("phase")

            if phase == "Completed":
                logger.info(f"Deleting completed NodeFailover resource for re-failover: {node_name}")
                crd.delete(node_name)
            else:
                logger.debug(f"NodeFailover resource already exists for {node_name} (phase: {phase})")
                return

        # 新規NodeFailoverリソースを作成
        logger.info(f"Creating NodeFailover resource for NotReady node {node_name}")

        # ノードからコピーするラベルを取得
        import os
        copy_label_keys = os.getenv("GCP_NODE_COPY_LABELS", "").split(",")
        copy_label_keys = [key.strip() for key in copy_label_keys if key.strip()]

        all_labels = k8s_client.get_node_custom_labels(node_name)
        target_labels = {k: v for k, v in all_labels.items() if k in copy_label_keys}

        crd.create(node_name, labels=target_labels)

    else:
        # Readyノード検出
        k8s_client = get_k8s_client()
        crd = get_nodefailover_crd(k8s_client.custom_objects_api)

        existing = crd.get(node_name)

        if existing:
            status = existing.get("status", {})
            phase = status.get("phase")

            # Recovering phaseでない場合のみ、復旧を検出
            if phase not in ["Recovering", "Draining", "Completed"]:
                logger.info(f"Onprem node recovery detected: {node_name}")

                from datetime import datetime, timezone
                crd.update_status(
                    node_name,
                    phase="Recovering",
                    recovery_detected_at=datetime.now(timezone.utc).isoformat()
                )

                from node_operator.crd.nodefailover import ConditionType
                crd.set_condition(node_name, ConditionType.ONPREM_RECOVERED, "True")


def _check_node_ready(node: Dict[str, Any]) -> bool:
    """ノードのReady状態をチェック"""
    status = node.get("status", {})
    conditions = status.get("conditions", [])

    for condition in conditions:
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"

    return False
