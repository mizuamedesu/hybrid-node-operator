import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

# CRD定義
GROUP = "failover.k8s.io"
VERSION = "v1"
PLURAL = "nodefailovers"


class NodeFailoverPhase:
    PENDING = "Pending"
    CREATING = "Creating"
    ACTIVE = "Active"
    RECOVERING = "Recovering"
    DRAINING = "Draining"
    COMPLETED = "Completed"


class ConditionType:
    VM_CREATED = "VMCreated"
    NODE_JOINED = "NodeJoined"
    TAINT_APPLIED = "TaintApplied"
    ONPREM_RECOVERED = "OnPremRecovered"
    GAMESERVERS_DRAINED = "GameServersDrained"


class NodeFailoverCRD:
    """NodeFailover CRDを操作するヘルパークラス"""

    def __init__(self, custom_objects_api):
        self.api = custom_objects_api

    def create(self, node_name: str, labels: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """NodeFailoverリソースを作成"""
        try:
            body = {
                "apiVersion": f"{GROUP}/{VERSION}",
                "kind": "NodeFailover",
                "metadata": {
                    "name": node_name.lower(),
                },
                "spec": {
                    "onpremNodeName": node_name,
                    "targetNodeLabels": labels or {}
                },
                "status": {
                    "phase": NodeFailoverPhase.PENDING,
                    "failedAt": datetime.now(timezone.utc).isoformat(),
                    "vmCreationAttempts": 0,
                    "conditions": []
                }
            }

            result = self.api.create_cluster_custom_object(
                group=GROUP,
                version=VERSION,
                plural=PLURAL,
                body=body
            )

            logger.info(f"Created NodeFailover resource for {node_name}")
            return result

        except ApiException as e:
            if e.status == 409:
                logger.warning(f"NodeFailover resource for {node_name} already exists")
                return self.get(node_name)
            else:
                logger.error(f"Error creating NodeFailover resource: {e}")
                return None

    def get(self, node_name: str) -> Optional[Dict[str, Any]]:
        """NodeFailoverリソースを取得"""
        try:
            return self.api.get_cluster_custom_object(
                group=GROUP,
                version=VERSION,
                plural=PLURAL,
                name=node_name.lower()
            )
        except ApiException as e:
            if e.status == 404:
                return None
            else:
                logger.error(f"Error getting NodeFailover resource: {e}")
                return None

    def list_all(self) -> List[Dict[str, Any]]:
        """全てのNodeFailoverリソースを取得"""
        try:
            result = self.api.list_cluster_custom_object(
                group=GROUP,
                version=VERSION,
                plural=PLURAL
            )
            return result.get("items", [])
        except ApiException as e:
            logger.error(f"Error listing NodeFailover resources: {e}")
            return []

    def update_status(
        self,
        node_name: str,
        phase: Optional[str] = None,
        gcp_vm_name: Optional[str] = None,
        recovery_detected_at: Optional[str] = None,
        vm_creation_attempts: Optional[int] = None,
        last_error: Optional[str] = None
    ) -> bool:
        """Statusフィールドを更新"""
        try:
            resource = self.get(node_name)
            if not resource:
                logger.error(f"NodeFailover resource for {node_name} not found")
                return False

            status = resource.get("status", {})

            if phase is not None:
                status["phase"] = phase
            if gcp_vm_name is not None:
                status["gcpVmName"] = gcp_vm_name
            if recovery_detected_at is not None:
                status["recoveryDetectedAt"] = recovery_detected_at
            if vm_creation_attempts is not None:
                status["vmCreationAttempts"] = vm_creation_attempts
            if last_error is not None:
                status["lastError"] = last_error

            body = {
                "status": status
            }

            self.api.patch_cluster_custom_object_status(
                group=GROUP,
                version=VERSION,
                plural=PLURAL,
                name=node_name.lower(),
                body=body
            )

            logger.info(f"Updated NodeFailover status for {node_name}")
            return True

        except ApiException as e:
            logger.error(f"Error updating NodeFailover status: {e}")
            return False

    def set_condition(
        self,
        node_name: str,
        condition_type: str,
        status: str,
        reason: Optional[str] = None,
        message: Optional[str] = None
    ) -> bool:
        """Conditionを設定"""
        try:
            resource = self.get(node_name)
            if not resource:
                logger.error(f"NodeFailover resource for {node_name} not found")
                return False

            conditions = resource.get("status", {}).get("conditions", [])

            # 既存の同じタイプのconditionを削除
            conditions = [c for c in conditions if c.get("type") != condition_type]

            # 新しいconditionを追加
            new_condition = {
                "type": condition_type,
                "status": status,
                "lastTransitionTime": datetime.now(timezone.utc).isoformat()
            }
            if reason:
                new_condition["reason"] = reason
            if message:
                new_condition["message"] = message

            conditions.append(new_condition)

            body = {
                "status": {
                    "conditions": conditions
                }
            }

            self.api.patch_cluster_custom_object_status(
                group=GROUP,
                version=VERSION,
                plural=PLURAL,
                name=node_name.lower(),
                body=body
            )

            logger.info(f"Set condition {condition_type}={status} for {node_name}")
            return True

        except ApiException as e:
            logger.error(f"Error setting condition: {e}")
            return False

    def get_condition(self, node_name: str, condition_type: str) -> Optional[Dict[str, Any]]:
        """特定のConditionを取得"""
        resource = self.get(node_name)
        if not resource:
            return None

        conditions = resource.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition.get("type") == condition_type:
                return condition

        return None

    def delete(self, node_name: str) -> bool:
        """NodeFailoverリソースを削除"""
        try:
            self.api.delete_cluster_custom_object(
                group=GROUP,
                version=VERSION,
                plural=PLURAL,
                name=node_name.lower()
            )
            logger.info(f"Deleted NodeFailover resource for {node_name}")
            return True

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"NodeFailover resource for {node_name} not found (already deleted?)")
                return True
            else:
                logger.error(f"Error deleting NodeFailover resource: {e}")
                return False


def get_nodefailover_crd(custom_objects_api) -> NodeFailoverCRD:
    """NodeFailoverCRDインスタンスを取得"""
    return NodeFailoverCRD(custom_objects_api)
