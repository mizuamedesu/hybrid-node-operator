import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class NodeFailoverState:
    onprem_node_name: str
    failed_at: str
    gcp_vm_name: Optional[str] = None
    gcp_vm_created: bool = False
    recovery_detected_at: Optional[str] = None
    taint_applied: bool = False
    taint_applied_at: Optional[str] = None
    vm_creation_attempts: int = 0
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


class StateManager:

    def __init__(self):
        self._failed_nodes: Dict[str, NodeFailoverState] = {}
        logger.info("StateManager initialized")

    def add_failed_node(self, node_name: str) -> NodeFailoverState:
        if node_name in self._failed_nodes:
            logger.warning(f"Node {node_name} already registered as failed")
            return self._failed_nodes[node_name]

        state = NodeFailoverState(
            onprem_node_name=node_name,
            failed_at=datetime.now(timezone.utc).isoformat()
        )
        self._failed_nodes[node_name] = state

        logger.info("Registered failed node", extra={
            "node_name": node_name,
            "failed_at": state.failed_at
        })

        return state

    def get_state(self, node_name: str) -> Optional[NodeFailoverState]:
        return self._failed_nodes.get(node_name)

    def update_vm_created(self, node_name: str, vm_name: str):
        state = self._failed_nodes.get(node_name)
        if not state:
            logger.error(f"Cannot update VM status: node {node_name} not found in state")
            return

        state.gcp_vm_name = vm_name
        state.gcp_vm_created = True

        logger.info("VM creation completed", extra={
            "node_name": node_name,
            "vm_name": vm_name
        })

    def update_recovery_detected(self, node_name: str):
        state = self._failed_nodes.get(node_name)
        if not state:
            logger.error(f"Cannot update recovery status: node {node_name} not found in state")
            return

        if state.recovery_detected_at is None:
            state.recovery_detected_at = datetime.now(timezone.utc).isoformat()
            logger.info("Onprem node recovery detected", extra={
                "node_name": node_name,
                "recovery_detected_at": state.recovery_detected_at
            })

    def update_taint_applied(self, node_name: str):
        state = self._failed_nodes.get(node_name)
        if not state:
            logger.error(f"Cannot update taint status: node {node_name} not found in state")
            return

        state.taint_applied = True
        state.taint_applied_at = datetime.now(timezone.utc).isoformat()
        logger.info("Taint applied to onprem node", extra={
            "node_name": node_name,
            "taint_applied_at": state.taint_applied_at
        })

    def increment_vm_creation_attempts(self, node_name: str):
        state = self._failed_nodes.get(node_name)
        if state:
            state.vm_creation_attempts += 1

    def set_error(self, node_name: str, error: str):
        state = self._failed_nodes.get(node_name)
        if state:
            state.last_error = error
            logger.error("Error recorded for node", extra={"node_name": node_name, "error": error})

    def remove_node(self, node_name: str):
        if node_name in self._failed_nodes:
            del self._failed_nodes[node_name]
            logger.info("Node removed from failover state", extra={"node_name": node_name})

    def get_all_failed_nodes(self) -> Dict[str, NodeFailoverState]:
        return self._failed_nodes.copy()

    def get_nodes_ready_for_taint(self, recovery_wait_seconds: int) -> list[str]:
        """復旧後の待機時間を経過したノードを取得"""
        ready = []
        now = datetime.now(timezone.utc)

        for node_name, state in self._failed_nodes.items():
            if (
                state.recovery_detected_at
                and not state.taint_applied
                and state.gcp_vm_created
            ):
                recovery_time = datetime.fromisoformat(state.recovery_detected_at)
                elapsed = (now - recovery_time).total_seconds()

                if elapsed >= recovery_wait_seconds:
                    ready.append(node_name)

        return ready

    def get_nodes_ready_for_cleanup(self) -> list[str]:
        """削除準備ができたノードを取得"""
        return [
            node_name
            for node_name, state in self._failed_nodes.items()
            if state.taint_applied and state.recovery_detected_at
        ]


_state_manager: Optional[StateManager] = None


def get_state_manager() -> StateManager:
    global _state_manager
    if _state_manager is None:
        _state_manager = StateManager()
    return _state_manager
