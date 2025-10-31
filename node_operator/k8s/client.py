import logging
from typing import Optional, List, Dict
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class KubernetesClient:

    def __init__(self):
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded Kubernetes configuration from kubeconfig")

        self.core_v1 = client.CoreV1Api()
        self.custom_objects_api = client.CustomObjectsApi()

    def is_node_ready(self, node_name: str) -> Optional[bool]:
        try:
            node = self.core_v1.read_node(node_name)

            if node.status and node.status.conditions:
                for condition in node.status.conditions:
                    if condition.type == "Ready":
                        return condition.status == "True"

            return False

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Node {node_name} not found")
                return None
            else:
                logger.error(f"Error checking node {node_name} status: {e}")
                raise

    def get_node_by_name(self, node_name: str) -> Optional[client.V1Node]:
        try:
            return self.core_v1.read_node(node_name)
        except ApiException as e:
            if e.status == 404:
                return None
            else:
                logger.error(f"Error getting node {node_name}: {e}")
                raise

    def list_nodes_by_label(self, label_selector: str) -> List[client.V1Node]:
        try:
            response = self.core_v1.list_node(label_selector=label_selector)
            return response.items
        except ApiException as e:
            logger.error(f"Error listing nodes with selector {label_selector}: {e}")
            raise

    def patch_node_labels(self, node_name: str, labels: Dict[str, str]) -> bool:
        try:
            body = {
                "metadata": {
                    "labels": labels
                }
            }
            self.core_v1.patch_node(node_name, body)
            logger.info(f"Updated labels on node {node_name}: {labels}")
            return True

        except ApiException as e:
            logger.error(f"Error patching labels on node {node_name}: {e}")
            return False

    def add_node_taint(
        self,
        node_name: str,
        key: str,
        value: str,
        effect: str
    ) -> bool:
        try:
            node = self.core_v1.read_node(node_name)

            taints = node.spec.taints or []

            for taint in taints:
                if taint.key == key:
                    logger.info(f"Taint {key} already exists on node {node_name}")
                    return True

            new_taint = client.V1Taint(
                key=key,
                value=value,
                effect=effect
            )
            taints.append(new_taint)

            body = {
                "spec": {
                    "taints": [
                        {
                            "key": t.key,
                            "value": t.value,
                            "effect": t.effect
                        }
                        for t in taints
                    ]
                }
            }
            self.core_v1.patch_node(node_name, body)

            logger.info(f"Added taint to node {node_name}", extra={
                "node": node_name,
                "taint_key": key,
                "taint_value": value,
                "effect": effect
            })
            return True

        except ApiException as e:
            logger.error(f"Error adding taint to node {node_name}: {e}")
            return False

    def drain_node(self, node_name: str) -> bool:
        """Cordon（スケジュール不可）に設定"""
        try:
            body = {
                "spec": {
                    "unschedulable": True
                }
            }
            self.core_v1.patch_node(node_name, body)
            logger.info(f"Cordoned node {node_name}")
            return True

        except ApiException as e:
            logger.error(f"Error draining node {node_name}: {e}")
            return False

    def delete_node(self, node_name: str) -> bool:
        try:
            self.core_v1.delete_node(node_name)
            logger.info(f"Deleted node {node_name} from cluster")
            return True

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Node {node_name} not found (already deleted?)")
                return True
            else:
                logger.error(f"Error deleting node {node_name}: {e}")
                return False

    def count_gameserver_pods_on_node(self, node_name: str) -> int:
        """指定ノード上のAllocated状態のGameServer数をカウント"""
        try:
            # GameServerカスタムリソースを全namespaceから取得
            gameservers = self.custom_objects_api.list_cluster_custom_object(
                group="agones.dev",
                version="v1",
                plural="gameservers"
            )

            allocated_count = 0
            total_count = 0

            for gs in gameservers.get("items", []):
                # このノード上のGameServerか確認
                gs_node_name = gs.get("status", {}).get("nodeName")
                if gs_node_name == node_name:
                    total_count += 1
                    # Allocated状態のみカウント
                    state = gs.get("status", {}).get("state")
                    if state == "Allocated":
                        allocated_count += 1

            logger.debug(
                f"Node {node_name} has {allocated_count} Allocated GameServers "
                f"(total: {total_count})"
            )
            return allocated_count

        except ApiException as e:
            logger.error(f"Error counting GameServers on node {node_name}: {e}")
            return 999

    async def wait_for_node_join(self, expected_node_name: str, timeout_seconds: int = 300) -> bool:
        """ノードのクラスタ参加を待機"""
        import asyncio

        elapsed = 0
        interval = 10

        while elapsed < timeout_seconds:
            node = self.get_node_by_name(expected_node_name)
            if node:
                logger.info(f"Node {expected_node_name} has joined the cluster")
                return True

            await asyncio.sleep(interval)
            elapsed += interval
            logger.debug(f"Waiting for node {expected_node_name} to join... ({elapsed}s)")

        logger.warning(f"Node {expected_node_name} did not join within {timeout_seconds}s")
        return False

    def get_ca_cert_hash(self) -> Optional[str]:
        """kubeadm join用のCA証明書ハッシュを取得"""
        try:
            import hashlib
            import base64
            import yaml
            from cryptography import x509
            from cryptography.hazmat.primitives import serialization

            cm = self.core_v1.read_namespaced_config_map(
                name="cluster-info",
                namespace="kube-public"
            )

            kubeconfig_data = cm.data.get("kubeconfig")
            if not kubeconfig_data:
                logger.error("No kubeconfig found in cluster-info ConfigMap")
                return None

            kubeconfig = yaml.safe_load(kubeconfig_data)

            ca_cert_b64 = kubeconfig["clusters"][0]["cluster"]["certificate-authority-data"]
            ca_cert_pem = base64.b64decode(ca_cert_b64)

            cert = x509.load_pem_x509_certificate(ca_cert_pem)
            public_key_der = cert.public_key().public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            ca_hash = hashlib.sha256(public_key_der).hexdigest()

            logger.info("Successfully calculated CA certificate hash")
            return ca_hash

        except Exception as e:
            logger.error(f"Error getting CA cert hash: {e}")
            return None

    def get_node_custom_labels(self, node_name: str) -> Dict[str, str]:
        """ノードのカスタムラベルを取得（K8sデフォルトラベルを除外）"""
        try:
            node = self.core_v1.read_node(node_name)
            if not node.metadata or not node.metadata.labels:
                return {}

            k8s_prefix_list = [
                "beta.kubernetes.io/",
                "kubernetes.io/",
                "node-role.kubernetes.io/",
                "node.kubernetes.io/"
            ]

            custom_labels = {}
            for key, value in node.metadata.labels.items():
                if not any(key.startswith(prefix) for prefix in k8s_prefix_list):
                    custom_labels[key] = value

            logger.debug(f"Custom labels for node {node_name}: {custom_labels}")
            return custom_labels

        except ApiException as e:
            logger.error(f"Error getting labels for node {node_name}: {e}")
            return {}


_k8s_client: Optional[KubernetesClient] = None


def get_k8s_client() -> KubernetesClient:
    global _k8s_client
    if _k8s_client is None:
        _k8s_client = KubernetesClient()
    return _k8s_client
