import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class TokenGenerator:

    def __init__(self, core_v1_api: client.CoreV1Api):
        self.core_v1 = core_v1_api

    def generate_token_id(self) -> str:
        return secrets.token_hex(3)

    def generate_token_secret(self) -> str:
        return secrets.token_hex(8)

    def create_bootstrap_token(self, ttl_seconds: int = 1800) -> Optional[str]:
        """Bootstrap tokenを作成"""
        token_id = self.generate_token_id()
        token_secret = self.generate_token_secret()
        token = f"{token_id}.{token_secret}"

        expiration = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        expiration_str = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")

        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=f"bootstrap-token-{token_id}",
                namespace="kube-system"
            ),
            type="bootstrap.kubernetes.io/token",
            string_data={
                "token-id": token_id,
                "token-secret": token_secret,
                "usage-bootstrap-authentication": "true",
                "usage-bootstrap-signing": "true",
                "auth-extra-groups": "system:bootstrappers:kubeadm:default-node-token",
                "expiration": expiration_str
            }
        )

        try:
            self.core_v1.create_namespaced_secret(
                namespace="kube-system",
                body=secret
            )

            logger.info("Created bootstrap token", extra={
                "token_id": token_id,
                "expiration": expiration_str,
                "ttl_seconds": ttl_seconds
            })

            return token

        except ApiException as e:
            logger.error(f"Failed to create bootstrap token: {e}")
            return None

    def delete_bootstrap_token(self, token_id: str) -> bool:
        secret_name = f"bootstrap-token-{token_id}"

        try:
            self.core_v1.delete_namespaced_secret(
                name=secret_name,
                namespace="kube-system"
            )

            logger.info(f"Deleted bootstrap token {token_id}")
            return True

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Bootstrap token {token_id} not found (already deleted?)")
                return True
            else:
                logger.error(f"Failed to delete bootstrap token {token_id}: {e}")
                return False

    def create_token_with_cleanup(self, ttl_seconds: int = 1800) -> Tuple[Optional[str], Optional[str]]:
        """Tokenとtoken_idを返す"""
        token = self.create_bootstrap_token(ttl_seconds)
        if token:
            token_id = token.split(".")[0]
            return token, token_id
        return None, None


_token_generator: Optional[TokenGenerator] = None


def get_token_generator(core_v1_api: Optional[client.CoreV1Api] = None) -> TokenGenerator:
    global _token_generator
    if _token_generator is None:
        if core_v1_api is None:
            from node_operator.k8s.client import get_k8s_client
            k8s_client = get_k8s_client()
            core_v1_api = k8s_client.core_v1
        _token_generator = TokenGenerator(core_v1_api)
    return _token_generator
