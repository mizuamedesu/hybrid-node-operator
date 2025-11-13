import logging
import time
from typing import Optional
from kubernetes import client
from kubernetes.client.rest import ApiException
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DistributedLock:
    """Kubernetes Leaseを使った分散ロック"""

    def __init__(self, k8s_core_v1: client.CoreV1Api, coordination_v1: client.CoordinationV1Api):
        self.core_v1 = k8s_core_v1
        self.coordination_v1 = coordination_v1
        self.namespace = "default"
        self.holder_identity = self._get_pod_name()

    def _get_pod_name(self) -> str:
        """現在のPod名を取得"""
        import os
        return os.getenv("HOSTNAME", "unknown-operator-pod")

    def acquire_lock(self, resource_name: str, timeout_seconds: int = 30) -> bool:
        """
        リソースに対するロックを取得

        Args:
            resource_name: ロック対象のリソース名（例: "vm-create-uc-k8s4p"）
            timeout_seconds: ロック取得試行のタイムアウト

        Returns:
            ロック取得成功時True、タイムアウト時False
        """
        lease_name = f"node-failover-lock-{resource_name}"
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                # 既存のLeaseを取得
                try:
                    lease = self.coordination_v1.read_namespaced_lease(
                        name=lease_name,
                        namespace=self.namespace
                    )

                    # Leaseが期限切れかチェック
                    if lease.spec.renew_time:
                        renew_time = lease.spec.renew_time
                        lease_duration = lease.spec.lease_duration_seconds or 15

                        elapsed = (datetime.now(timezone.utc) - renew_time).total_seconds()

                        if elapsed < lease_duration:
                            # まだ有効なLeaseが存在
                            if lease.spec.holder_identity == self.holder_identity:
                                logger.info(f"Already holding lock for {resource_name}")
                                return True
                            else:
                                logger.debug(f"Lock {lease_name} held by {lease.spec.holder_identity}")
                                time.sleep(1)
                                continue

                    # Leaseが期限切れなので更新して取得
                    lease.spec.holder_identity = self.holder_identity
                    lease.spec.acquire_time = datetime.now(timezone.utc)
                    lease.spec.renew_time = datetime.now(timezone.utc)
                    lease.spec.lease_duration_seconds = 15

                    self.coordination_v1.replace_namespaced_lease(
                        name=lease_name,
                        namespace=self.namespace,
                        body=lease
                    )

                    logger.info(f"Acquired lock for {resource_name}")
                    return True

                except ApiException as e:
                    if e.status == 404:
                        # Leaseが存在しないので作成
                        lease = client.V1Lease(
                            metadata=client.V1ObjectMeta(
                                name=lease_name,
                                namespace=self.namespace
                            ),
                            spec=client.V1LeaseSpec(
                                holder_identity=self.holder_identity,
                                acquire_time=datetime.now(timezone.utc),
                                renew_time=datetime.now(timezone.utc),
                                lease_duration_seconds=15
                            )
                        )

                        self.coordination_v1.create_namespaced_lease(
                            namespace=self.namespace,
                            body=lease
                        )

                        logger.info(f"Acquired lock for {resource_name}")
                        return True
                    else:
                        raise

            except ApiException as e:
                if e.status == 409:
                    # 競合発生、リトライ
                    logger.debug(f"Conflict acquiring lock for {resource_name}, retrying")
                    time.sleep(1)
                    continue
                else:
                    logger.error(f"Error acquiring lock for {resource_name}: {e}")
                    return False

            except Exception as e:
                logger.error(f"Unexpected error acquiring lock for {resource_name}: {e}")
                return False

        logger.warning(f"Timeout acquiring lock for {resource_name}")
        return False

    def release_lock(self, resource_name: str):
        """ロックを解放"""
        lease_name = f"node-failover-lock-{resource_name}"

        try:
            self.coordination_v1.delete_namespaced_lease(
                name=lease_name,
                namespace=self.namespace
            )
            logger.info(f"Released lock for {resource_name}")

        except ApiException as e:
            if e.status == 404:
                logger.debug(f"Lock {lease_name} already released")
            else:
                logger.error(f"Error releasing lock for {resource_name}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error releasing lock for {resource_name}: {e}")
