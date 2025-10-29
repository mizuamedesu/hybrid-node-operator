import logging
import time
from typing import Optional, Dict
from google.cloud import compute_v1
from google.api_core import exceptions as google_exceptions

logger = logging.getLogger(__name__)


class GCPComputeClient:

    def __init__(
        self,
        project_id: str,
        zone: str,
        machine_type: str,
        network: str,
        subnet: str,
        image_project: str,
        image_name: str
    ):
        self.project_id = project_id
        self.zone = zone
        self.machine_type = machine_type
        self.network = network
        self.subnet = subnet
        self.image_project = image_project
        self.image_name = image_name

        self.instances_client = compute_v1.InstancesClient()
        self.operations_client = compute_v1.ZoneOperationsClient()
        self.region = "-".join(zone.split("-")[:-1])

        logger.info("GCP Compute client initialized", extra={
            "project_id": project_id,
            "zone": zone,
            "machine_type": machine_type
        })

    def create_instance(
        self,
        instance_name: str,
        startup_script: str,
        labels: Optional[Dict[str, str]] = None
    ) -> bool:
        try:
            instance = compute_v1.Instance()
            instance.name = instance_name
            instance.machine_type = f"zones/{self.zone}/machineTypes/{self.machine_type}"

            # Boot disk設定
            disk = compute_v1.AttachedDisk()
            disk.boot = True
            disk.auto_delete = True
            initialize_params = compute_v1.AttachedDiskInitializeParams()
            initialize_params.source_image = (
                f"projects/{self.image_project}/global/images/{self.image_name}"
            )
            initialize_params.disk_size_gb = 40
            initialize_params.disk_type = f"zones/{self.zone}/diskTypes/pd-ssd"
            disk.initialize_params = initialize_params
            instance.disks = [disk]

            # Network設定
            network_interface = compute_v1.NetworkInterface()
            network_interface.network = f"projects/{self.project_id}/global/networks/{self.network}"
            network_interface.subnetwork = (
                f"projects/{self.project_id}/regions/{self.region}/subnetworks/{self.subnet}"
            )
            instance.network_interfaces = [network_interface]

            # Startup script
            metadata = compute_v1.Metadata()
            metadata_item = compute_v1.Items()
            metadata_item.key = "startup-script"
            metadata_item.value = startup_script
            metadata.items = [metadata_item]
            instance.metadata = metadata

            # Nested virtualization有効化（Kata Containers用）
            advanced_machine_features = compute_v1.AdvancedMachineFeatures()
            advanced_machine_features.enable_nested_virtualization = True
            instance.advanced_machine_features = advanced_machine_features

            # ラベル設定
            instance.labels = labels or {}
            instance.labels["managed-by"] = "node-failover-operator"

            logger.info(f"Creating GCP instance {instance_name}", extra={
                "instance_name": instance_name,
                "zone": self.zone,
                "machine_type": self.machine_type
            })

            request = compute_v1.InsertInstanceRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.instance_resource = instance

            operation = self.instances_client.insert(request=request)
            self._wait_for_operation(operation.name)

            logger.info(f"Successfully created instance {instance_name}")
            return True

        except google_exceptions.GoogleAPIError as e:
            logger.error(f"Failed to create instance {instance_name}: {e}", extra={
                "instance_name": instance_name,
                "error": str(e)
            })
            return False

    def delete_instance(self, instance_name: str) -> bool:
        try:
            logger.info(f"Deleting GCP instance {instance_name}", extra={"instance_name": instance_name})

            request = compute_v1.DeleteInstanceRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.instance = instance_name

            operation = self.instances_client.delete(request=request)
            self._wait_for_operation(operation.name)

            logger.info(f"Successfully deleted instance {instance_name}")
            return True

        except google_exceptions.NotFound:
            logger.warning(f"Instance {instance_name} not found (already deleted?)")
            return True

        except google_exceptions.GoogleAPIError as e:
            logger.error(f"Failed to delete instance {instance_name}: {e}", extra={
                "instance_name": instance_name,
                "error": str(e)
            })
            return False

    def instance_exists(self, instance_name: str) -> bool:
        try:
            request = compute_v1.GetInstanceRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.instance = instance_name

            self.instances_client.get(request=request)
            return True

        except google_exceptions.NotFound:
            return False

        except google_exceptions.GoogleAPIError as e:
            logger.error(f"Error checking instance {instance_name}: {e}")
            return False

    def get_instance_status(self, instance_name: str) -> Optional[str]:
        try:
            request = compute_v1.GetInstanceRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.instance = instance_name

            instance = self.instances_client.get(request=request)
            return instance.status

        except google_exceptions.NotFound:
            return None

        except google_exceptions.GoogleAPIError as e:
            logger.error(f"Error getting instance {instance_name} status: {e}")
            return None

    def _wait_for_operation(self, operation_name: str, timeout: int = 300):
        start_time = time.time()

        while True:
            request = compute_v1.GetZoneOperationRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.operation = operation_name

            operation = self.operations_client.get(request=request)

            if operation.status == compute_v1.Operation.Status.DONE:
                if operation.error:
                    errors = ", ".join([e.message for e in operation.error.errors])
                    raise RuntimeError(f"Operation failed: {errors}")
                return

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Operation {operation_name} timed out after {timeout}s")

            time.sleep(2)

    def list_managed_instances(self) -> list[str]:
        try:
            request = compute_v1.ListInstancesRequest()
            request.project = self.project_id
            request.zone = self.zone
            request.filter = "labels.managed-by=node-failover-operator"

            instances = self.instances_client.list(request=request)
            return [instance.name for instance in instances]

        except google_exceptions.GoogleAPIError as e:
            logger.error(f"Error listing managed instances: {e}")
            return []


_gcp_client: Optional[GCPComputeClient] = None


def get_gcp_client(
    project_id: Optional[str] = None,
    zone: Optional[str] = None,
    machine_type: Optional[str] = None,
    network: Optional[str] = None,
    subnet: Optional[str] = None,
    image_project: Optional[str] = None,
    image_name: Optional[str] = None
) -> GCPComputeClient:
    import os
    global _gcp_client
    if _gcp_client is None:
        project_id = project_id or os.getenv("GCP_PROJECT_ID")
        zone = zone or os.getenv("GCP_ZONE")
        machine_type = machine_type or os.getenv("GCP_MACHINE_TYPE", "n2-standard-4")
        network = network or os.getenv("GCP_NETWORK")
        subnet = subnet or os.getenv("GCP_SUBNET")
        image_project = image_project or os.getenv("GCP_IMAGE_PROJECT")
        image_name = image_name or os.getenv("GCP_IMAGE_NAME")

        if not all([project_id, zone, machine_type, network, subnet, image_project, image_name]):
            raise ValueError("All parameters required when initializing GCPComputeClient")

        _gcp_client = GCPComputeClient(
            project_id=project_id,
            zone=zone,
            machine_type=machine_type,
            network=network,
            subnet=subnet,
            image_project=image_project,
            image_name=image_name
        )
    return _gcp_client
