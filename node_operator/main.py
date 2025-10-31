import logging
import os
import sys
from pythonjsonlogger import jsonlogger
import kopf


def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.handlers = []

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={
            "asctime": "timestamp",
            "levelname": "level",
            "name": "logger"
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    kopf_logger = logging.getLogger("kopf")
    kopf_logger.setLevel(logging.INFO)

    logging.info("Logging configured", extra={"log_level": log_level})


def validate_environment():
    required_vars = [
        "GCP_PROJECT_ID",
        "GCP_ZONE",
        "GCP_MACHINE_TYPE",
        "GCP_NETWORK",
        "GCP_SUBNET",
        "GCP_IMAGE_PROJECT",
        "GCP_IMAGE_NAME",
        "K8S_API_SERVER"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        logging.error("Missing required environment variables", extra={"missing_vars": missing})
        sys.exit(1)

    logging.info("Environment validation passed")


def initialize_clients():
    from node_operator.gcp.compute import get_gcp_client
    from node_operator.k8s.client import get_k8s_client

    k8s_client = get_k8s_client()
    logging.info("Kubernetes client initialized")

    gcp_client = get_gcp_client(
        project_id=os.getenv("GCP_PROJECT_ID"),
        zone=os.getenv("GCP_ZONE"),
        machine_type=os.getenv("GCP_MACHINE_TYPE", "n2-standard-4"),
        network=os.getenv("GCP_NETWORK"),
        subnet=os.getenv("GCP_SUBNET"),
        image_project=os.getenv("GCP_IMAGE_PROJECT"),
        image_name=os.getenv("GCP_IMAGE_NAME")
    )
    logging.info("GCP Compute client initialized")

    return k8s_client, gcp_client


from node_operator.handlers import node_watcher, reconciler


@kopf.on.login()
def login(**kwargs):
    return kopf.login_via_client(**kwargs)


def main():
    setup_logging()

    logging.info("Node Failover Operator starting", extra={
        "version": "1.0.0",
        "event": "operator_starting"
    })

    validate_environment()
    initialize_clients()

    logging.info("Node Failover Operator initialization complete")

    kopf.run(clusterwide=True, liveness_endpoint="http://0.0.0.0:8080/healthz")

if __name__ == "__main__":
    main()
