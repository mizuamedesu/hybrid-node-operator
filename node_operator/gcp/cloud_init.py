import logging

logger = logging.getLogger(__name__)


def generate_startup_script(
    api_server_endpoint: str,
    token: str,
    ca_cert_hash: str
) -> str:
    script = f"""#!/bin/bash
set -e

exec > >(tee /var/log/startup-script.log)
exec 2>&1

echo "Starting node setup at $(date)"

# GCEメタデータから情報を取得
echo "Fetching GCE metadata..."
PROJECT=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | cut -d'/' -f4)
INSTANCE_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)

echo "Instance info: $PROJECT/$ZONE/$INSTANCE_NAME"

# kubelet設定ディレクトリを作成
mkdir -p /var/lib/kubelet

# cloud-providerとprovider-idをkubelet起動オプションに設定
echo "Configuring kubelet with cloud-provider settings..."
cat > /etc/default/kubelet << EOF
KUBELET_EXTRA_ARGS="--cloud-provider=external --provider-id=gce://$PROJECT/$ZONE/$INSTANCE_NAME"
EOF

sleep 5

echo "Joining Kubernetes cluster..."
kubeadm join {api_server_endpoint} \\
  --token {token} \\
  --discovery-token-ca-cert-hash sha256:{ca_cert_hash}

sleep 10

if kubectl get nodes $(hostname) &> /dev/null; then
    echo "Node successfully joined the cluster"
else
    echo "WARNING: Node join may have failed, but continuing..."
fi

echo "Node setup completed at $(date)"
echo "SETUP_COMPLETE"
"""
    return script


def generate_vm_name(onprem_node_name: str) -> str:
    """VM名を生成（GCPの命名規則に準拠）"""
    import time

    sanitized = onprem_node_name.lower().replace("_", "-")
    sanitized = "".join(c for c in sanitized if c.isalnum() or c == "-")

    if not sanitized[0].isalpha():
        sanitized = "node-" + sanitized

    timestamp = str(int(time.time()))
    vm_name = f"gcp-temp-{sanitized}-{timestamp}"

    # GCP上限63文字
    if len(vm_name) > 63:
        max_prefix_len = 63 - len(timestamp) - 10
        vm_name = f"gcp-temp-{sanitized[:max_prefix_len]}-{timestamp}"

    return vm_name
