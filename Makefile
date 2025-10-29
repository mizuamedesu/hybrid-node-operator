.PHONY: help build push deploy test clean

# Variables
IMAGE_NAME ?= node-failover-operator
IMAGE_TAG ?= latest
REGISTRY ?= your-registry
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

help:
	@echo "Available targets:"
	@echo "  build      - Build Docker image"
	@echo "  push       - Push Docker image to registry"
	@echo "  deploy     - Deploy to Kubernetes"
	@echo "  test       - Run unit tests"
	@echo "  clean      - Clean up Python cache files"
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY   - Container registry (default: your-registry)"
	@echo "  IMAGE_TAG  - Image tag (default: latest)"

build:
	@echo "Building Docker image: $(FULL_IMAGE)"
	docker build -t $(FULL_IMAGE) .

push: build
	@echo "Pushing Docker image: $(FULL_IMAGE)"
	docker push $(FULL_IMAGE)

deploy:
	@echo "Deploying to Kubernetes..."
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/configmap.yaml
	kubectl apply -f deploy/deployment.yaml
	@echo "Waiting for deployment to be ready..."
	kubectl wait --for=condition=available --timeout=300s deployment/node-failover-operator

test:
	@echo "Running unit tests..."
	pytest tests/

clean:
	@echo "Cleaning Python cache files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

logs:
	@echo "Tailing operator logs..."
	kubectl logs -f -l app=node-failover-operator

status:
	@echo "Operator Status:"
	kubectl get pods -l app=node-failover-operator
	@echo ""
	@echo "Temporary GCP Nodes:"
	kubectl get nodes -l node-type=gcp-temporary
	@echo ""
	@echo "GCP VMs managed by operator:"
	gcloud compute instances list --filter="labels.managed-by=node-failover-operator"
