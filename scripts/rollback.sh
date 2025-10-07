#!/bin/bash
# Carbon Ingestion Platform Rollback Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DEPLOYMENT_MODE=${1:-"docker-compose"}
NAMESPACE="carbon-ingestion"

echo -e "${YELLOW}=== Carbon Ingestion Platform Rollback ===${NC}"
echo "Mode: $DEPLOYMENT_MODE"
echo ""

# Function to rollback Docker Compose deployment
rollback_docker_compose() {
    echo -e "${YELLOW}Rolling back Docker Compose deployment...${NC}"

    # Stop all services
    echo "Stopping application services..."
    docker-compose -f docker-compose.services.yml down

    echo "Stopping observability stack..."
    docker-compose -f docker-compose.observability.yml down

    # Keep infrastructure running (Kafka, ClickHouse, etc.)
    echo -e "${YELLOW}Note: Infrastructure services (Kafka, ClickHouse, Redis) are still running${NC}"
    echo "To stop infrastructure: docker-compose -f docker-compose.infra.yml down"

    echo -e "${GREEN}✓ Docker Compose rollback completed${NC}"
    echo ""
}

# Function to rollback Kubernetes deployment
rollback_kubernetes() {
    echo -e "${YELLOW}Rolling back Kubernetes deployment...${NC}"

    # Check if previous deployment exists
    if ! kubectl get namespace $NAMESPACE &> /dev/null; then
        echo -e "${RED}Error: Namespace $NAMESPACE not found${NC}"
        exit 1
    fi

    # Scale down deployments to zero
    echo "Scaling down deployments..."
    kubectl scale deployment --all --replicas=0 -n $NAMESPACE

    # Wait for pods to terminate
    echo "Waiting for pods to terminate..."
    kubectl wait --for=delete pod --all --timeout=120s -n $NAMESPACE || true

    # Option to delete resources completely
    read -p "Delete all resources in namespace $NAMESPACE? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Deleting all resources..."
        kubectl delete all --all -n $NAMESPACE
        echo -e "${GREEN}✓ All resources deleted${NC}"
    else
        echo -e "${YELLOW}Resources scaled to zero but not deleted${NC}"
        echo "To restore: kubectl scale deployment --all --replicas=3 -n $NAMESPACE"
    fi

    echo -e "${GREEN}✓ Kubernetes rollback completed${NC}"
    echo ""
}

# Function to backup current state
backup_current_state() {
    echo -e "${YELLOW}Backing up current state...${NC}"

    BACKUP_DIR="backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        # Backup Docker Compose state
        docker-compose -f docker-compose.services.yml config > "$BACKUP_DIR/docker-compose-services.yml"
        echo -e "${GREEN}✓ Docker Compose configuration backed up to $BACKUP_DIR${NC}"
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        # Backup Kubernetes resources
        kubectl get all -n $NAMESPACE -o yaml > "$BACKUP_DIR/kubernetes-resources.yaml"
        echo -e "${GREEN}✓ Kubernetes resources backed up to $BACKUP_DIR${NC}"
    fi

    echo ""
}

# Function to verify rollback
verify_rollback() {
    echo -e "${YELLOW}Verifying rollback...${NC}"

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        # Check if services are stopped
        RUNNING_SERVICES=$(docker-compose -f docker-compose.services.yml ps --services --filter "status=running" | wc -l)

        if [ "$RUNNING_SERVICES" -eq 0 ]; then
            echo -e "${GREEN}✓ All services stopped${NC}"
        else
            echo -e "${RED}✗ Some services are still running${NC}"
            docker-compose -f docker-compose.services.yml ps
        fi
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        # Check if pods are terminated
        RUNNING_PODS=$(kubectl get pods -n $NAMESPACE --field-selector=status.phase=Running 2>/dev/null | wc -l)

        if [ "$RUNNING_PODS" -le 1 ]; then  # Header line counts as 1
            echo -e "${GREEN}✓ All pods terminated${NC}"
        else
            echo -e "${RED}✗ Some pods are still running${NC}"
            kubectl get pods -n $NAMESPACE
        fi
    fi

    echo ""
}

# Main rollback flow
main() {
    # Confirm rollback
    echo -e "${RED}WARNING: This will rollback the deployment${NC}"
    read -p "Are you sure you want to proceed? (y/N): " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Rollback cancelled"
        exit 0
    fi

    backup_current_state

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        rollback_docker_compose
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        rollback_kubernetes
    else
        echo -e "${RED}Error: Invalid deployment mode: $DEPLOYMENT_MODE${NC}"
        exit 1
    fi

    verify_rollback

    echo -e "${GREEN}=== Rollback Complete ===${NC}"
    echo ""
    echo "To redeploy: ./scripts/deploy.sh $ENVIRONMENT $DEPLOYMENT_MODE"
    echo ""
}

# Run main function
main
