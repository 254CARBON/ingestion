#!/bin/bash
# Carbon Ingestion Platform Deployment Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${1:-"development"}
DEPLOYMENT_MODE=${2:-"docker-compose"}
NAMESPACE="carbon-ingestion"

echo -e "${GREEN}=== Carbon Ingestion Platform Deployment ===${NC}"
echo "Environment: $ENVIRONMENT"
echo "Mode: $DEPLOYMENT_MODE"
echo ""

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        if ! command -v docker-compose &> /dev/null; then
            echo -e "${RED}Error: docker-compose not found${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Docker Compose found${NC}"
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        if ! command -v kubectl &> /dev/null; then
            echo -e "${RED}Error: kubectl not found${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ kubectl found${NC}"
    fi

    echo ""
}

# Function to validate configuration
validate_configuration() {
    echo -e "${YELLOW}Validating configuration...${NC}"

    # Check if required config files exist
    if [ ! -f "configs/normalization_rules.yaml" ]; then
        echo -e "${RED}Error: normalization_rules.yaml not found${NC}"
        exit 1
    fi

    if [ ! -f "configs/enrichment_taxonomy.yaml" ]; then
        echo -e "${RED}Error: enrichment_taxonomy.yaml not found${NC}"
        exit 1
    fi

    if [ ! -f "configs/aggregation_policies.yaml" ]; then
        echo -e "${RED}Error: aggregation_policies.yaml not found${NC}"
        exit 1
    fi

    if [ ! -f "configs/projection_policies.yaml" ]; then
        echo -e "${RED}Error: projection_policies.yaml not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Configuration files validated${NC}"
    echo ""
}

# Function to deploy with Docker Compose
deploy_docker_compose() {
    echo -e "${YELLOW}Deploying with Docker Compose...${NC}"

    # Build images
    echo "Building service images..."
    docker-compose -f docker-compose.services.yml build

    # Start infrastructure services
    echo "Starting infrastructure services..."
    docker-compose -f docker-compose.infra.yml up -d

    # Wait for infrastructure to be ready
    echo "Waiting for infrastructure services..."
    sleep 10

    # Start application services
    echo "Starting application services..."
    docker-compose -f docker-compose.services.yml up -d

    # Start observability stack
    echo "Starting observability stack..."
    docker-compose -f docker-compose.observability.yml up -d

    echo -e "${GREEN}✓ Docker Compose deployment completed${NC}"
    echo ""
}

# Function to deploy to Kubernetes
deploy_kubernetes() {
    echo -e "${YELLOW}Deploying to Kubernetes...${NC}"

    # Create namespace if it doesn't exist
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

    # Apply configurations
    echo "Applying ConfigMaps..."
    kubectl apply -f k8s/configmap.yaml -n $NAMESPACE

    # Deploy services
    echo "Deploying services..."
    kubectl apply -f k8s/normalization-service.yaml -n $NAMESPACE
    kubectl apply -f k8s/enrichment-service.yaml -n $NAMESPACE
    kubectl apply -f k8s/aggregation-service.yaml -n $NAMESPACE
    kubectl apply -f k8s/projection-service.yaml -n $NAMESPACE

    # Wait for deployments to be ready
    echo "Waiting for deployments..."
    kubectl wait --for=condition=available --timeout=300s \
        deployment/normalization-service \
        deployment/enrichment-service \
        deployment/aggregation-service \
        deployment/projection-service \
        -n $NAMESPACE

    echo -e "${GREEN}✓ Kubernetes deployment completed${NC}"
    echo ""
}

# Function to run health checks
run_health_checks() {
    echo -e "${YELLOW}Running health checks...${NC}"

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        SERVICES=("normalization:8510" "enrichment:8511" "aggregation:8512" "projection:8513")

        for service in "${SERVICES[@]}"; do
            IFS=':' read -r name port <<< "$service"
            echo "Checking $name..."

            if curl -f -s "http://localhost:$port/health" > /dev/null; then
                echo -e "${GREEN}✓ $name is healthy${NC}"
            else
                echo -e "${RED}✗ $name health check failed${NC}"
            fi
        done
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        SERVICES=("normalization-service" "enrichment-service" "aggregation-service" "projection-service")

        for service in "${SERVICES[@]}"; do
            echo "Checking $service..."

            if kubectl get pods -l app=$service -n $NAMESPACE | grep -q "Running"; then
                echo -e "${GREEN}✓ $service is running${NC}"
            else
                echo -e "${RED}✗ $service is not running${NC}"
            fi
        done
    fi

    echo ""
}

# Function to display deployment info
display_deployment_info() {
    echo -e "${GREEN}=== Deployment Complete ===${NC}"
    echo ""
    echo "Service Endpoints:"

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        echo "  - Normalization: http://localhost:8510"
        echo "  - Enrichment: http://localhost:8511"
        echo "  - Aggregation: http://localhost:8512"
        echo "  - Projection: http://localhost:8513"
        echo "  - Data Quality: http://localhost:8514"
        echo "  - Grafana: http://localhost:3000"
        echo "  - Prometheus: http://localhost:9090"
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        echo "  Use kubectl port-forward to access services"
        echo "  Example: kubectl port-forward svc/normalization-service 8510:8510 -n $NAMESPACE"
    fi

    echo ""
    echo "Next steps:"
    echo "  1. Check service health: ./scripts/health_check.sh"
    echo "  2. View logs: docker-compose logs -f (or kubectl logs)"
    echo "  3. Access Grafana dashboards"
    echo ""
}

# Main deployment flow
main() {
    check_prerequisites
    validate_configuration

    if [ "$DEPLOYMENT_MODE" == "docker-compose" ]; then
        deploy_docker_compose
    elif [ "$DEPLOYMENT_MODE" == "kubernetes" ]; then
        deploy_kubernetes
    else
        echo -e "${RED}Error: Invalid deployment mode: $DEPLOYMENT_MODE${NC}"
        exit 1
    fi

    run_health_checks
    display_deployment_info
}

# Run main function
main
