# 254Carbon Ingestion Platform - Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the 254Carbon Ingestion Platform in various environments, from local development to production Kubernetes clusters.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development](#local-development)
3. [Docker Compose Deployment](#docker-compose-deployment)
4. [Kubernetes Deployment](#kubernetes-deployment)
5. [Helm Chart Deployment](#helm-chart-deployment)
6. [Configuration](#configuration)
7. [Monitoring and Observability](#monitoring-and-observability)
8. [Security](#security)
9. [Troubleshooting](#troubleshooting)
10. [Maintenance](#maintenance)

## Prerequisites

### System Requirements

- **CPU**: Minimum 4 cores, recommended 8+ cores
- **Memory**: Minimum 8GB RAM, recommended 16GB+ RAM
- **Storage**: Minimum 100GB SSD, recommended 500GB+ SSD
- **Network**: Stable internet connection for data ingestion

### Software Requirements

- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **Kubernetes**: Version 1.24+
- **Helm**: Version 3.8+
- **kubectl**: Version 1.24+

### External Dependencies

- **Kafka Cluster**: For message streaming
- **ClickHouse**: For time-series data storage
- **Redis**: For caching and session management
- **Schema Registry**: For Avro schema management

## Local Development

### Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/254carbon/ingestion.git
   cd ingestion
   ```

2. **Set up Python environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements-dev.txt
   pip install -e .
   ```

3. **Start infrastructure services**:
   ```bash
   make infra-up
   ```

4. **Start microservices**:
   ```bash
   make services-up
   ```

5. **Start Airflow**:
   ```bash
   make airflow-up
   ```

6. **Verify deployment**:
   ```bash
   make health-check
   ```

### Development Workflow

1. **Run tests**:
   ```bash
   make test
   ```

2. **Lint code**:
   ```bash
   make lint
   ```

3. **Format code**:
   ```bash
   make format
   ```

4. **Validate schemas**:
   ```bash
   make validate-schemas
   ```

5. **Generate connectors index**:
   ```bash
   make connectors-index
   ```

## Docker Compose Deployment

### Infrastructure Services

Start the core infrastructure services:

```bash
docker-compose -f docker-compose.infra.yml up -d
```

This starts:
- Kafka cluster with Zookeeper
- ClickHouse database
- Redis cache
- Schema Registry
- Kafka UI for development

### Microservices

Start the microservices:

```bash
docker-compose -f docker-compose.services.yml up -d
```

This starts:
- Connector Registry Service
- Normalization Service
- Enrichment Service
- Aggregation Service
- Projection Service

### Airflow

Start Airflow for orchestration:

```bash
docker-compose -f docker-compose.airflow.yml up -d
```

### Health Checks

Check service health:

```bash
# Check infrastructure
docker-compose -f docker-compose.infra.yml ps

# Check services
docker-compose -f docker-compose.services.yml ps

# Check Airflow
docker-compose -f docker-compose.airflow.yml ps
```

### Logs

View logs:

```bash
# Infrastructure logs
docker-compose -f docker-compose.infra.yml logs -f

# Service logs
docker-compose -f docker-compose.services.yml logs -f

# Airflow logs
docker-compose -f docker-compose.airflow.yml logs -f
```

## Kubernetes Deployment

### Prerequisites

1. **Kubernetes cluster** (version 1.24+)
2. **kubectl** configured
3. **Helm** installed
4. **Storage classes** configured
5. **Ingress controller** (optional)

### Namespace Setup

Create the namespace:

```bash
kubectl apply -f k8s/namespace.yaml
```

### ConfigMaps

Apply configuration:

```bash
kubectl apply -f k8s/configmap.yaml
```

### Services

Deploy services:

```bash
# Normalization Service
kubectl apply -f k8s/normalization-service.yaml

# Enrichment Service
kubectl apply -f k8s/enrichment-service.yaml

# Aggregation Service
kubectl apply -f k8s/aggregation-service.yaml
```

### Verification

Check deployment status:

```bash
# Check pods
kubectl get pods -n carbon-ingestion

# Check services
kubectl get services -n carbon-ingestion

# Check deployments
kubectl get deployments -n carbon-ingestion

# Check horizontal pod autoscalers
kubectl get hpa -n carbon-ingestion
```

## Helm Chart Deployment

### Add Helm Repositories

```bash
# Add Bitnami repository
helm repo add bitnami https://charts.bitnami.com/bitnami

# Add Prometheus repository
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

# Add Grafana repository
helm repo add grafana https://grafana.github.io/helm-charts

# Update repositories
helm repo update
```

### Install Dependencies

```bash
# Install Kafka
helm install kafka bitnami/kafka \
  --namespace carbon-ingestion \
  --create-namespace \
  --values helm/carbon-ingestion/values-kafka.yaml

# Install ClickHouse
helm install clickhouse clickhouse/clickhouse \
  --namespace carbon-ingestion \
  --values helm/carbon-ingestion/values-clickhouse.yaml

# Install Redis
helm install redis bitnami/redis \
  --namespace carbon-ingestion \
  --values helm/carbon-ingestion/values-redis.yaml
```

### Install Carbon Ingestion

```bash
# Install the chart
helm install carbon-ingestion ./helm/carbon-ingestion \
  --namespace carbon-ingestion \
  --create-namespace \
  --values helm/carbon-ingestion/values.yaml

# Upgrade the chart
helm upgrade carbon-ingestion ./helm/carbon-ingestion \
  --namespace carbon-ingestion \
  --values helm/carbon-ingestion/values.yaml
```

### Uninstall

```bash
# Uninstall the chart
helm uninstall carbon-ingestion --namespace carbon-ingestion

# Uninstall dependencies
helm uninstall redis --namespace carbon-ingestion
helm uninstall clickhouse --namespace carbon-ingestion
helm uninstall kafka --namespace carbon-ingestion
```

## Configuration

### Environment Variables

Key environment variables:

```bash
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS=kafka-cluster:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# ClickHouse configuration
CLICKHOUSE_DSN=clickhouse://default@clickhouse:9000/carbon_ingestion

# Redis configuration
REDIS_URL=redis://redis:6379/0

# Service configuration
LOG_LEVEL=INFO
PARALLELISM=4
```

### Configuration Files

- **Normalization Rules**: `configs/normalization_rules.yaml`
- **Enrichment Taxonomy**: `configs/enrichment_taxonomy.yaml`
- **Aggregation Policies**: `configs/aggregation_policies.yaml`

### Secrets Management

Create secrets:

```bash
# Create secret for database credentials
kubectl create secret generic db-credentials \
  --from-literal=username=admin \
  --from-literal=password=secret \
  --namespace carbon-ingestion

# Create secret for API keys
kubectl create secret generic api-keys \
  --from-literal=miso-api-key=your-key \
  --from-literal=caiso-api-key=your-key \
  --namespace carbon-ingestion
```

## Monitoring and Observability

### Prometheus Metrics

Services expose metrics at `/metrics` endpoint:

- **Normalization Service**: `http://normalization-service:8510/metrics`
- **Enrichment Service**: `http://enrichment-service:8511/metrics`
- **Aggregation Service**: `http://aggregation-service:8512/metrics`

### Grafana Dashboards

Import dashboards:

1. **Service Metrics Dashboard**
2. **Data Pipeline Dashboard**
3. **Connector Health Dashboard**
4. **System Performance Dashboard**

### Logging

Structured JSON logging:

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "service": "normalization-service",
  "message": "Data normalized successfully",
  "market": "MISO",
  "record_count": 1000,
  "processing_time_ms": 150
}
```

### Alerting

Configure alerts for:

- **Service Health**: Pod restarts, health check failures
- **Data Quality**: Validation errors, missing data
- **Performance**: High latency, low throughput
- **Resource Usage**: CPU, memory, disk usage

## Security

### Network Policies

Apply network policies:

```bash
kubectl apply -f k8s/network-policies.yaml
```

### Pod Security Policies

Enable pod security policies:

```bash
kubectl apply -f k8s/pod-security-policies.yaml
```

### RBAC

Configure role-based access control:

```bash
kubectl apply -f k8s/rbac.yaml
```

### TLS/SSL

Enable TLS for services:

```bash
# Generate certificates
kubectl apply -f k8s/certificates.yaml

# Update service configurations
kubectl apply -f k8s/tls-services.yaml
```

## Troubleshooting

### Common Issues

#### Service Not Starting

1. **Check pod status**:
   ```bash
   kubectl describe pod <pod-name> -n carbon-ingestion
   ```

2. **Check logs**:
   ```bash
   kubectl logs <pod-name> -n carbon-ingestion
   ```

3. **Check events**:
   ```bash
   kubectl get events -n carbon-ingestion
   ```

#### Data Not Flowing

1. **Check Kafka topics**:
   ```bash
   kubectl exec -it kafka-0 -n carbon-ingestion -- kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

2. **Check ClickHouse tables**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SHOW TABLES"
   ```

3. **Check service connectivity**:
   ```bash
   kubectl exec -it <pod-name> -n carbon-ingestion -- curl http://normalization-service:8510/health
   ```

#### Performance Issues

1. **Check resource usage**:
   ```bash
   kubectl top pods -n carbon-ingestion
   kubectl top nodes
   ```

2. **Check HPA status**:
   ```bash
   kubectl describe hpa -n carbon-ingestion
   ```

3. **Check service metrics**:
   ```bash
   kubectl port-forward svc/normalization-service 8510:8510 -n carbon-ingestion
   curl http://localhost:8510/metrics
   ```

### Debug Commands

```bash
# Get all resources
kubectl get all -n carbon-ingestion

# Describe service
kubectl describe service normalization-service -n carbon-ingestion

# Get service endpoints
kubectl get endpoints -n carbon-ingestion

# Check persistent volumes
kubectl get pv,pvc -n carbon-ingestion

# Check ingress
kubectl get ingress -n carbon-ingestion
```

## Maintenance

### Updates

1. **Update service images**:
   ```bash
   kubectl set image deployment/normalization-service normalization-service=ghcr.io/254carbon/ingestion:v1.1.0 -n carbon-ingestion
   ```

2. **Rolling update**:
   ```bash
   kubectl rollout restart deployment/normalization-service -n carbon-ingestion
   ```

3. **Check rollout status**:
   ```bash
   kubectl rollout status deployment/normalization-service -n carbon-ingestion
   ```

### Backups

1. **Database backup**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "BACKUP DATABASE carbon_ingestion TO Disk('backups', 'backup_$(date +%Y%m%d_%H%M%S)')"
   ```

2. **Configuration backup**:
   ```bash
   kubectl get configmap carbon-ingestion-config -n carbon-ingestion -o yaml > backup-config.yaml
   ```

### Scaling

1. **Scale services**:
   ```bash
   kubectl scale deployment normalization-service --replicas=5 -n carbon-ingestion
   ```

2. **Update HPA**:
   ```bash
   kubectl patch hpa normalization-service-hpa -n carbon-ingestion -p '{"spec":{"maxReplicas":20}}'
   ```

### Cleanup

1. **Remove resources**:
   ```bash
   kubectl delete namespace carbon-ingestion
   ```

2. **Clean up persistent volumes**:
   ```bash
   kubectl delete pv --all
   ```

## Support

For additional support:

- **Documentation**: [GitHub Wiki](https://github.com/254carbon/ingestion/wiki)
- **Issues**: [GitHub Issues](https://github.com/254carbon/ingestion/issues)
- **Discussions**: [GitHub Discussions](https://github.com/254carbon/ingestion/discussions)
- **Email**: team@254carbon.com
