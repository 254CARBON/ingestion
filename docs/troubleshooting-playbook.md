# 254Carbon Ingestion Platform - Troubleshooting Playbook

## Overview

This troubleshooting playbook provides step-by-step procedures for diagnosing and resolving common issues in the 254Carbon Ingestion Platform.

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Service Health Issues](#service-health-issues)
3. [Data Pipeline Issues](#data-pipeline-issues)
4. [Performance Issues](#performance-issues)
5. [Infrastructure Issues](#infrastructure-issues)
6. [Security Issues](#security-issues)
7. [Monitoring Issues](#monitoring-issues)
8. [Emergency Procedures](#emergency-procedures)

## Quick Diagnostics

### Health Check Commands

```bash
# Check all pods
kubectl get pods -n carbon-ingestion

# Check service status
kubectl get services -n carbon-ingestion

# Check deployments
kubectl get deployments -n carbon-ingestion

# Check events
kubectl get events -n carbon-ingestion --sort-by='.lastTimestamp'

# Check resource usage
kubectl top pods -n carbon-ingestion
kubectl top nodes
```

### Service Endpoints

```bash
# Test service endpoints
curl http://normalization-service:8510/health
curl http://enrichment-service:8511/health
curl http://aggregation-service:8512/health

# Check metrics
curl http://normalization-service:8510/metrics
curl http://enrichment-service:8511/metrics
curl http://aggregation-service:8512/metrics
```

### Log Analysis

```bash
# Get recent logs
kubectl logs -l app=normalization-service -n carbon-ingestion --tail=100

# Follow logs
kubectl logs -f deployment/normalization-service -n carbon-ingestion

# Get logs from specific container
kubectl logs deployment/normalization-service -c normalization-service -n carbon-ingestion
```

## Service Health Issues

### Service Not Starting

#### Symptoms
- Pod status shows `CrashLoopBackOff` or `Pending`
- Service endpoints not responding
- Health checks failing

#### Diagnosis Steps

1. **Check pod status**:
   ```bash
   kubectl describe pod <pod-name> -n carbon-ingestion
   ```

2. **Check pod logs**:
   ```bash
   kubectl logs <pod-name> -n carbon-ingestion
   ```

3. **Check resource constraints**:
   ```bash
   kubectl describe pod <pod-name> -n carbon-ingestion | grep -A 5 "Limits\|Requests"
   ```

4. **Check node resources**:
   ```bash
   kubectl describe node <node-name>
   ```

#### Common Causes and Solutions

**Cause 1: Resource Constraints**
- **Solution**: Increase resource limits or requests
- **Command**: 
  ```bash
  kubectl patch deployment normalization-service -n carbon-ingestion -p '{"spec":{"template":{"spec":{"containers":[{"name":"normalization-service","resources":{"limits":{"memory":"4Gi"}}}]}}}}'
  ```

**Cause 2: Configuration Errors**
- **Solution**: Check ConfigMap and environment variables
- **Command**:
  ```bash
  kubectl get configmap carbon-ingestion-config -n carbon-ingestion -o yaml
  ```

**Cause 3: Dependency Issues**
- **Solution**: Verify dependent services are running
- **Command**:
  ```bash
  kubectl get pods -l app=kafka -n carbon-ingestion
  kubectl get pods -l app=clickhouse -n carbon-ingestion
  ```

### Service Restarting Frequently

#### Symptoms
- Pod restart count increasing
- Service availability intermittent
- Error logs showing crashes

#### Diagnosis Steps

1. **Check restart count**:
   ```bash
   kubectl get pods -n carbon-ingestion -o wide
   ```

2. **Check previous container logs**:
   ```bash
   kubectl logs <pod-name> -n carbon-ingestion --previous
   ```

3. **Check liveness probe**:
   ```bash
   kubectl describe pod <pod-name> -n carbon-ingestion | grep -A 10 "Liveness"
   ```

#### Common Causes and Solutions

**Cause 1: Memory Leaks**
- **Solution**: Increase memory limits or fix memory leaks
- **Command**:
  ```bash
  kubectl patch deployment normalization-service -n carbon-ingestion -p '{"spec":{"template":{"spec":{"containers":[{"name":"normalization-service","resources":{"limits":{"memory":"8Gi"}}}]}}}}'
  ```

**Cause 2: Health Check Failures**
- **Solution**: Adjust health check parameters
- **Command**:
  ```bash
  kubectl patch deployment normalization-service -n carbon-ingestion -p '{"spec":{"template":{"spec":{"containers":[{"name":"normalization-service","livenessProbe":{"initialDelaySeconds":60}}]}}}}'
  ```

## Data Pipeline Issues

### Data Not Flowing

#### Symptoms
- No data in output topics
- Empty ClickHouse tables
- Connector status showing errors

#### Diagnosis Steps

1. **Check Kafka topics**:
   ```bash
   kubectl exec -it kafka-0 -n carbon-ingestion -- kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

2. **Check topic messages**:
   ```bash
   kubectl exec -it kafka-0 -n carbon-ingestion -- kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingestion.miso.raw.v1 --from-beginning --max-messages 10
   ```

3. **Check ClickHouse tables**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SHOW TABLES FROM carbon_ingestion"
   ```

4. **Check connector logs**:
   ```bash
   kubectl logs -l app=connector-registry -n carbon-ingestion --tail=100
   ```

#### Common Causes and Solutions

**Cause 1: Schema Registry Issues**
- **Solution**: Restart Schema Registry
- **Command**:
  ```bash
  kubectl rollout restart deployment schema-registry -n carbon-ingestion
  ```

**Cause 2: Connector Configuration Errors**
- **Solution**: Check connector YAML files
- **Command**:
  ```bash
  kubectl get configmap connectors-config -n carbon-ingestion -o yaml
  ```

**Cause 3: Network Connectivity Issues**
- **Solution**: Check service mesh and network policies
- **Command**:
  ```bash
  kubectl exec -it <pod-name> -n carbon-ingestion -- nslookup kafka-cluster
  kubectl exec -it <pod-name> -n carbon-ingestion -- telnet kafka-cluster 9092
   ```

### Data Quality Issues

#### Symptoms
- Validation errors in logs
- Low quality scores
- Missing or corrupted data

#### Diagnosis Steps

1. **Check validation errors**:
   ```bash
   kubectl logs -l app=normalization-service -n carbon-ingestion | grep "validation_error"
   ```

2. **Check quality metrics**:
   ```bash
   curl http://normalization-service:8510/metrics | grep quality
   ```

3. **Check data samples**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SELECT * FROM carbon_ingestion.bronze_raw_trades LIMIT 10"
   ```

#### Common Causes and Solutions

**Cause 1: Schema Mismatches**
- **Solution**: Update schemas or add schema evolution
- **Command**:
  ```bash
  kubectl exec -it schema-registry-0 -n carbon-ingestion -- curl -X GET http://localhost:8081/subjects
  ```

**Cause 2: Data Source Issues**
- **Solution**: Check external data sources
- **Command**:
  ```bash
  kubectl logs -l app=miso-connector -n carbon-ingestion --tail=100
  ```

## Performance Issues

### High Latency

#### Symptoms
- Slow data processing
- High response times
- Backlog in Kafka topics

#### Diagnosis Steps

1. **Check processing metrics**:
   ```bash
   curl http://normalization-service:8510/metrics | grep processing_time
   ```

2. **Check Kafka consumer lag**:
   ```bash
   kubectl exec -it kafka-0 -n carbon-ingestion -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group normalization-consumer
   ```

3. **Check resource usage**:
   ```bash
   kubectl top pods -n carbon-ingestion
   ```

#### Common Causes and Solutions

**Cause 1: Resource Bottlenecks**
- **Solution**: Scale up services or increase resources
- **Command**:
  ```bash
  kubectl scale deployment normalization-service --replicas=5 -n carbon-ingestion
  ```

**Cause 2: Database Performance**
- **Solution**: Optimize ClickHouse queries or increase resources
- **Command**:
  ```bash
  kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SELECT * FROM system.processes"
   ```

### Low Throughput

#### Symptoms
- Low message processing rate
- Small batch sizes
- Underutilized resources

#### Diagnosis Steps

1. **Check throughput metrics**:
   ```bash
   curl http://normalization-service:8510/metrics | grep throughput
   ```

2. **Check batch sizes**:
   ```bash
   kubectl logs -l app=normalization-service -n carbon-ingestion | grep "batch_size"
   ```

3. **Check parallelism settings**:
   ```bash
   kubectl get configmap carbon-ingestion-config -n carbon-ingestion -o yaml | grep PARALLELISM
   ```

#### Common Causes and Solutions

**Cause 1: Low Parallelism**
- **Solution**: Increase parallelism settings
- **Command**:
  ```bash
  kubectl patch configmap carbon-ingestion-config -n carbon-ingestion -p '{"data":{"PARALLELISM":"8"}}'
  ```

**Cause 2: Small Batch Sizes**
- **Solution**: Increase batch sizes
- **Command**:
  ```bash
  kubectl patch configmap carbon-ingestion-config -n carbon-ingestion -p '{"data":{"BATCH_SIZE":"2000"}}'
  ```

## Infrastructure Issues

### Kafka Issues

#### Symptoms
- Kafka pods not running
- Topic creation failures
- Message delivery issues

#### Diagnosis Steps

1. **Check Kafka cluster status**:
   ```bash
   kubectl get pods -l app=kafka -n carbon-ingestion
   ```

2. **Check Kafka logs**:
   ```bash
   kubectl logs -l app=kafka -n carbon-ingestion --tail=100
   ```

3. **Check Zookeeper status**:
   ```bash
   kubectl get pods -l app=zookeeper -n carbon-ingestion
   ```

#### Common Causes and Solutions

**Cause 1: Zookeeper Issues**
- **Solution**: Restart Zookeeper
- **Command**:
  ```bash
  kubectl rollout restart statefulset zookeeper -n carbon-ingestion
  ```

**Cause 2: Storage Issues**
- **Solution**: Check persistent volumes
- **Command**:
  ```bash
  kubectl get pv,pvc -n carbon-ingestion
  kubectl describe pvc kafka-data-kafka-0 -n carbon-ingestion
  ```

### ClickHouse Issues

#### Symptoms
- ClickHouse pods not running
- Query failures
- Data not persisting

#### Diagnosis Steps

1. **Check ClickHouse status**:
   ```bash
   kubectl get pods -l app=clickhouse -n carbon-ingestion
   ```

2. **Check ClickHouse logs**:
   ```bash
   kubectl logs -l app=clickhouse -n carbon-ingestion --tail=100
   ```

3. **Test ClickHouse connectivity**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SELECT 1"
   ```

#### Common Causes and Solutions

**Cause 1: Memory Issues**
- **Solution**: Increase memory limits
- **Command**:
  ```bash
  kubectl patch deployment clickhouse -n carbon-ingestion -p '{"spec":{"template":{"spec":{"containers":[{"name":"clickhouse","resources":{"limits":{"memory":"8Gi"}}}]}}}}'
  ```

**Cause 2: Disk Space Issues**
- **Solution**: Clean up old data or increase storage
- **Command**:
  ```bash
  kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SELECT * FROM system.disks"
  ```

## Security Issues

### Authentication Failures

#### Symptoms
- Service authentication errors
- API key validation failures
- Certificate errors

#### Diagnosis Steps

1. **Check secrets**:
   ```bash
   kubectl get secrets -n carbon-ingestion
   kubectl describe secret api-keys -n carbon-ingestion
   ```

2. **Check service account**:
   ```bash
   kubectl get serviceaccount -n carbon-ingestion
   kubectl describe serviceaccount carbon-ingestion -n carbon-ingestion
   ```

3. **Check RBAC**:
   ```bash
   kubectl get roles,rolebindings -n carbon-ingestion
   ```

#### Common Causes and Solutions

**Cause 1: Missing Secrets**
- **Solution**: Create required secrets
- **Command**:
  ```bash
  kubectl create secret generic api-keys --from-literal=miso-api-key=your-key -n carbon-ingestion
  ```

**Cause 2: RBAC Issues**
- **Solution**: Update RBAC configuration
- **Command**:
  ```bash
  kubectl apply -f k8s/rbac.yaml
  ```

### Network Security Issues

#### Symptoms
- Service communication failures
- Network policy violations
- Ingress/egress issues

#### Diagnosis Steps

1. **Check network policies**:
   ```bash
   kubectl get networkpolicies -n carbon-ingestion
   kubectl describe networkpolicy carbon-ingestion-network-policy -n carbon-ingestion
   ```

2. **Test network connectivity**:
   ```bash
   kubectl exec -it <pod-name> -n carbon-ingestion -- ping normalization-service
   kubectl exec -it <pod-name> -n carbon-ingestion -- telnet normalization-service 8510
   ```

#### Common Causes and Solutions

**Cause 1: Network Policy Too Restrictive**
- **Solution**: Update network policies
- **Command**:
  ```bash
  kubectl apply -f k8s/network-policies.yaml
  ```

## Monitoring Issues

### Metrics Not Available

#### Symptoms
- Prometheus not scraping metrics
- Grafana dashboards empty
- Alert rules not firing

#### Diagnosis Steps

1. **Check Prometheus targets**:
   ```bash
   kubectl port-forward svc/prometheus-server 9090:9090 -n carbon-ingestion
   # Open http://localhost:9090/targets
   ```

2. **Check service annotations**:
   ```bash
   kubectl get pods -n carbon-ingestion -o yaml | grep -A 5 prometheus.io
   ```

3. **Test metrics endpoint**:
   ```bash
   kubectl exec -it <pod-name> -n carbon-ingestion -- curl http://localhost:8510/metrics
   ```

#### Common Causes and Solutions

**Cause 1: Missing Prometheus Annotations**
- **Solution**: Add Prometheus annotations
- **Command**:
  ```bash
  kubectl patch deployment normalization-service -n carbon-ingestion -p '{"spec":{"template":{"metadata":{"annotations":{"prometheus.io/scrape":"true","prometheus.io/port":"8510","prometheus.io/path":"/metrics"}}}}}'
  ```

### Logging Issues

#### Symptoms
- Missing logs
- Log format issues
- Log aggregation failures

#### Diagnosis Steps

1. **Check log configuration**:
   ```bash
   kubectl get configmap carbon-ingestion-config -n carbon-ingestion -o yaml | grep LOG
   ```

2. **Check log volume**:
   ```bash
   kubectl exec -it <pod-name> -n carbon-ingestion -- df -h /var/log
   ```

#### Common Causes and Solutions

**Cause 1: Log Level Too High**
- **Solution**: Lower log level
- **Command**:
  ```bash
  kubectl patch configmap carbon-ingestion-config -n carbon-ingestion -p '{"data":{"LOG_LEVEL":"DEBUG"}}'
  ```

## Emergency Procedures

### Service Outage

1. **Identify affected services**:
   ```bash
   kubectl get pods -n carbon-ingestion | grep -v Running
   ```

2. **Restart affected services**:
   ```bash
   kubectl rollout restart deployment normalization-service -n carbon-ingestion
   kubectl rollout restart deployment enrichment-service -n carbon-ingestion
   kubectl rollout restart deployment aggregation-service -n carbon-ingestion
   ```

3. **Monitor recovery**:
   ```bash
   kubectl rollout status deployment normalization-service -n carbon-ingestion
   ```

### Data Loss Prevention

1. **Check data persistence**:
   ```bash
   kubectl get pv,pvc -n carbon-ingestion
   ```

2. **Backup critical data**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "BACKUP DATABASE carbon_ingestion TO Disk('backups', 'emergency_backup_$(date +%Y%m%d_%H%M%S)')"
   ```

3. **Verify backup**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "SELECT * FROM system.backups"
   ```

### Complete System Recovery

1. **Stop all services**:
   ```bash
   kubectl scale deployment --replicas=0 --all -n carbon-ingestion
   ```

2. **Restore from backup**:
   ```bash
   kubectl exec -it clickhouse-0 -n carbon-ingestion -- clickhouse-client --query "RESTORE DATABASE carbon_ingestion FROM Disk('backups', 'backup_name')"
   ```

3. **Restart services**:
   ```bash
   kubectl scale deployment --replicas=3 normalization-service -n carbon-ingestion
   kubectl scale deployment --replicas=2 enrichment-service -n carbon-ingestion
   kubectl scale deployment --replicas=2 aggregation-service -n carbon-ingestion
   ```

## Support Contacts

- **Platform Team**: platform@254carbon.com
- **On-Call Engineer**: oncall@254carbon.com
- **Emergency Hotline**: +1-555-0123

## Additional Resources

- [Deployment Guide](deployment-guide.md)
- [API Documentation](api-documentation.md)
- [Monitoring Guide](monitoring-guide.md)
- [Security Guide](security-guide.md)
