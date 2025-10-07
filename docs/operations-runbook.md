# Operations Runbook

## Overview

This runbook provides operational procedures for the 254Carbon ingestion platform, including common issues, resolutions, escalation procedures, and disaster recovery steps.

## Table of Contents

1. [Service Architecture](#service-architecture)
2. [Common Issues](#common-issues)
3. [Escalation Procedures](#escalation-procedures)
4. [Disaster Recovery](#disaster-recovery)
5. [Maintenance Procedures](#maintenance-procedures)
6. [Monitoring and Alerting](#monitoring-and-alerting)

## Service Architecture

### Service Dependencies

```
Connectors → Normalization → Enrichment → Aggregation → Projection
     ↓            ↓              ↓             ↓            ↓
  Kafka       Kafka          Kafka         Kafka      ClickHouse
                                                      + Redis
```

### Critical Services

| Service | Port | Dependencies | Impact if Down |
|---------|------|--------------|----------------|
| Kafka | 9092 | None | Complete pipeline failure |
| ClickHouse | 9000 | None | No data persistence |
| Redis | 6379 | None | No caching, slower queries |
| Normalization | 8510 | Kafka | Raw data not processed |
| Enrichment | 8511 | Kafka, Normalization | No semantic enrichment |
| Aggregation | 8512 | Kafka, Enrichment | No OHLC bars or metrics |
| Projection | 8513 | Kafka, ClickHouse, Redis | No serving layer updates |

## Common Issues

### Issue 1: Service Not Starting

**Symptoms**:
- Service pod/container in CrashLoopBackOff
- Health check failures
- Service logs show startup errors

**Diagnosis**:
```bash
# Check service logs
kubectl logs deployment/normalization-service -n carbon-ingestion
# or
docker-compose logs normalization

# Check service status
kubectl get pods -l app=normalization-service -n carbon-ingestion
# or
docker-compose ps
```

**Resolution**:
1. Check for missing environment variables
2. Verify database connectivity
3. Check Kafka broker availability
4. Review configuration files for errors
5. Verify secrets are properly mounted

**Escalation**: If unresolved after 15 minutes, escalate to Platform Team

---

### Issue 2: High Consumer Lag

**Symptoms**:
- Kafka consumer lag > 1000 messages
- Increasing latency in data processing
- Alerts firing for consumer lag

**Diagnosis**:
```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --describe --group enrichment-service

# Check service resource usage
kubectl top pod -l app=enrichment-service -n carbon-ingestion
```

**Resolution**:
1. **Scale up service**:
   ```bash
   kubectl scale deployment enrichment-service --replicas=5 -n carbon-ingestion
   ```

2. **Increase parallelism**:
   ```bash
   kubectl set env deployment/enrichment-service PARALLELISM=8 -n carbon-ingestion
   ```

3. **Check for slow processing**:
   - Review service logs for errors
   - Check database query performance
   - Monitor external API latency

**Escalation**: If lag continues to grow after scaling, escalate immediately

---

### Issue 3: Data Quality Degradation

**Symptoms**:
- Quality score < 0.7
- High anomaly detection rate
- Validation errors increasing

**Diagnosis**:
```bash
# Check data quality metrics
curl http://localhost:8514/quality/report?hours=24

# Check anomalies
curl http://localhost:8514/anomalies?limit=50

# Check quality dashboard
# Open Grafana: http://localhost:3000/d/data-quality
```

**Resolution**:
1. **Identify source**:
   - Check which market/connector is affected
   - Review connector logs for extraction errors
   - Verify upstream data source health

2. **Temporary mitigation**:
   - Disable problematic connector if necessary
   - Enable stricter validation rules
   - Increase quality threshold alerts

3. **Long-term fix**:
   - Contact data provider if source issue
   - Update validation rules if false positives
   - Enhance error handling in connector

**Escalation**: If quality score < 0.5, escalate to Data Team immediately

---

### Issue 4: ClickHouse Write Failures

**Symptoms**:
- Projection service errors
- ClickHouse write latency > 5s
- Data not appearing in serving layer

**Diagnosis**:
```bash
# Check ClickHouse health
curl http://clickhouse:8123/ping

# Check table statistics
clickhouse-client --query="SELECT * FROM system.parts WHERE database='carbon_ingestion'"

# Check disk space
df -h /var/lib/clickhouse
```

**Resolution**:
1. **Check disk space**:
   ```bash
   # If disk full, clean old partitions
   clickhouse-client --query="ALTER TABLE carbon_ingestion.gold_ohlc_bars DROP PARTITION '2024-01-01'"
   ```

2. **Optimize tables**:
   ```bash
   clickhouse-client --query="OPTIMIZE TABLE carbon_ingestion.gold_ohlc_bars FINAL"
   ```

3. **Check connection pool**:
   - Increase pool size in projection service
   - Verify connection limits in ClickHouse

**Escalation**: If writes failing for > 5 minutes, escalate to Platform Team

---

### Issue 5: Redis Cache Issues

**Symptoms**:
- Low cache hit rate (< 70%)
- Redis memory warnings
- Slow query responses

**Diagnosis**:
```bash
# Check Redis info
redis-cli INFO

# Check memory usage
redis-cli INFO memory

# Check cache hit rate
curl http://localhost:8513/cache/stats
```

**Resolution**:
1. **Memory issues**:
   ```bash
   # Clear cache if needed
   redis-cli FLUSHDB
   
   # Or increase max memory
   redis-cli CONFIG SET maxmemory 2gb
   ```

2. **Low hit rate**:
   - Review cache TTL settings
   - Check if cache invalidation is too aggressive
   - Verify query patterns match cache keys

3. **Connection issues**:
   - Check Redis service health
   - Verify connection pool settings
   - Review network connectivity

**Escalation**: If cache completely unavailable, escalate immediately

---

### Issue 6: Kafka Topic Lag

**Symptoms**:
- Messages piling up in topics
- Increasing end-to-end latency
- Consumer groups falling behind

**Diagnosis**:
```bash
# List topics and their lag
kafka-consumer-groups --bootstrap-server kafka:9092 --all-groups --describe

# Check topic details
kafka-topics --bootstrap-server kafka:9092 --describe --topic normalized.market.ticks.v1
```

**Resolution**:
1. **Scale consumers**:
   ```bash
   # Increase consumer replicas
   kubectl scale deployment normalization-service --replicas=5 -n carbon-ingestion
   ```

2. **Increase partitions** (if needed):
   ```bash
   kafka-topics --bootstrap-server kafka:9092 \
     --alter --topic normalized.market.ticks.v1 \
     --partitions 10
   ```

3. **Optimize consumer configuration**:
   - Increase `max_poll_records`
   - Adjust `fetch_min_bytes` and `fetch_max_wait_ms`
   - Enable compression

**Escalation**: If lag > 10,000 messages, escalate to Platform Team

## Escalation Procedures

### Severity Levels

| Level | Description | Response Time | Escalation |
|-------|-------------|---------------|------------|
| P1 - Critical | Complete service outage | 15 minutes | Immediate page |
| P2 - High | Degraded performance | 1 hour | Email + Slack |
| P3 - Medium | Non-critical issue | 4 hours | Slack |
| P4 - Low | Minor issue | Next business day | Ticket |

### Escalation Contacts

1. **Platform Team** (Primary)
   - Slack: #carbon-ingestion-ops
   - Email: platform@254carbon.com
   - PagerDuty: Carbon Ingestion Platform

2. **Data Team** (Data Quality Issues)
   - Slack: #carbon-data-quality
   - Email: data@254carbon.com

3. **Infrastructure Team** (Infrastructure Issues)
   - Slack: #infrastructure
   - Email: infra@254carbon.com

4. **On-Call Engineer**
   - PagerDuty: 254Carbon On-Call

### Escalation Decision Tree

```
Issue Detected
    ↓
Is service completely down? → YES → P1: Page on-call immediately
    ↓ NO
Is data quality < 0.5? → YES → P2: Escalate to Data Team
    ↓ NO
Is performance degraded > 50%? → YES → P2: Escalate to Platform Team
    ↓ NO
Is issue affecting users? → YES → P3: Notify Platform Team
    ↓ NO
Log issue → P4: Create ticket
```

## Disaster Recovery

### Scenario 1: Complete Data Loss

**Recovery Procedure**:

1. **Assess scope**:
   - Identify affected time range
   - Determine which services/tables affected
   - Estimate recovery time

2. **Restore from backups**:
   ```bash
   # Restore ClickHouse from backup
   clickhouse-backup restore --schema --data latest
   
   # Verify restoration
   clickhouse-client --query="SELECT count() FROM carbon_ingestion.gold_ohlc_bars"
   ```

3. **Reprocess historical data**:
   ```bash
   # Use reprocessing API
   curl -X POST http://localhost:8510/reprocess \
     -H "Content-Type: application/json" \
     -d '{
       "start_time": "2025-01-01T00:00:00Z",
       "end_time": "2025-01-15T23:59:59Z",
       "market": "CAISO"
     }'
   ```

4. **Verify data integrity**:
   - Run data quality checks
   - Compare record counts
   - Validate key metrics

**RTO**: 4 hours
**RPO**: 1 hour (based on backup frequency)

---

### Scenario 2: Service Cluster Failure

**Recovery Procedure**:

1. **Failover to backup cluster**:
   ```bash
   # Update DNS/load balancer to point to backup cluster
   # Or use Kubernetes federation for automatic failover
   ```

2. **Verify backup cluster health**:
   ```bash
   kubectl get pods -n carbon-ingestion --context=backup-cluster
   ```

3. **Sync data** (if needed):
   - Use Kafka MirrorMaker for topic replication
   - Use ClickHouse replication for database sync

**RTO**: 30 minutes
**RPO**: Near-zero (with proper replication)

---

### Scenario 3: Kafka Cluster Failure

**Recovery Procedure**:

1. **Assess Kafka cluster state**:
   ```bash
   kafka-broker-api-versions --bootstrap-server kafka:9092
   ```

2. **Restart Kafka brokers**:
   ```bash
   # Restart one broker at a time
   kubectl delete pod kafka-0 -n kafka
   kubectl wait --for=condition=ready pod/kafka-0 -n kafka --timeout=300s
   ```

3. **Verify topic replication**:
   ```bash
   kafka-topics --bootstrap-server kafka:9092 --describe
   ```

4. **Resume service consumption**:
   - Services should automatically reconnect
   - Monitor consumer lag
   - Verify data flow resumes

**RTO**: 1 hour
**RPO**: Zero (Kafka retains messages)

## Maintenance Procedures

### Planned Maintenance Window

1. **Pre-Maintenance** (T-24 hours):
   - Notify stakeholders
   - Create backup of all data
   - Prepare rollback plan
   - Test changes in staging

2. **During Maintenance**:
   - Enable maintenance mode (if available)
   - Stop consumers to prevent data loss
   - Perform maintenance tasks
   - Run validation tests

3. **Post-Maintenance**:
   - Resume services
   - Monitor for errors
   - Verify data flow
   - Send completion notification

### Service Updates

```bash
# Rolling update for zero-downtime
kubectl set image deployment/normalization-service \
  normalization-service=carbon-ingestion/normalization:v1.1.0 \
  -n carbon-ingestion

# Monitor rollout
kubectl rollout status deployment/normalization-service -n carbon-ingestion

# Rollback if needed
kubectl rollout undo deployment/normalization-service -n carbon-ingestion
```

### Database Maintenance

```bash
# ClickHouse table optimization
clickhouse-client --query="OPTIMIZE TABLE carbon_ingestion.gold_ohlc_bars FINAL"

# Partition management
clickhouse-client --query="ALTER TABLE carbon_ingestion.gold_ohlc_bars DROP PARTITION '2024-01-01'"

# Vacuum old data
clickhouse-client --query="ALTER TABLE carbon_ingestion.gold_ohlc_bars DELETE WHERE aggregation_date < '2024-01-01'"
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Throughput**: Records processed per second
2. **Latency**: End-to-end processing time
3. **Error Rate**: Errors per service
4. **Quality Score**: Data quality metrics
5. **Consumer Lag**: Kafka consumer lag
6. **Resource Usage**: CPU, memory, disk

### Alert Response

#### Critical Alerts (P1)

- **ServiceDown**: Service unavailable for > 2 minutes
  - **Action**: Restart service immediately
  - **Escalate**: If restart fails

- **DataLoss**: Data loss detected
  - **Action**: Stop all writes, assess scope
  - **Escalate**: Immediately to Platform Lead

- **SecurityBreach**: Unauthorized access detected
  - **Action**: Isolate affected systems
  - **Escalate**: Immediately to Security Team

#### High Alerts (P2)

- **HighErrorRate**: Error rate > 10%
  - **Action**: Investigate logs, identify root cause
  - **Escalate**: If unresolved in 30 minutes

- **LowQualityScore**: Quality score < 0.5
  - **Action**: Check data sources, review validation rules
  - **Escalate**: If quality doesn't improve in 1 hour

- **HighLatency**: p95 latency > 10s
  - **Action**: Check resource usage, scale if needed
  - **Escalate**: If latency persists after scaling

### Dashboard Links

- **Pipeline Overview**: http://grafana:3000/d/pipeline-monitoring
- **Data Quality**: http://grafana:3000/d/data-quality
- **Service Health**: http://grafana:3000/d/carbon-ingestion-overview

## Contact Information

### Team Contacts

- **Platform Team**: platform@254carbon.com, Slack: #carbon-platform
- **Data Team**: data@254carbon.com, Slack: #carbon-data
- **Infrastructure**: infra@254carbon.com, Slack: #infrastructure
- **Security**: security@254carbon.com, Slack: #security

### On-Call Rotation

- **Primary**: Check PagerDuty schedule
- **Secondary**: Check PagerDuty schedule
- **Escalation**: Platform Lead

### External Contacts

- **CAISO Support**: support@caiso.com
- **MISO Support**: support@misoenergy.org
- **Cloud Provider**: AWS/GCP/Azure support portal

## Appendix

### Useful Commands

```bash
# Check all service health
for port in 8510 8511 8512 8513 8514; do
  echo "Checking port $port..."
  curl -f http://localhost:$port/health || echo "FAILED"
done

# View service logs
kubectl logs -f deployment/normalization-service -n carbon-ingestion --tail=100

# Check Kafka topics
kafka-topics --bootstrap-server kafka:9092 --list

# Check ClickHouse tables
clickhouse-client --query="SHOW TABLES FROM carbon_ingestion"

# Check Redis keys
redis-cli KEYS "*" | head -20

# Generate connector index
python scripts/generate_connectors_index.py

# Test connector
python scripts/connector_manager.py test caiso

# Deploy platform
./scripts/deploy.sh production kubernetes

# Rollback deployment
./scripts/rollback.sh kubernetes
```

### Log Locations

- **Docker Compose**: `docker-compose logs <service>`
- **Kubernetes**: `kubectl logs deployment/<service> -n carbon-ingestion`
- **Local Development**: `logs/<service>.log`

### Configuration Files

- **Normalization**: `configs/normalization_rules.yaml`
- **Enrichment**: `configs/enrichment_taxonomy.yaml`
- **Aggregation**: `configs/aggregation_policies.yaml`
- **Projection**: `configs/projection_policies.yaml`
- **Data Quality**: `configs/data_quality_rules.yaml`

---

**Last Updated**: 2025-10-06  
**Version**: 1.0.0  
**Owner**: Platform Team
