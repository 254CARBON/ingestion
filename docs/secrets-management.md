# Secrets Management Guide

## Overview

This document describes the secrets management approach for the 254Carbon ingestion platform, including best practices for handling sensitive configuration data across different environments.

## Secrets Categories

### 1. API Keys and Credentials
- CAISO API key
- MISO API key
- Other market data provider credentials

### 2. Database Credentials
- ClickHouse username and password
- Redis password (if authentication enabled)

### 3. Service-to-Service Authentication
- Kafka SASL credentials
- Schema Registry credentials
- mTLS certificates and keys

### 4. Observability Credentials
- Grafana admin password
- Prometheus remote write credentials
- Jaeger authentication tokens

## Environment-Specific Approaches

### Development Environment

**Approach**: Local `.env` files (not committed to git)

1. Copy the example file:
   ```bash
   cp configs/environments/development/env.example .env
   ```

2. Edit `.env` with actual credentials:
   ```bash
   # Edit the file
   vim .env
   
   # Set restrictive permissions
   chmod 600 .env
   ```

3. Load environment variables:
   ```bash
   source .env
   # or
   export $(cat .env | xargs)
   ```

**Security Notes**:
- Never commit `.env` files to version control
- Add `.env` to `.gitignore`
- Use different credentials than production
- Rotate credentials regularly

### Staging Environment

**Approach**: Kubernetes Secrets or HashiCorp Vault

#### Using Kubernetes Secrets

1. Create secrets from file:
   ```bash
   kubectl create secret generic carbon-ingestion-secrets \
     --from-env-file=configs/environments/staging/env.example \
     --namespace=carbon-ingestion \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

2. Reference in deployment:
   ```yaml
   envFrom:
     - secretRef:
         name: carbon-ingestion-secrets
   ```

#### Using HashiCorp Vault

1. Store secrets in Vault:
   ```bash
   vault kv put secret/carbon-ingestion/staging \
     caiso_api_key="..." \
     miso_api_key="..." \
     clickhouse_password="..."
   ```

2. Use Vault Agent or CSI driver to inject secrets

### Production Environment

**Approach**: HashiCorp Vault + Kubernetes Secrets Operator

#### Recommended Setup

1. **Vault Configuration**:
   ```bash
   # Enable KV secrets engine
   vault secrets enable -path=carbon-ingestion kv-v2
   
   # Create policy
   vault policy write carbon-ingestion-read - <<EOF
   path "carbon-ingestion/data/production/*" {
     capabilities = ["read", "list"]
   }
   EOF
   
   # Store secrets
   vault kv put carbon-ingestion/production/connectors \
     caiso_api_key="ACTUAL_KEY" \
     miso_api_key="ACTUAL_KEY"
   
   vault kv put carbon-ingestion/production/databases \
     clickhouse_password="ACTUAL_PASSWORD" \
     redis_password="ACTUAL_PASSWORD"
   ```

2. **Kubernetes Integration**:
   ```yaml
   # Use Vault Secrets Operator
   apiVersion: secrets.hashicorp.com/v1beta1
   kind: VaultStaticSecret
   metadata:
     name: carbon-ingestion-secrets
     namespace: carbon-ingestion
   spec:
     type: kv-v2
     mount: carbon-ingestion
     path: production/connectors
     destination:
       name: carbon-ingestion-secrets
       create: true
   ```

3. **Service Configuration**:
   ```yaml
   envFrom:
     - secretRef:
         name: carbon-ingestion-secrets
   ```

## Secret Rotation

### Rotation Schedule

| Secret Type | Rotation Frequency | Owner |
|-------------|-------------------|-------|
| API Keys | 90 days | Platform Team |
| Database Passwords | 60 days | Platform Team |
| Service Certificates | 30 days (automated) | Security Team |
| Admin Credentials | 30 days | Security Team |

### Rotation Procedure

1. **Pre-Rotation**:
   - Generate new credentials
   - Test new credentials in staging
   - Schedule maintenance window (if needed)

2. **Rotation**:
   ```bash
   # Update Vault secret
   vault kv put carbon-ingestion/production/connectors \
     caiso_api_key="NEW_KEY"
   
   # Trigger secret refresh (if using Vault Operator)
   kubectl annotate vaultstaticsecret carbon-ingestion-secrets \
     secrets.hashicorp.com/refresh="$(date +%s)" \
     -n carbon-ingestion
   
   # Restart services to pick up new secrets
   kubectl rollout restart deployment -n carbon-ingestion
   ```

3. **Post-Rotation**:
   - Verify services are healthy
   - Monitor for authentication errors
   - Revoke old credentials after grace period

## Secrets Validation

### Startup Validation

All services validate required secrets on startup:

```python
def validate_secrets():
    """Validate that all required secrets are present."""
    required_secrets = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "CLICKHOUSE_DSN",
        "REDIS_URL"
    ]
    
    missing = [s for s in required_secrets if not os.getenv(s)]
    
    if missing:
        raise ValueError(f"Missing required secrets: {missing}")
```

### Runtime Validation

Services periodically validate that credentials are still valid:

```python
async def validate_credentials():
    """Validate credentials are still valid."""
    try:
        # Test database connection
        await clickhouse_client.ping()
        
        # Test Redis connection
        await redis_client.ping()
        
        # Test Kafka connection
        await kafka_producer.bootstrap()
        
        return True
    except Exception as e:
        logger.error("Credential validation failed", error=str(e))
        return False
```

## Best Practices

### 1. Principle of Least Privilege
- Grant minimum necessary permissions
- Use separate credentials for each service
- Implement read-only credentials where possible

### 2. Encryption
- Encrypt secrets at rest (Vault, Kubernetes Secrets with encryption)
- Use TLS for all secret transmission
- Never log secrets or include in error messages

### 3. Auditing
- Log all secret access attempts
- Monitor for unauthorized access
- Alert on failed authentication attempts

### 4. Separation of Concerns
- Different secrets for different environments
- No shared credentials between environments
- Separate admin and service credentials

### 5. Emergency Procedures
- Document emergency rotation procedures
- Maintain break-glass access procedures
- Keep backup admin credentials in secure location

## Troubleshooting

### Issue: Service fails to start with authentication error

**Solution**:
1. Check if secret exists:
   ```bash
   kubectl get secret carbon-ingestion-secrets -n carbon-ingestion
   ```

2. Verify secret contents (base64 decode):
   ```bash
   kubectl get secret carbon-ingestion-secrets -n carbon-ingestion -o yaml
   ```

3. Check service logs for specific error:
   ```bash
   kubectl logs deployment/normalization-service -n carbon-ingestion
   ```

### Issue: Credentials expired or revoked

**Solution**:
1. Rotate credentials immediately
2. Update Vault/Kubernetes secrets
3. Restart affected services
4. Monitor for continued errors

### Issue: Secret not propagating to pods

**Solution**:
1. Check Vault Operator status
2. Verify RBAC permissions
3. Force pod restart:
   ```bash
   kubectl delete pod -l app=normalization-service -n carbon-ingestion
   ```

## Security Checklist

- [ ] All secrets stored in Vault or Kubernetes Secrets
- [ ] No secrets in source code or configuration files
- [ ] `.env` files added to `.gitignore`
- [ ] Secrets encrypted at rest
- [ ] TLS enabled for all secret transmission
- [ ] Rotation schedule defined and automated
- [ ] Access audit logging enabled
- [ ] Emergency procedures documented
- [ ] Secrets validated on service startup
- [ ] Monitoring alerts configured for auth failures

## Contact

For secrets management issues or questions:
- Platform Team: platform@254carbon.com
- Security Team: security@254carbon.com
- Emergency: Use PagerDuty escalation
