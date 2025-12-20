# Security Model

## Overview

This document describes the security controls implemented across the pipeline.

```
┌─────────────────────────────────────────────────────────────────┐
│                     SECURITY LAYERS                              │
├─────────────────────────────────────────────────────────────────┤
│  1. Authentication   │ OAuth2, API Keys, IAM Roles              │
│  2. Authorization    │ RBAC, Least Privilege, Policies          │
│  3. Encryption       │ TLS in transit, SSE-S3 at rest           │
│  4. Secrets Mgmt     │ Environment vars, no hardcoded creds     │
│  5. Audit            │ Structured logs, access history          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. API Authentication

### OAuth2 (API A)

**Flow:** Client Credentials Grant

```
┌──────────┐                              ┌──────────┐
│  Client  │──── client_id + secret ────▶│  Token   │
│          │◀─────── access_token ───────│ Endpoint │
└──────────┘                              └──────────┘
```

**Implementation:**
```python
# src/auth/oauth2.py
class OAuth2Client:
    - Automatic token refresh before expiry
    - Token expiry buffer (60 seconds)
    - Secure token storage in memory only
```

**Security Controls:**
- ✅ Tokens refreshed automatically before expiry
- ✅ Client secret never logged
- ✅ HTTPS only for token endpoint
- ✅ Token stored in memory, not disk

### API Key (API B)

**Implementation:**
```python
# src/auth/api_key.py
class APIKeyAuth:
    - Header-based authentication
    - Key name configurable
```

**Security Controls:**
- ✅ API key passed via header (not URL)
- ✅ Key never logged
- ✅ HTTPS only

---

## 2. AWS IAM Configuration

### S3 Bucket Policy

**Bucket:** `motorola-api-snowflake-staging-<unique>`

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::motorola-api-snowflake-staging-*",
                "arn:aws:s3:::motorola-api-snowflake-staging-*/*"
            ],
            "Condition": {
                "Bool": {"aws:SecureTransport": "false"}
            }
        }
    ]
}
```

### IAM Policy: SnowflakeReadS3StagingPolicy

**Principle of Least Privilege:** Only allows read operations.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListBucket",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::motorola-api-snowflake-staging-<unique>"]
        },
        {
            "Sid": "ReadObjects",
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::motorola-api-snowflake-staging-<unique>/*"]
        }
    ]
}
```

**NOT Allowed:**
- ❌ `s3:PutObject` - Snowflake doesn't need to write
- ❌ `s3:DeleteObject` - No deletion required
- ❌ Access to other buckets

### IAM Role: SnowflakeS3IntegrationRole

**Trust Policy:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<SNOWFLAKE_IAM_USER_ARN>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<SNOWFLAKE_EXTERNAL_ID>"
                }
            }
        }
    ]
}
```

**Security Features:**
- ✅ External ID prevents confused deputy attack
- ✅ Only Snowflake's specific IAM user can assume
- ✅ No wildcard principals

### IAM Policy: PipelineIngestionPolicy

For the ingestion service to write to S3:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "WriteToStaging",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::motorola-api-snowflake-staging-<unique>/*"
        },
        {
            "Sid": "ListBucket",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::motorola-api-snowflake-staging-<unique>"
        }
    ]
}
```

---

## 3. Snowflake Security

### Role-Based Access Control (RBAC)

```sql
-- Loader role (pipeline writes)
CREATE ROLE LOADER_ROLE;
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE VIDEO_ANALYTICS TO ROLE LOADER_ROLE;
GRANT USAGE ON SCHEMA VIDEO_ANALYTICS.RAW TO ROLE LOADER_ROLE;
GRANT INSERT ON ALL TABLES IN SCHEMA VIDEO_ANALYTICS.RAW TO ROLE LOADER_ROLE;

-- Transformer role (SQL transforms)
CREATE ROLE TRANSFORMER_ROLE;
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE VIDEO_ANALYTICS TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA VIDEO_ANALYTICS.RAW TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON SCHEMA VIDEO_ANALYTICS.CLEAN TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON SCHEMA VIDEO_ANALYTICS.ANALYTICS TO ROLE TRANSFORMER_ROLE;

-- Analyst role (read-only)
CREATE ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE VIDEO_ANALYTICS TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA VIDEO_ANALYTICS.CLEAN TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA VIDEO_ANALYTICS.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA VIDEO_ANALYTICS.CLEAN TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA VIDEO_ANALYTICS.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA VIDEO_ANALYTICS.ANALYTICS TO ROLE ANALYST_ROLE;
```

### Role Hierarchy

```
ACCOUNTADMIN
     │
     ▼
  SYSADMIN
     │
     ├──▶ LOADER_ROLE (pipeline service account)
     │
     ├──▶ TRANSFORMER_ROLE (Airflow transforms)
     │
     └──▶ ANALYST_ROLE (BI users)
```

### Data Masking (Optional)

For sensitive fields:

```sql
-- Create masking policy
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') 
        THEN REGEXP_REPLACE(val, '.+@', '***@')
        ELSE val
    END;

-- Apply to column
ALTER TABLE CLEAN.CLN_API_A_EVENTS 
MODIFY COLUMN email SET MASKING POLICY email_mask;
```

---

## 4. Secrets Management

### Environment Variables

**Never commit credentials to code!**

```bash
# .env (gitignored)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=xxxxx
SNOWFLAKE_PASSWORD=xxxxx
API_A_OAUTH_CLIENT_SECRET=xxxxx
API_B_API_KEY=xxxxx
```

### .gitignore Protection

```gitignore
# Never commit
.env
.env.local
*.pem
*.key
secrets/
credentials/
```

### Production: Use Secrets Manager

For production deployments, use:

| Platform | Service |
|----------|---------|
| AWS | Secrets Manager, Parameter Store |
| Azure | Key Vault |
| GCP | Secret Manager |
| K8s | Kubernetes Secrets |
| Airflow | Connections, Variables (encrypted) |

**Example: AWS Secrets Manager**
```python
import boto3

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return response['SecretString']
```

---

## 5. Encryption

### In Transit

| Connection | Encryption |
|------------|------------|
| API calls | HTTPS (TLS 1.2+) |
| S3 uploads | HTTPS |
| Snowflake | TLS |
| Airflow UI | HTTPS (configure reverse proxy) |

### At Rest

| Storage | Encryption |
|---------|------------|
| S3 | SSE-S3 (AES-256) |
| Snowflake | Automatic (AES-256) |
| Airflow metadata DB | PostgreSQL encryption |

**S3 Bucket Encryption:**
```bash
aws s3api put-bucket-encryption \
    --bucket motorola-api-snowflake-staging-xxx \
    --server-side-encryption-configuration '{
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }'
```

---

## 6. Network Security

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        VPC                                   │
│  ┌─────────────────┐     ┌─────────────────┐               │
│  │  Private Subnet │     │  Private Subnet │               │
│  │  ┌───────────┐  │     │  ┌───────────┐  │               │
│  │  │  Airflow  │  │     │  │  Airflow  │  │               │
│  │  │  Workers  │  │     │  │  Scheduler│  │               │
│  │  └───────────┘  │     │  └───────────┘  │               │
│  └────────┬────────┘     └────────┬────────┘               │
│           │                       │                         │
│           └───────────┬───────────┘                         │
│                       │                                     │
│           ┌───────────▼───────────┐                         │
│           │     NAT Gateway       │                         │
│           └───────────┬───────────┘                         │
└───────────────────────┼─────────────────────────────────────┘
                        │
                        ▼
            ┌───────────────────────┐
            │   Internet Gateway    │
            └───────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      ┌───────┐    ┌───────┐    ┌───────────┐
      │ APIs  │    │  S3   │    │ Snowflake │
      └───────┘    └───────┘    └───────────┘
```

### Snowflake Network Policies

```sql
-- Restrict access to known IPs
CREATE OR REPLACE NETWORK POLICY pipeline_network_policy
    ALLOWED_IP_LIST = ('203.0.113.0/24', '198.51.100.0/24')
    BLOCKED_IP_LIST = ();

-- Apply to account
ALTER ACCOUNT SET NETWORK_POLICY = pipeline_network_policy;
```

---

## 7. Audit & Compliance

### Logging

All operations are logged with:
- Timestamp
- User/service identity
- Action performed
- Resource accessed
- Success/failure status

### Snowflake Access History

```sql
-- Query access history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY query_start_time DESC;
```

### S3 Access Logs

Enable S3 access logging:
```bash
aws s3api put-bucket-logging \
    --bucket motorola-api-snowflake-staging-xxx \
    --bucket-logging-status '{
        "LoggingEnabled": {
            "TargetBucket": "logs-bucket",
            "TargetPrefix": "s3-access-logs/"
        }
    }'
```

---

## 8. Security Checklist

### Development
- [ ] No credentials in code
- [ ] .env in .gitignore
- [ ] Use environment variables
- [ ] HTTPS for all API calls

### Deployment
- [ ] Use secrets manager
- [ ] Enable S3 encryption
- [ ] Configure IAM least privilege
- [ ] Set up Snowflake RBAC

### Monitoring
- [ ] Enable access logging
- [ ] Set up alerts for auth failures
- [ ] Review access periodically
- [ ] Rotate credentials regularly

### Incident Response
- [ ] Document credential rotation process
- [ ] Have revocation procedures ready
- [ ] Maintain contact list for security issues

