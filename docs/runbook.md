# Pipeline Runbook & Troubleshooting Guide

## Quick Reference

| Issue | Quick Fix |
|-------|-----------|
| API 429 (Rate Limit) | Wait for backoff, check rate limits |
| API Auth Error | Rotate credentials, refresh token |
| Snowpipe Not Loading | Check stage files, refresh pipe |
| Schema Drift | Update mapping, bump version |
| Airflow Task Failed | Check logs, retry or backfill |
| Data Quality Failed | Investigate DQ table, fix source |

---

## Issue 1: API 429 — Rate Limited

### Symptoms
- API returns HTTP 429 status code
- Logs show: `"Rate limited (429), waiting Xs"`
- Ingestion task takes longer than expected

### Cause
- Exceeded API rate limits (requests per minute/hour)
- Multiple concurrent runs hitting same API
- Backfill jobs consuming quota

### Resolution

**Step 1: Check current rate limit configuration**
```python
# In src/clients/api_a_client.py
rate_limit_requests: int = 100  # requests per period
rate_limit_period: int = 60     # seconds
```

**Step 2: Review logs for retry pattern**
```bash
# Search logs for rate limit events
grep "429" logs/scheduler/*.log
```

**Step 3: Adjust rate limits if needed**
```python
# Reduce request rate
client = ApiAClient(
    rate_limit_requests=50,  # Reduce from 100
    rate_limit_period=60,
)
```

**Step 4: Check for concurrent runs**
```bash
# In Airflow UI: check for parallel DAG runs
# Set max_active_runs=1 in DAG definition
```

### Prevention
- Set conservative rate limits (50% of API max)
- Use `max_active_runs=1` to prevent concurrent runs
- Implement request queuing for high-volume sources

---

## Issue 2: API Authentication Error

### Symptoms
- HTTP 401 or 403 errors
- Logs show: `"Authentication failed"` or `"Invalid token"`
- Token refresh failures

### Cause
- Expired OAuth2 token
- Revoked API credentials
- Incorrect client ID/secret
- Token URL changed

### Resolution

**For OAuth2 APIs (API A):**

**Step 1: Verify credentials in environment**
```bash
# Check .env file
cat .env | grep API_A_OAUTH
```

**Step 2: Test token manually**
```bash
curl -X POST https://api.example.com/oauth/token \
  -d "grant_type=client_credentials" \
  -d "client_id=$API_A_OAUTH_CLIENT_ID" \
  -d "client_secret=$API_A_OAUTH_CLIENT_SECRET"
```

**Step 3: Rotate credentials if compromised**
1. Generate new credentials in API provider portal
2. Update `.env` with new values
3. Restart Airflow workers: `docker-compose restart`

**Step 4: Force token refresh**
```python
# In Python console
from src.auth.oauth2 import OAuth2Client
client = OAuth2Client(...)
client._token_info = None  # Clear cached token
client.get_access_token()  # Force new token
```

**For API Key APIs (API B):**

**Step 1: Verify API key**
```bash
curl -H "X-API-Key: $API_B_API_KEY" https://api.example.com/v2/health
```

**Step 2: Regenerate key if needed**
1. Go to API provider dashboard
2. Revoke old key, generate new key
3. Update `.env`: `API_B_API_KEY=new_key_here`
4. Restart services

### Prevention
- Set up credential rotation schedule (90 days)
- Monitor for auth failures in alerts
- Use secrets manager (AWS Secrets Manager, Vault)

---

## Issue 3: Snowpipe Didn't Load Data

### Symptoms
- S3 files exist but RAW tables are empty
- `SELECT COUNT(*) FROM RAW.API_A_EVENTS` returns 0
- Pipe status shows errors

### Cause
- Pipe not refreshed after file upload
- File format mismatch
- S3 permissions issue
- Files in wrong path pattern

### Resolution

**Step 1: Verify files exist in stage**
```sql
-- List files in stage
LIST @s3_stage_motorola/source=api_a/;

-- Check specific date partition
LIST @s3_stage_motorola/source=api_a/dt=2025-12-19/;
```

**Step 2: Check pipe status**
```sql
-- Get pipe status
SELECT SYSTEM$PIPE_STATUS('pipe_api_a');

-- Expected: {"executionState":"RUNNING",...}
```

**Step 3: Manually refresh pipe**
```sql
ALTER PIPE pipe_api_a REFRESH;
```

**Step 4: Check copy history for errors**
```sql
-- View recent copy operations
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'API_A_EVENTS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

**Step 5: Check for load errors**
```sql
-- Find failed loads
SELECT 
    FILE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUM
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...))
WHERE STATUS = 'LOAD_FAILED';
```

**Step 6: Test manual COPY**
```sql
-- Test loading a specific file
COPY INTO RAW.API_A_EVENTS
FROM @s3_stage_motorola/source=api_a/dt=2025-12-19/hour=15/
FILE_FORMAT = (FORMAT_NAME = ff_jsonl)
ON_ERROR = CONTINUE;
```

**Common Errors & Fixes:**

| Error | Fix |
|-------|-----|
| `"Access Denied"` | Update IAM role trust policy |
| `"File format error"` | Check JSON is valid, one object per line |
| `"Column mismatch"` | Verify COPY INTO column mapping |
| `"No files found"` | Check path pattern in pipe definition |

### Prevention
- Monitor Snowpipe status in DQ checks
- Set up alerts for empty loads
- Validate file format before upload

---

## Issue 4: Schema Drift Detected

### Symptoms
- New fields appearing in API responses
- Missing expected fields
- Type mismatches in CLEAN layer
- DQ checks failing on schema validation

### Cause
- API provider added new fields
- API version upgrade
- Field renamed or deprecated
- Data type changed

### Resolution

**Step 1: Identify the drift**
```sql
-- Compare RAW payload keys to CLEAN columns
SELECT DISTINCT 
    f.key AS raw_field,
    CASE WHEN c.column_name IS NULL THEN 'NEW' ELSE 'EXISTS' END AS status
FROM RAW.API_A_EVENTS r,
     LATERAL FLATTEN(input => r.payload) f
LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
    ON c.TABLE_NAME = 'CLN_API_A_EVENTS'
    AND UPPER(f.key) = UPPER(c.COLUMN_NAME)
WHERE r.ingested_at > DATEADD(DAY, -1, CURRENT_TIMESTAMP())
ORDER BY status DESC, raw_field;
```

**Step 2: Update CLEAN table mapping**

Edit `sql/07_clean_tables.sql`:
```sql
-- Add new field to CLEAN table
CREATE OR REPLACE TABLE CLEAN.CLN_API_A_EVENTS AS
SELECT
    payload:id::STRING AS id,
    payload:new_field::STRING AS new_field,  -- NEW FIELD
    -- ... existing fields
FROM RAW.API_A_EVENTS
QUALIFY ROW_NUMBER() OVER (...) = 1;
```

**Step 3: Bump schema version**

Update in pipeline metadata:
```python
# In src/utils/s3_writer.py or config
SCHEMA_VERSION = "1.1"  # Was "1.0"
```

**Step 4: Update validation if needed**
```python
# In transform_records call
valid_records, invalid_records = transform_records(
    raw_records,
    required_fields=["id", "new_required_field"],  # Add if required
    ...
)
```

**Step 5: Deploy changes**
```bash
# Run updated SQL
snowsql -f sql/07_clean_tables.sql

# Or trigger via Airflow
airflow dags trigger api_to_snowflake_pipeline
```

**Step 6: Document the change**
- Add entry to CHANGELOG.md
- Update data dictionary in docs/
- Notify downstream consumers

### Prevention
- Monitor for new fields in DQ checks
- Subscribe to API changelog notifications
- Version your schema mappings
- Test schema changes in staging first

---

## Issue 5: Airflow Task Failed

### Symptoms
- Task shows red (failed) in Airflow UI
- Email/Slack notification received
- Downstream tasks not running

### Resolution

**Step 1: Check task logs**
```bash
# Via Airflow UI: Click task → Log

# Or via CLI
docker-compose exec airflow-webserver \
  airflow tasks logs api_to_snowflake_pipeline ingest_api_data_to_s3 2025-12-19
```

**Step 2: Identify error type**

| Error Pattern | Likely Cause |
|---------------|--------------|
| `ConnectionError` | Network/API down |
| `SnowflakeError` | Snowflake issue |
| `S3UploadError` | AWS permissions |
| `ValidationError` | Data quality issue |

**Step 3: Retry failed task**
```bash
# Via UI: Click task → Clear → Confirm

# Via CLI
airflow tasks clear api_to_snowflake_pipeline \
  -t ingest_api_data_to_s3 \
  -s 2025-12-19
```

**Step 4: Backfill if needed**
```bash
airflow dags backfill api_to_snowflake_pipeline \
  -s 2025-12-18 \
  -e 2025-12-19
```

---

## Issue 6: Data Quality Check Failed

### Symptoms
- `validate_dq_results` task failed
- DQ_CHECK_RESULTS table shows failures
- Alerts triggered

### Resolution

**Step 1: Query failed checks**
```sql
SELECT 
    check_name,
    table_name,
    result_value,
    threshold_value,
    error_message,
    executed_at
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE passed = FALSE
  AND executed_at > DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY executed_at DESC;
```

**Step 2: Investigate by check type**

**Freshness Failed:**
```sql
-- Check when data last arrived
SELECT MAX(ingested_at) FROM CLEAN.CLN_API_A_EVENTS;

-- Check if source API is down
-- → Run manual API health check
```

**Row Count = 0:**
```sql
-- Check RAW layer
SELECT COUNT(*) FROM RAW.API_A_EVENTS;

-- If RAW has data but CLEAN is empty, check transform
-- → Review 07_clean_tables.sql for errors
```

**Null Primary Keys:**
```sql
-- Find records with null IDs
SELECT * FROM CLEAN.CLN_API_A_EVENTS WHERE id IS NULL LIMIT 10;

-- Check source data
SELECT payload FROM RAW.API_A_EVENTS 
WHERE payload:id IS NULL LIMIT 10;
```

**Duplicates Found:**
```sql
-- Find duplicate IDs
SELECT id, COUNT(*) AS cnt
FROM CLEAN.CLN_API_A_EVENTS
GROUP BY id
HAVING cnt > 1;

-- Check if QUALIFY is working
-- → Re-run 07_clean_tables.sql
```

---

## Escalation Path

| Severity | Response Time | Escalate To |
|----------|---------------|-------------|
| P1 - Data Loss | 15 min | On-call engineer |
| P2 - Pipeline Down | 1 hour | Data engineering team |
| P3 - Delayed Data | 4 hours | Data engineering team |
| P4 - Minor Issue | 24 hours | Backlog/ticket |

---

## Useful Commands

```bash
# Airflow
docker-compose logs -f airflow-scheduler
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow tasks list api_to_snowflake_pipeline

# Snowflake (via snowsql)
snowsql -q "SELECT SYSTEM\$PIPE_STATUS('pipe_api_a')"
snowsql -q "SHOW PIPES"

# AWS S3
aws s3 ls s3://motorola-api-snowflake-staging-xxx/source=api_a/ --recursive
aws s3 cp s3://bucket/path/file.jsonl - | head -5

# Python ingestion test
python -m src.ingest_to_s3 --source api_a --log-level DEBUG
```

---

## Monitoring Queries

```sql
-- Pipeline health dashboard
SELECT * FROM ANALYTICS.VW_DATA_FRESHNESS;

-- Recent DQ check summary
SELECT
    DATE(executed_at) AS check_date,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE executed_at > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY DATE(executed_at)
ORDER BY check_date DESC;

-- Ingestion volume trend
SELECT * FROM ANALYTICS.VW_DAILY_EVENT_COUNTS
WHERE event_date > DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY event_date DESC;
```

