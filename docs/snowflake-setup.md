# Snowflake Setup Guide

## Phase 3: Snowflake Infrastructure Setup

### Step 8: Create Database, Schemas & Warehouse

Run the SQL script: `sql/01_database_setup.sql`

```sql
CREATE OR REPLACE DATABASE VIDEO_ANALYTICS;
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.RAW;
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.CLEAN;
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.ANALYTICS;

CREATE OR REPLACE WAREHOUSE PIPELINE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### Architecture: Layered Schema Design

```
VIDEO_ANALYTICS (Database)
├── RAW           → Raw JSON payloads from APIs
├── CLEAN         → Typed, deduplicated, standardized
└── ANALYTICS     → Aggregations, KPIs, reports
```

| Schema | Purpose | Table Naming |
|--------|---------|--------------|
| `RAW` | Landing zone for raw VARIANT data | `RAW_<SOURCE>_JSON` |
| `CLEAN` | Typed columns, deduped, standardized | `CLN_<ENTITY>` |
| `ANALYTICS` | Business-ready aggregations | `ANL_<MART>` |

### Warehouse Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| Size | XSMALL | Cost-effective for pipeline loads |
| Auto Suspend | 60 seconds | Minimize idle costs |
| Auto Resume | TRUE | Seamless on-demand activation |

---

### Step 9: Create Storage Integration for S3

Run the SQL script: `sql/02_storage_integration.sql`

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_int_motorola
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/SnowflakeS3IntegrationRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://motorola-api-snowflake-staging-<unique>/');
```

**Then retrieve the integration details:**

```sql
DESC INTEGRATION s3_int_motorola;
```

**Copy these values from the output:**

| Property | Use For |
|----------|---------|
| `STORAGE_AWS_IAM_USER_ARN` | Principal in AWS trust policy |
| `STORAGE_AWS_EXTERNAL_ID` | ExternalId condition in trust policy |

---

### Step 10: Update AWS IAM Role Trust Relationship (Critical!)

1. **Navigate**: AWS → IAM → Roles → `SnowflakeS3IntegrationRole`
2. **Click**: Trust relationships → Edit trust policy
3. **Replace with** (use values from Step 9):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

> See `infrastructure/iam-snowflake-trust-policy.json` for template.

---

---

### Step 11: Create File Format & External Stage

Run the SQL script: `sql/03_stage_and_format.sql`

```sql
USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT ff_jsonl TYPE = JSON;

CREATE OR REPLACE STAGE s3_stage_motorola
  URL = 's3://motorola-api-snowflake-staging-<unique>/'
  STORAGE_INTEGRATION = s3_int_motorola
  FILE_FORMAT = ff_jsonl;
```

---

### Step 12: Create RAW Tables (VARIANT-first)

Run the SQL script: `sql/04_raw_tables.sql`

```sql
CREATE OR REPLACE TABLE RAW.API_A_EVENTS (
  payload VARIANT,
  source STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING,
  file_name STRING
);

CREATE OR REPLACE TABLE RAW.API_B_EVENTS (
  payload VARIANT,
  source STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING,
  file_name STRING
);
```

| Column | Type | Purpose |
|--------|------|---------|
| `payload` | VARIANT | Raw JSON payload |
| `source` | STRING | Source identifier |
| `ingested_at` | TIMESTAMP_NTZ | Load timestamp |
| `batch_id` | STRING | Batch ID from path |
| `file_name` | STRING | Source S3 file path |

---

### Step 13: Create Snowpipe Pipes

Run the SQL script: `sql/05_snowpipes.sql`

```sql
CREATE OR REPLACE PIPE pipe_api_a AUTO_INGEST = FALSE AS
COPY INTO RAW.API_A_EVENTS (payload, source, batch_id, file_name)
FROM (
  SELECT
    $1,
    'api_a',
    SPLIT_PART(METADATA$FILENAME,'batch_id=',2),
    METADATA$FILENAME
  FROM @s3_stage_motorola/source=api_a/
)
FILE_FORMAT = (FORMAT_NAME = ff_jsonl);
```

> **Note**: `AUTO_INGEST = FALSE` means pipes are triggered by Airflow, not S3 events.

**Trigger pipe refresh:**
```sql
ALTER PIPE pipe_api_a REFRESH;
SELECT SYSTEM$PIPE_STATUS('pipe_api_a');
```

---

---

## Phase 5: Load & Transform (RAW → CLEAN)

### Step 19: Test Snowpipe Refresh

Run the SQL script: `sql/06_refresh_pipes.sql`

```sql
-- Trigger pipe refresh
ALTER PIPE pipe_api_a REFRESH;
ALTER PIPE pipe_api_b REFRESH;

-- Verify data loaded
SELECT COUNT(*) FROM RAW.API_A_EVENTS;
SELECT COUNT(*) FROM RAW.API_B_EVENTS;
```

### Step 20: Build CLEAN Tables

Run the SQL script: `sql/07_clean_tables.sql`

**Pattern: Parse JSON + Dedupe with QUALIFY**

```sql
CREATE OR REPLACE TABLE CLEAN.CLN_API_A_EVENTS AS
SELECT
    payload:id::STRING AS id,
    payload:updated_at::TIMESTAMP_NTZ AS updated_at,
    payload:name::STRING AS name,
    -- ... more fields
    ingested_at,
    batch_id
FROM RAW.API_A_EVENTS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payload:id::STRING 
    ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC
) = 1;
```

**Incremental Updates**: Use `sql/08_clean_incremental.sql` for MERGE-based updates.

---

## Summary

| Step | Script | Objects Created |
|------|--------|-----------------|
| 8 | `01_database_setup.sql` | DATABASE, SCHEMAS, WAREHOUSE |
| 9 | `02_storage_integration.sql` | STORAGE INTEGRATION |
| 11 | `03_stage_and_format.sql` | FILE FORMAT, STAGE |
| 12 | `04_raw_tables.sql` | RAW tables |
| 13 | `05_snowpipes.sql` | Snowpipes |
| 19 | `06_refresh_pipes.sql` | (test refresh) |
| 20 | `07_clean_tables.sql` | CLEAN tables |
| 20+ | `08_clean_incremental.sql` | (incremental MERGE) |

## Next Steps

→ Phase 5 continued: Analytics Layer (Step 21)

