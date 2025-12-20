# Data Model Documentation

## Overview

The data warehouse follows a **medallion architecture** with three layers:

```
RAW (Bronze) → CLEAN (Silver) → ANALYTICS (Gold)
```

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **RAW** | Landing zone, raw JSON | As-is from source |
| **CLEAN** | Typed, deduplicated, standardized | Validated |
| **ANALYTICS** | Aggregated, business-ready | Curated |

---

## Database Structure

```
VIDEO_ANALYTICS (Database)
├── RAW (Schema)
│   ├── API_A_EVENTS
│   └── API_B_EVENTS
├── CLEAN (Schema)
│   ├── CLN_API_A_EVENTS
│   └── CLN_API_B_EVENTS
└── ANALYTICS (Schema)
    ├── VW_DAILY_EVENT_COUNTS
    ├── VW_DATA_FRESHNESS
    ├── VW_INGESTION_HISTORY
    ├── VW_TOP_CATEGORIES
    ├── VW_STATUS_DISTRIBUTION
    ├── VW_PRIORITY_DISTRIBUTION
    ├── VW_HOURLY_ACTIVITY
    ├── VW_EXECUTIVE_DASHBOARD
    ├── ANL_DAILY_SUMMARY
    └── DQ_CHECK_RESULTS
```

---

## RAW Layer

### Purpose
- Landing zone for raw API data
- Preserves original payload as VARIANT
- Includes ingestion metadata

### RAW.API_A_EVENTS

```sql
CREATE TABLE RAW.API_A_EVENTS (
    payload         VARIANT,            -- Raw JSON from API
    source          STRING,             -- 'api_a'
    ingested_at     TIMESTAMP_NTZ,      -- When loaded to Snowflake
    batch_id        STRING,             -- Pipeline batch identifier
    file_name       STRING              -- Source S3 file path
);
```

| Column | Type | Description |
|--------|------|-------------|
| `payload` | VARIANT | Complete JSON object from API |
| `source` | STRING | Source identifier ('api_a') |
| `ingested_at` | TIMESTAMP_NTZ | Snowflake load timestamp |
| `batch_id` | STRING | UUID linking to pipeline run |
| `file_name` | STRING | S3 path for traceability |

### RAW.API_B_EVENTS

Same structure as API_A_EVENTS with `source = 'api_b'`.

### Sample Query

```sql
-- View raw payload
SELECT 
    payload:id::STRING AS id,
    payload:name::STRING AS name,
    payload,
    ingested_at
FROM RAW.API_A_EVENTS
LIMIT 10;
```

---

## CLEAN Layer

### Purpose
- Parse JSON into typed columns
- Deduplicate by primary key
- Standardize timestamps and formats
- Apply business logic

### CLEAN.CLN_API_A_EVENTS

```sql
CREATE TABLE CLEAN.CLN_API_A_EVENTS (
    -- Primary Key
    id              STRING,
    
    -- Timestamps
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    
    -- Core Fields
    name            STRING,
    status          STRING,
    event_type      STRING,
    value           NUMBER,
    description     STRING,
    
    -- Nested Fields (flattened)
    category        STRING,
    tags            ARRAY,
    
    -- Metadata
    ingested_at     TIMESTAMP_NTZ,
    batch_id        STRING,
    file_name       STRING,
    _loaded_at      TIMESTAMP_NTZ
);
```

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `id` | STRING | `payload:id` | Unique record identifier |
| `created_at` | TIMESTAMP_NTZ | `payload:created_at` | When record was created |
| `updated_at` | TIMESTAMP_NTZ | `payload:updated_at` | Last update timestamp |
| `name` | STRING | `payload:name` | Record name |
| `status` | STRING | `payload:status` | Current status |
| `event_type` | STRING | `payload:type` | Event type classification |
| `value` | NUMBER | `payload:value` | Numeric value |
| `description` | STRING | `payload:description` | Text description |
| `category` | STRING | `payload:metadata:category` | Nested category |
| `tags` | ARRAY | `payload:metadata:tags` | Array of tags |
| `_loaded_at` | TIMESTAMP_NTZ | `CURRENT_TIMESTAMP()` | CLEAN layer load time |

### CLEAN.CLN_API_B_EVENTS

```sql
CREATE TABLE CLEAN.CLN_API_B_EVENTS (
    -- Primary Key
    id              STRING,
    
    -- Timestamps
    created_at      TIMESTAMP_NTZ,
    modified_at     TIMESTAMP_NTZ,
    
    -- Core Fields
    title           STRING,
    state           STRING,
    priority        STRING,
    amount          NUMBER(18,2),
    notes           STRING,
    
    -- References
    user_id         STRING,
    user_name       STRING,
    
    -- Metadata
    ingested_at     TIMESTAMP_NTZ,
    batch_id        STRING,
    file_name       STRING,
    _loaded_at      TIMESTAMP_NTZ
);
```

### Deduplication Logic

```sql
-- Keep only the latest version of each record
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id 
    ORDER BY updated_at DESC NULLS LAST
) = 1
```

### Sample Queries

```sql
-- Count by status
SELECT status, COUNT(*) 
FROM CLEAN.CLN_API_A_EVENTS 
GROUP BY status;

-- Recent updates
SELECT * 
FROM CLEAN.CLN_API_A_EVENTS 
WHERE updated_at > DATEADD(DAY, -1, CURRENT_TIMESTAMP())
ORDER BY updated_at DESC;
```

---

## ANALYTICS Layer

### Purpose
- Pre-aggregated metrics
- Business KPIs
- Dashboard-ready views
- Data quality tracking

### Views

#### VW_DAILY_EVENT_COUNTS
Daily record counts by source.

```sql
SELECT source, event_date, event_count, unique_records
FROM ANALYTICS.VW_DAILY_EVENT_COUNTS;
```

| Column | Type | Description |
|--------|------|-------------|
| `source` | STRING | Data source (api_a, api_b) |
| `event_date` | DATE | Date of events |
| `event_count` | NUMBER | Total records |
| `unique_records` | NUMBER | Distinct IDs |

#### VW_DATA_FRESHNESS
Data freshness and lag metrics.

```sql
SELECT source, freshness_status, event_age_minutes, avg_ingestion_lag_minutes
FROM ANALYTICS.VW_DATA_FRESHNESS;
```

| Column | Type | Description |
|--------|------|-------------|
| `source` | STRING | Data source |
| `latest_event_time` | TIMESTAMP | Most recent event |
| `latest_ingestion_time` | TIMESTAMP | Most recent load |
| `event_age_minutes` | NUMBER | Minutes since latest event |
| `freshness_status` | STRING | HEALTHY / WARNING / STALE |
| `avg_ingestion_lag_minutes` | NUMBER | Average event-to-load time |

#### VW_INGESTION_HISTORY
Batch-level ingestion tracking.

| Column | Type | Description |
|--------|------|-------------|
| `source` | STRING | Data source |
| `batch_id` | STRING | Pipeline batch ID |
| `batch_start` | TIMESTAMP | First record ingested |
| `record_count` | NUMBER | Records in batch |

#### VW_TOP_CATEGORIES
Top categories by volume (API A).

| Column | Type | Description |
|--------|------|-------------|
| `category` | STRING | Category name |
| `event_count` | NUMBER | Total records |
| `pct_of_total` | NUMBER | Percentage of all records |

#### VW_EXECUTIVE_DASHBOARD
Combined summary view for dashboards.

| Column | Type | Description |
|--------|------|-------------|
| `source` | STRING | Data source |
| `total_records` | NUMBER | All-time count |
| `records_today` | NUMBER | Today's count |
| `freshness_status` | STRING | Health status |
| `events_last_7_days` | NUMBER | Weekly volume |

### Tables

#### ANL_DAILY_SUMMARY
Materialized daily summary (refreshed each run).

```sql
SELECT * FROM ANALYTICS.ANL_DAILY_SUMMARY
WHERE event_date >= DATEADD(DAY, -7, CURRENT_DATE());
```

#### DQ_CHECK_RESULTS
Data quality check history.

```sql
CREATE TABLE ANALYTICS.DQ_CHECK_RESULTS (
    check_id        STRING,
    check_name      STRING,         -- row_count_check, null_primary_key_check, etc.
    check_type      STRING,         -- completeness, validity, timeliness, uniqueness
    table_name      STRING,         -- Target table
    result_value    NUMBER,         -- Actual value
    threshold_value NUMBER,         -- Expected threshold
    passed          BOOLEAN,        -- Pass/fail
    error_message   STRING,         -- Error details if failed
    executed_at     TIMESTAMP_NTZ   -- When check ran
);
```

---

## Entity Relationship

```
┌─────────────────┐         ┌─────────────────┐
│ RAW.API_A_EVENTS│         │ RAW.API_B_EVENTS│
│   (VARIANT)     │         │   (VARIANT)     │
└────────┬────────┘         └────────┬────────┘
         │                           │
         │ Parse & Dedupe            │ Parse & Dedupe
         ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│CLN_API_A_EVENTS │         │CLN_API_B_EVENTS │
│   (Typed)       │         │   (Typed)       │
└────────┬────────┘         └────────┬────────┘
         │                           │
         └───────────┬───────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │   ANALYTICS VIEWS     │
         │  (Aggregated KPIs)    │
         └───────────────────────┘
```

---

## Data Lineage

| Target | Source | Transformation |
|--------|--------|----------------|
| RAW.API_A_EVENTS | S3 stage (Snowpipe) | Direct load |
| CLEAN.CLN_API_A_EVENTS | RAW.API_A_EVENTS | Parse JSON, dedupe |
| VW_DAILY_EVENT_COUNTS | CLEAN tables | GROUP BY date |
| VW_DATA_FRESHNESS | CLEAN tables | MAX timestamps, CASE |
| ANL_DAILY_SUMMARY | VW_DAILY_EVENT_COUNTS | Materialize |

---

## Retention Policy

| Layer | Retention | Rationale |
|-------|-----------|-----------|
| S3 Staging | 30 days | Reprocessing window |
| RAW | 90 days | Audit trail |
| CLEAN | Indefinite | Business data |
| ANALYTICS | Indefinite | Reporting history |

---

## Naming Conventions

| Object Type | Convention | Example |
|-------------|------------|---------|
| RAW table | `<SOURCE>_EVENTS` | `API_A_EVENTS` |
| CLEAN table | `CLN_<ENTITY>` | `CLN_API_A_EVENTS` |
| Analytics table | `ANL_<MART>` | `ANL_DAILY_SUMMARY` |
| Analytics view | `VW_<NAME>` | `VW_DATA_FRESHNESS` |
| Columns | `snake_case` | `created_at`, `user_id` |
| Metadata columns | `_prefix` | `_loaded_at`, `_batch_id` |

