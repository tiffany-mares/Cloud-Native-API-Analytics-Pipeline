# Architecture Overview

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         CLOUD-NATIVE API ANALYTICS PIPELINE                      │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   API A      │     │   API B      │     │  API C       │
│  (OAuth2)    │     │  (API Key)   │     │  (Optional)  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LAYER (Python)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │   auth/     │  │  clients/   │  │ transform/  │  │   utils/    │            │
│  │  OAuth2     │  │  API calls  │  │  Normalize  │  │  S3 Writer  │            │
│  │  API Key    │  │  Pagination │  │  Validate   │  │  Logging    │            │
│  └─────────────┘  │  Rate Limit │  │  Dedupe     │  └─────────────┘            │
│                   └─────────────┘  └─────────────┘                              │
└─────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           STAGING LAYER (S3)                                     │
│                                                                                  │
│   s3://bucket/source=api_a/dt=2025-12-19/hour=15/batch_id=abc123/part-0001.jsonl│
│   s3://bucket/source=api_b/dt=2025-12-19/hour=15/batch_id=abc123/part-0001.jsonl│
│                                                                                  │
│   • Partitioned by source/date/hour                                             │
│   • JSONL format (one JSON object per line)                                     │
│   • SSE-S3 encryption at rest                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼ (Snowpipe auto-ingest)
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SNOWFLAKE DATA WAREHOUSE                               │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                         RAW SCHEMA                                       │   │
│   │   • RAW.API_A_EVENTS (payload VARIANT, metadata)                        │   │
│   │   • RAW.API_B_EVENTS (payload VARIANT, metadata)                        │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                            │                                                     │
│                            ▼ (SQL transforms)                                    │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                        CLEAN SCHEMA                                      │   │
│   │   • CLEAN.CLN_API_A_EVENTS (typed, deduped)                             │   │
│   │   • CLEAN.CLN_API_B_EVENTS (typed, deduped)                             │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                            │                                                     │
│                            ▼ (aggregations)                                      │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                      ANALYTICS SCHEMA                                    │   │
│   │   • VW_DAILY_EVENT_COUNTS                                               │   │
│   │   • VW_DATA_FRESHNESS                                                   │   │
│   │   • VW_TOP_CATEGORIES                                                   │   │
│   │   • ANL_DAILY_SUMMARY                                                   │   │
│   │   • DQ_CHECK_RESULTS                                                    │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMERS                                              │
│   • BI Tools (Tableau, Looker, Power BI)                                        │
│   • Data Science / ML                                                           │
│   • Operational Dashboards                                                      │
│   • Ad-hoc SQL Analysis                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATION (Airflow)                                │
│                                                                                  │
│   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐          │
│   │ Ingest  │ → │ Refresh │ → │  Clean  │ → │Analytics│ → │   DQ    │          │
│   │  APIs   │   │  Pipes  │   │   SQL   │   │   SQL   │   │ Checks  │          │
│   └─────────┘   └─────────┘   └─────────┘   └─────────┘   └─────────┘          │
│                                                                                  │
│   Schedule: @hourly | Retries: 3 | Backoff: Exponential                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Data Sources

| Source | Auth Type | Pagination | Rate Limit |
|--------|-----------|------------|------------|
| API A | OAuth2 (client credentials) | Cursor-based | 100 req/min |
| API B | API Key (header) | Offset-based | 60 req/min |
| API C | Optional (SOAP) | - | - |

### 2. Ingestion Layer

**Location:** `src/`

| Module | Purpose |
|--------|---------|
| `auth/` | OAuth2 token refresh, API key handling |
| `clients/` | API wrappers with retry, rate limiting, pagination |
| `transform/` | JSON flattening, normalization, validation, deduplication |
| `utils/` | S3 writer, structured logging, file I/O |

**Key Features:**
- Automatic token refresh (OAuth2)
- Exponential backoff on failures
- Rate limit awareness (429 handling)
- Structured JSON logging
- Incremental extraction (since parameter)

### 3. Staging Layer (S3)

**Bucket:** `motorola-api-snowflake-staging-<unique>`

**Path Convention:**
```
source=<source>/dt=YYYY-MM-DD/hour=HH/batch_id=<uuid>/part-0001.jsonl
```

**Features:**
- Partitioned for efficient querying
- Versioning enabled
- Server-side encryption (SSE-S3)
- Lifecycle policies for retention

### 4. Snowflake Data Warehouse

**Database:** `VIDEO_ANALYTICS`

| Schema | Purpose | Naming Convention |
|--------|---------|-------------------|
| `RAW` | Landing zone for raw JSON | `RAW_<SOURCE>_JSON` |
| `CLEAN` | Typed, deduped, standardized | `CLN_<ENTITY>` |
| `ANALYTICS` | Aggregations, KPIs, reports | `ANL_<MART>`, `VW_<NAME>` |

**Loading:** Snowpipe (triggered by Airflow `ALTER PIPE REFRESH`)

### 5. Orchestration (Airflow)

**DAG:** `api_to_snowflake_pipeline`

**Schedule:** Hourly (`@hourly`)

**Task Flow:**
```
start → ingest_api_data_to_s3 → [refresh_snowpipe_api_a, refresh_snowpipe_api_b]
      → wait_for_snowpipe_load → run_clean_sql → run_analytics_sql
      → run_dq_checks_sql → validate_dq_results → publish_metrics → end
```

**Features:**
- Retries with exponential backoff
- Failure callbacks (logging, alerts)
- XCom for passing batch_id between tasks
- Data quality gates

### 6. Observability

| Component | Tool |
|-----------|------|
| Logs | Structured JSON (Python logging) |
| Metrics | Pipeline metrics in logs + Snowflake tables |
| Alerts | Airflow failure callbacks |
| Monitoring | Snowflake DQ_CHECK_RESULTS table |

---

## Data Flow

```
1. EXTRACT    APIs → Python clients fetch data with pagination
2. TRANSFORM  Normalize timestamps, validate, dedupe
3. LOAD       Write JSONL to S3 staging
4. INGEST     Snowpipe loads to RAW tables
5. CLEAN      SQL transforms RAW → CLEAN (typed, deduped)
6. AGGREGATE  SQL builds ANALYTICS views/tables
7. VALIDATE   DQ checks run, results stored
8. SERVE      BI tools query ANALYTICS layer
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Ingestion | Python 3.11, requests, boto3 |
| Staging | AWS S3 |
| Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.8 |
| CI/CD | GitHub Actions |
| Containerization | Docker, Docker Compose |

---

## Scalability Considerations

| Aspect | Current | Scaling Path |
|--------|---------|--------------|
| Warehouse | XSMALL | Increase size or use auto-scaling |
| Ingestion | Single process | Parallelize by source |
| Storage | Single bucket | Partition by date, lifecycle rules |
| Airflow | LocalExecutor | CeleryExecutor / KubernetesExecutor |

---

## Failure Modes & Recovery

| Failure Point | Impact | Recovery |
|---------------|--------|----------|
| API down | No new data | Automatic retry, backfill later |
| S3 unavailable | Can't stage | Retry with backoff |
| Snowpipe error | Data not loaded | Manual pipe refresh |
| Transform error | Stale CLEAN data | Fix SQL, re-run |
| Airflow down | No orchestration | Manual trigger when restored |

