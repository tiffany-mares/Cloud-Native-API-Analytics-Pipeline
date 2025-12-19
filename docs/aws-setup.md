# AWS Setup Guide (S3 + IAM)

## Phase 2: AWS Infrastructure Setup

### Step 4: Create S3 Staging Bucket

1. **Navigate**: AWS Console → S3 → Create bucket

2. **Configuration**:
   | Setting | Value |
   |---------|-------|
   | Bucket name | `motorola-api-snowflake-staging-<unique>` |
   | Block public access | ✅ ON (all options checked) |
   | Versioning | ✅ Enabled |
   | Encryption | SSE-S3 (Amazon S3 managed keys) |

3. **Create the bucket**

---

### Step 5: Staging Path Format

The pipeline writes files using this exact partitioning scheme:

```
source=<source>/dt=YYYY-MM-DD/hour=HH/batch_id=<id>/part-0001.jsonl
```

**Example paths:**
```
source=api_a/dt=2025-12-17/hour=15/batch_id=abc123/part-0001.jsonl
source=api_b/dt=2025-12-17/hour=16/batch_id=def456/part-0001.jsonl
source=weather_api/dt=2025-12-18/hour=09/batch_id=xyz789/part-0001.jsonl
```

**Partition breakdown:**
| Partition | Purpose |
|-----------|---------|
| `source=` | Data source identifier |
| `dt=` | Date partition (YYYY-MM-DD) |
| `hour=` | Hour partition (00-23) |
| `batch_id=` | Unique batch identifier for traceability |

---

### Step 6: Create IAM Policy for Snowflake

1. **Navigate**: IAM → Policies → Create policy → JSON tab

2. **Paste the policy** (see `infrastructure/iam-snowflake-s3-policy.json`)

3. **Name**: `SnowflakeReadS3StagingPolicy`

4. **Description**: "Allows Snowflake to read objects from the API staging bucket"

---

---

### Step 7: Create IAM Role for Snowflake

1. **Navigate**: IAM → Roles → Create role

2. **Trusted entity**: Select "AWS account" (temporary - will update later)

3. **Attach policy**: `SnowflakeReadS3StagingPolicy`

4. **Role name**: `SnowflakeS3IntegrationRole`

5. **Create the role**

> ⚠️ **Important**: After creating the Snowflake storage integration (Phase 3), you'll need to update this role's trust relationship with the External ID provided by Snowflake.

---

## Next Steps

After completing AWS setup:
1. Note your bucket name for Snowflake storage integration
2. Note the IAM role ARN: `arn:aws:iam::<account-id>:role/SnowflakeS3IntegrationRole`
3. Proceed to Phase 3: Snowflake Setup
4. Update `.env` with your S3 bucket name

