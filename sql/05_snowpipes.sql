-- ============================================
-- Phase 3: Snowpipe Configuration
-- Step 13: Create Snowpipe pipes (Airflow-triggered)
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA RAW;

-- ============================================
-- Pipe for API A
-- AUTO_INGEST = FALSE → Triggered by Airflow
-- ============================================
CREATE OR REPLACE PIPE pipe_api_a AUTO_INGEST = FALSE AS
COPY INTO RAW.API_A_EVENTS (payload, source, batch_id, file_name)
FROM (
  SELECT
    $1,
    'api_a',
    SPLIT_PART(METADATA$FILENAME, 'batch_id=', 2),
    METADATA$FILENAME
  FROM @s3_stage_motorola/source=api_a/
)
FILE_FORMAT = (FORMAT_NAME = ff_jsonl);

-- ============================================
-- Pipe for API B
-- AUTO_INGEST = FALSE → Triggered by Airflow
-- ============================================
CREATE OR REPLACE PIPE pipe_api_b AUTO_INGEST = FALSE AS
COPY INTO RAW.API_B_EVENTS (payload, source, batch_id, file_name)
FROM (
  SELECT
    $1,
    'api_b',
    SPLIT_PART(METADATA$FILENAME, 'batch_id=', 2),
    METADATA$FILENAME
  FROM @s3_stage_motorola/source=api_b/
)
FILE_FORMAT = (FORMAT_NAME = ff_jsonl);

-- ============================================
-- Verify pipes created
-- ============================================
SHOW PIPES;

-- ============================================
-- Usage: Trigger pipe refresh from Airflow
-- ============================================
-- To manually refresh a pipe (or call from Airflow):
-- ALTER PIPE pipe_api_a REFRESH;
-- ALTER PIPE pipe_api_b REFRESH;

-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('pipe_api_a');
-- SELECT SYSTEM$PIPE_STATUS('pipe_api_b');

