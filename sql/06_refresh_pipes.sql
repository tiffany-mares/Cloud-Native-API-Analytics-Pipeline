-- ============================================
-- Phase 5: Test Snowpipe Refresh
-- Step 19: Trigger Snowpipe refresh manually
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE PIPELINE_WH;

-- ============================================
-- Refresh pipes to load staged files
-- ============================================

-- Refresh API A pipe
ALTER PIPE pipe_api_a REFRESH;

-- Refresh API B pipe
ALTER PIPE pipe_api_b REFRESH;

-- ============================================
-- Check pipe status
-- ============================================

SELECT SYSTEM$PIPE_STATUS('pipe_api_a') AS api_a_status;
SELECT SYSTEM$PIPE_STATUS('pipe_api_b') AS api_b_status;

-- ============================================
-- Verify data loaded
-- ============================================

-- Wait a few seconds for Snowpipe to process, then check counts
SELECT 'API_A_EVENTS' AS table_name, COUNT(*) AS row_count FROM RAW.API_A_EVENTS
UNION ALL
SELECT 'API_B_EVENTS' AS table_name, COUNT(*) AS row_count FROM RAW.API_B_EVENTS;

-- ============================================
-- Sample data inspection
-- ============================================

-- Preview API A raw data
SELECT 
    payload,
    source,
    ingested_at,
    batch_id,
    file_name
FROM RAW.API_A_EVENTS
LIMIT 5;

-- Preview API B raw data
SELECT 
    payload,
    source,
    ingested_at,
    batch_id,
    file_name
FROM RAW.API_B_EVENTS
LIMIT 5;

-- ============================================
-- Check for load errors
-- ============================================

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'API_A_EVENTS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'Loaded'
ORDER BY LAST_LOAD_TIME DESC;

