-- ============================================
-- Data Quality Checks
-- Step 24: Minimum DQ Check Set
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE WAREHOUSE PIPELINE_WH;

-- ============================================
-- DQ Results Table (stores check history)
-- ============================================

CREATE TABLE IF NOT EXISTS ANALYTICS.DQ_CHECK_RESULTS (
    check_id STRING DEFAULT UUID_STRING(),
    check_name STRING,
    check_type STRING,
    table_name STRING,
    check_sql STRING,
    result_value NUMBER,
    threshold_value NUMBER,
    passed BOOLEAN,
    error_message STRING,
    executed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    batch_id STRING
);

-- ============================================
-- CHECK 1: Row Count > 0 for each CLEAN table
-- ============================================

-- API A Events - Row Count Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'row_count_check' AS check_name,
    'completeness' AS check_type,
    'CLEAN.CLN_API_A_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    1 AS threshold_value,
    COUNT(*) > 0 AS passed,
    CASE WHEN COUNT(*) = 0 THEN 'Table is empty - no records found' ELSE NULL END AS error_message
FROM CLEAN.CLN_API_A_EVENTS;

-- API B Events - Row Count Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'row_count_check' AS check_name,
    'completeness' AS check_type,
    'CLEAN.CLN_API_B_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    1 AS threshold_value,
    COUNT(*) > 0 AS passed,
    CASE WHEN COUNT(*) = 0 THEN 'Table is empty - no records found' ELSE NULL END AS error_message
FROM CLEAN.CLN_API_B_EVENTS;

-- ============================================
-- CHECK 2: No NULL Primary Keys (ID field)
-- ============================================

-- API A Events - Null ID Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'null_primary_key_check' AS check_name,
    'validity' AS check_type,
    'CLEAN.CLN_API_A_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    0 AS threshold_value,
    COUNT(*) = 0 AS passed,
    CASE WHEN COUNT(*) > 0 THEN 'Found ' || COUNT(*) || ' records with NULL id' ELSE NULL END AS error_message
FROM CLEAN.CLN_API_A_EVENTS
WHERE id IS NULL;

-- API B Events - Null ID Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'null_primary_key_check' AS check_name,
    'validity' AS check_type,
    'CLEAN.CLN_API_B_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    0 AS threshold_value,
    COUNT(*) = 0 AS passed,
    CASE WHEN COUNT(*) > 0 THEN 'Found ' || COUNT(*) || ' records with NULL id' ELSE NULL END AS error_message
FROM CLEAN.CLN_API_B_EVENTS
WHERE id IS NULL;

-- ============================================
-- CHECK 3: Freshness - Latest ingested_at within X hours
-- ============================================

-- API A Events - Freshness Check (6 hour threshold)
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'freshness_check' AS check_name,
    'timeliness' AS check_type,
    'CLEAN.CLN_API_A_EVENTS' AS table_name,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) AS result_value,
    6 AS threshold_value,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) <= 6 AS passed,
    CASE 
        WHEN MAX(ingested_at) IS NULL THEN 'No data found - cannot check freshness'
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 6 
        THEN 'Data is ' || TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) || ' hours old (threshold: 6 hours)'
        ELSE NULL 
    END AS error_message
FROM CLEAN.CLN_API_A_EVENTS;

-- API B Events - Freshness Check (6 hour threshold)
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'freshness_check' AS check_name,
    'timeliness' AS check_type,
    'CLEAN.CLN_API_B_EVENTS' AS table_name,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) AS result_value,
    6 AS threshold_value,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) <= 6 AS passed,
    CASE 
        WHEN MAX(ingested_at) IS NULL THEN 'No data found - cannot check freshness'
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 6 
        THEN 'Data is ' || TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) || ' hours old (threshold: 6 hours)'
        ELSE NULL 
    END AS error_message
FROM CLEAN.CLN_API_B_EVENTS;

-- ============================================
-- CHECK 4: No Duplicate Primary Keys
-- ============================================

-- API A Events - Duplicate Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'duplicate_primary_key_check' AS check_name,
    'uniqueness' AS check_type,
    'CLEAN.CLN_API_A_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    0 AS threshold_value,
    COUNT(*) = 0 AS passed,
    CASE WHEN COUNT(*) > 0 THEN 'Found ' || COUNT(*) || ' duplicate id values' ELSE NULL END AS error_message
FROM (
    SELECT id, COUNT(*) AS cnt
    FROM CLEAN.CLN_API_A_EVENTS
    GROUP BY id
    HAVING cnt > 1
);

-- API B Events - Duplicate Check
INSERT INTO ANALYTICS.DQ_CHECK_RESULTS (check_name, check_type, table_name, result_value, threshold_value, passed, error_message)
SELECT
    'duplicate_primary_key_check' AS check_name,
    'uniqueness' AS check_type,
    'CLEAN.CLN_API_B_EVENTS' AS table_name,
    COUNT(*) AS result_value,
    0 AS threshold_value,
    COUNT(*) = 0 AS passed,
    CASE WHEN COUNT(*) > 0 THEN 'Found ' || COUNT(*) || ' duplicate id values' ELSE NULL END AS error_message
FROM (
    SELECT id, COUNT(*) AS cnt
    FROM CLEAN.CLN_API_B_EVENTS
    GROUP BY id
    HAVING cnt > 1
);

-- ============================================
-- View Latest DQ Results
-- ============================================

SELECT 
    check_name,
    check_type,
    table_name,
    result_value,
    threshold_value,
    passed,
    error_message,
    executed_at
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE executed_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY executed_at DESC, table_name, check_name;

-- ============================================
-- Summary of Latest Run
-- ============================================

SELECT
    COUNT(*) AS total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed_checks,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate_pct
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE executed_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP());

-- ============================================
-- Alert Query: Return failed checks (for Airflow)
-- If any rows returned, raise alert
-- ============================================

SELECT 
    check_name,
    table_name,
    error_message
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE executed_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND passed = FALSE;

