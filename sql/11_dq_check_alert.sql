-- ============================================
-- DQ Check Alert Query
-- Returns failed checks from the last run
-- Used by Airflow to determine if DQ passed
-- ============================================

USE DATABASE VIDEO_ANALYTICS;

-- This query returns rows only if there are failures
-- Airflow can check: if rows returned, fail the task

SELECT 
    check_name,
    check_type,
    table_name,
    result_value,
    threshold_value,
    error_message,
    executed_at
FROM ANALYTICS.DQ_CHECK_RESULTS
WHERE executed_at >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
  AND passed = FALSE
ORDER BY check_type, table_name;

