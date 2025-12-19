-- ============================================
-- Phase 5: ANALYTICS Layer
-- Step 21: Build ANALYTICS tables/views
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE PIPELINE_WH;

-- ============================================
-- DAILY COUNTS: Event volume by day
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_DAILY_EVENT_COUNTS AS
SELECT
    'api_a' AS source,
    DATE(created_at) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS unique_records
FROM CLEAN.CLN_API_A_EVENTS
GROUP BY DATE(created_at)

UNION ALL

SELECT
    'api_b' AS source,
    DATE(created_at) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS unique_records
FROM CLEAN.CLN_API_B_EVENTS
GROUP BY DATE(created_at)

ORDER BY event_date DESC, source;

COMMENT ON VIEW ANALYTICS.VW_DAILY_EVENT_COUNTS IS 'Daily event counts by source';

-- ============================================
-- DAILY SUMMARY TABLE (materialized)
-- ============================================

CREATE OR REPLACE TABLE ANALYTICS.ANL_DAILY_SUMMARY AS
SELECT
    source,
    event_date,
    event_count,
    unique_records,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM ANALYTICS.VW_DAILY_EVENT_COUNTS;

COMMENT ON TABLE ANALYTICS.ANL_DAILY_SUMMARY IS 'Materialized daily summary for dashboards';

-- ============================================
-- FRESHNESS & INGESTION LAG METRICS
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_DATA_FRESHNESS AS
SELECT
    'api_a' AS source,
    
    -- Latest data timestamps
    MAX(updated_at) AS latest_event_time,
    MAX(ingested_at) AS latest_ingestion_time,
    MAX(_loaded_at) AS latest_load_time,
    
    -- Freshness metrics
    TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP()) AS event_age_minutes,
    TIMESTAMPDIFF(MINUTE, MAX(ingested_at), CURRENT_TIMESTAMP()) AS ingestion_age_minutes,
    
    -- Ingestion lag (time from event to ingestion)
    AVG(TIMESTAMPDIFF(MINUTE, updated_at, ingested_at)) AS avg_ingestion_lag_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, updated_at, ingested_at)) AS max_ingestion_lag_minutes,
    
    -- Record counts
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DATE(ingested_at) = CURRENT_DATE() THEN 1 END) AS records_today,
    
    -- Health status
    CASE
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 6 THEN 'WARNING'
        ELSE 'HEALTHY'
    END AS freshness_status
    
FROM CLEAN.CLN_API_A_EVENTS

UNION ALL

SELECT
    'api_b' AS source,
    MAX(modified_at) AS latest_event_time,
    MAX(ingested_at) AS latest_ingestion_time,
    MAX(_loaded_at) AS latest_load_time,
    TIMESTAMPDIFF(MINUTE, MAX(modified_at), CURRENT_TIMESTAMP()) AS event_age_minutes,
    TIMESTAMPDIFF(MINUTE, MAX(ingested_at), CURRENT_TIMESTAMP()) AS ingestion_age_minutes,
    AVG(TIMESTAMPDIFF(MINUTE, modified_at, ingested_at)) AS avg_ingestion_lag_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, modified_at, ingested_at)) AS max_ingestion_lag_minutes,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DATE(ingested_at) = CURRENT_DATE() THEN 1 END) AS records_today,
    CASE
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
        WHEN TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) > 6 THEN 'WARNING'
        ELSE 'HEALTHY'
    END AS freshness_status
FROM CLEAN.CLN_API_B_EVENTS;

COMMENT ON VIEW ANALYTICS.VW_DATA_FRESHNESS IS 'Data freshness and ingestion lag metrics by source';

-- ============================================
-- INGESTION HISTORY (by batch)
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_INGESTION_HISTORY AS
SELECT
    'api_a' AS source,
    batch_id,
    MIN(ingested_at) AS batch_start,
    MAX(ingested_at) AS batch_end,
    COUNT(*) AS record_count,
    COUNT(DISTINCT id) AS unique_ids,
    MIN(file_name) AS file_name
FROM CLEAN.CLN_API_A_EVENTS
GROUP BY batch_id

UNION ALL

SELECT
    'api_b' AS source,
    batch_id,
    MIN(ingested_at) AS batch_start,
    MAX(ingested_at) AS batch_end,
    COUNT(*) AS record_count,
    COUNT(DISTINCT id) AS unique_ids,
    MIN(file_name) AS file_name
FROM CLEAN.CLN_API_B_EVENTS
GROUP BY batch_id

ORDER BY batch_start DESC;

COMMENT ON VIEW ANALYTICS.VW_INGESTION_HISTORY IS 'Ingestion history by batch';

-- ============================================
-- TOP CATEGORIES (API A)
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_TOP_CATEGORIES AS
SELECT
    category,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS unique_records,
    MIN(created_at) AS first_seen,
    MAX(updated_at) AS last_updated,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM CLEAN.CLN_API_A_EVENTS
WHERE category IS NOT NULL
GROUP BY category
ORDER BY event_count DESC;

COMMENT ON VIEW ANALYTICS.VW_TOP_CATEGORIES IS 'Top categories by event count (API A)';

-- ============================================
-- STATUS DISTRIBUTION (API A)
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_STATUS_DISTRIBUTION AS
SELECT
    status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM CLEAN.CLN_API_A_EVENTS
GROUP BY status
ORDER BY count DESC;

COMMENT ON VIEW ANALYTICS.VW_STATUS_DISTRIBUTION IS 'Status distribution (API A)';

-- ============================================
-- PRIORITY DISTRIBUTION (API B)
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_PRIORITY_DISTRIBUTION AS
SELECT
    priority,
    state,
    COUNT(*) AS count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM CLEAN.CLN_API_B_EVENTS
GROUP BY priority, state
ORDER BY priority, state;

COMMENT ON VIEW ANALYTICS.VW_PRIORITY_DISTRIBUTION IS 'Priority and state distribution (API B)';

-- ============================================
-- HOURLY ACTIVITY PATTERN
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_HOURLY_ACTIVITY AS
SELECT
    source,
    HOUR(created_at) AS hour_of_day,
    DAYNAME(created_at) AS day_of_week,
    COUNT(*) AS event_count
FROM (
    SELECT 'api_a' AS source, created_at FROM CLEAN.CLN_API_A_EVENTS
    UNION ALL
    SELECT 'api_b' AS source, created_at FROM CLEAN.CLN_API_B_EVENTS
) combined
GROUP BY source, HOUR(created_at), DAYNAME(created_at)
ORDER BY source, hour_of_day;

COMMENT ON VIEW ANALYTICS.VW_HOURLY_ACTIVITY IS 'Hourly activity patterns by source';

-- ============================================
-- COMBINED DASHBOARD VIEW
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.VW_EXECUTIVE_DASHBOARD AS
SELECT
    f.source,
    f.total_records,
    f.records_today,
    f.freshness_status,
    f.event_age_minutes,
    f.avg_ingestion_lag_minutes,
    d.event_count AS events_last_7_days
FROM ANALYTICS.VW_DATA_FRESHNESS f
LEFT JOIN (
    SELECT 
        source, 
        SUM(event_count) AS event_count
    FROM ANALYTICS.VW_DAILY_EVENT_COUNTS
    WHERE event_date >= DATEADD(DAY, -7, CURRENT_DATE())
    GROUP BY source
) d ON f.source = d.source;

COMMENT ON VIEW ANALYTICS.VW_EXECUTIVE_DASHBOARD IS 'Executive summary dashboard';

-- ============================================
-- Verify ANALYTICS objects
-- ============================================

SHOW VIEWS IN SCHEMA ANALYTICS;
SHOW TABLES IN SCHEMA ANALYTICS;

-- Sample queries
SELECT * FROM ANALYTICS.VW_DATA_FRESHNESS;
SELECT * FROM ANALYTICS.VW_DAILY_EVENT_COUNTS LIMIT 10;
SELECT * FROM ANALYTICS.VW_TOP_CATEGORIES LIMIT 10;

