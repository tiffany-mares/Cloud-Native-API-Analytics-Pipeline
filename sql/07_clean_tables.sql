-- ============================================
-- Phase 5: CLEAN Layer Tables
-- Step 20: Build CLEAN tables (typed, deduped)
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA CLEAN;
USE WAREHOUSE PIPELINE_WH;

-- ============================================
-- CLEAN: API A Events
-- - Parse JSON fields from payload
-- - Cast to proper types
-- - Dedupe by id, keep latest updated_at
-- ============================================

CREATE OR REPLACE TABLE CLEAN.CLN_API_A_EVENTS AS
SELECT
    -- Primary key
    payload:id::STRING AS id,
    
    -- Timestamps
    payload:created_at::TIMESTAMP_NTZ AS created_at,
    payload:updated_at::TIMESTAMP_NTZ AS updated_at,
    
    -- Core fields (adjust based on actual API response)
    payload:name::STRING AS name,
    payload:status::STRING AS status,
    payload:type::STRING AS event_type,
    payload:value::NUMBER AS value,
    payload:description::STRING AS description,
    
    -- Nested object example
    payload:metadata:category::STRING AS category,
    payload:metadata:tags::ARRAY AS tags,
    
    -- Pipeline metadata
    ingested_at,
    batch_id,
    file_name,
    
    -- Audit fields
    CURRENT_TIMESTAMP() AS _loaded_at
    
FROM RAW.API_A_EVENTS

-- Dedupe: keep latest version of each record
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payload:id::STRING 
    ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC NULLS LAST
) = 1;

-- Add comments
COMMENT ON TABLE CLEAN.CLN_API_A_EVENTS IS 'Cleaned and deduped events from API A';

-- ============================================
-- CLEAN: API B Events
-- - Parse JSON fields from payload
-- - Cast to proper types
-- - Dedupe by id, keep latest modified_at
-- ============================================

CREATE OR REPLACE TABLE CLEAN.CLN_API_B_EVENTS AS
SELECT
    -- Primary key
    payload:id::STRING AS id,
    
    -- Timestamps
    payload:created_at::TIMESTAMP_NTZ AS created_at,
    payload:modified_at::TIMESTAMP_NTZ AS modified_at,
    
    -- Core fields (adjust based on actual API response)
    payload:title::STRING AS title,
    payload:state::STRING AS state,
    payload:priority::STRING AS priority,
    payload:amount::NUMBER(18, 2) AS amount,
    payload:notes::STRING AS notes,
    
    -- User reference
    payload:user_id::STRING AS user_id,
    payload:user_name::STRING AS user_name,
    
    -- Pipeline metadata
    ingested_at,
    batch_id,
    file_name,
    
    -- Audit fields
    CURRENT_TIMESTAMP() AS _loaded_at
    
FROM RAW.API_B_EVENTS

-- Dedupe: keep latest version of each record
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payload:id::STRING 
    ORDER BY payload:modified_at::TIMESTAMP_NTZ DESC NULLS LAST
) = 1;

-- Add comments
COMMENT ON TABLE CLEAN.CLN_API_B_EVENTS IS 'Cleaned and deduped events from API B';

-- ============================================
-- Verify CLEAN tables
-- ============================================

SELECT 'CLN_API_A_EVENTS' AS table_name, COUNT(*) AS row_count FROM CLEAN.CLN_API_A_EVENTS
UNION ALL
SELECT 'CLN_API_B_EVENTS' AS table_name, COUNT(*) AS row_count FROM CLEAN.CLN_API_B_EVENTS;

-- Check for duplicates (should return 0)
SELECT 'API_A duplicates' AS check_name, COUNT(*) AS duplicate_count
FROM (
    SELECT id, COUNT(*) AS cnt
    FROM CLEAN.CLN_API_A_EVENTS
    GROUP BY id
    HAVING cnt > 1
)
UNION ALL
SELECT 'API_B duplicates' AS check_name, COUNT(*) AS duplicate_count
FROM (
    SELECT id, COUNT(*) AS cnt
    FROM CLEAN.CLN_API_B_EVENTS
    GROUP BY id
    HAVING cnt > 1
);

