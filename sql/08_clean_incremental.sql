-- ============================================
-- Phase 5: Incremental CLEAN Updates
-- Alternative to full rebuild for production
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA CLEAN;
USE WAREHOUSE PIPELINE_WH;

-- ============================================
-- Incremental pattern using MERGE
-- This preserves existing data and upserts new records
-- ============================================

-- First, ensure tables exist (run 07_clean_tables.sql first)

-- ============================================
-- MERGE: API A Events (Incremental)
-- ============================================

MERGE INTO CLEAN.CLN_API_A_EVENTS AS target
USING (
    -- Source: new/updated records from RAW
    SELECT
        payload:id::STRING AS id,
        payload:created_at::TIMESTAMP_NTZ AS created_at,
        payload:updated_at::TIMESTAMP_NTZ AS updated_at,
        payload:name::STRING AS name,
        payload:status::STRING AS status,
        payload:type::STRING AS event_type,
        payload:value::NUMBER AS value,
        payload:description::STRING AS description,
        payload:metadata:category::STRING AS category,
        payload:metadata:tags::ARRAY AS tags,
        ingested_at,
        batch_id,
        file_name,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM RAW.API_A_EVENTS
    WHERE ingested_at > (
        SELECT COALESCE(MAX(_loaded_at), '1900-01-01') FROM CLEAN.CLN_API_A_EVENTS
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:id::STRING 
        ORDER BY payload:updated_at::TIMESTAMP_NTZ DESC NULLS LAST
    ) = 1
) AS source
ON target.id = source.id

-- Update if source is newer
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.name = source.name,
    target.status = source.status,
    target.event_type = source.event_type,
    target.value = source.value,
    target.description = source.description,
    target.category = source.category,
    target.tags = source.tags,
    target.ingested_at = source.ingested_at,
    target.batch_id = source.batch_id,
    target.file_name = source.file_name,
    target._loaded_at = source._loaded_at

-- Insert new records
WHEN NOT MATCHED THEN INSERT (
    id, created_at, updated_at, name, status, event_type, value, 
    description, category, tags, ingested_at, batch_id, file_name, _loaded_at
) VALUES (
    source.id, source.created_at, source.updated_at, source.name, source.status,
    source.event_type, source.value, source.description, source.category, 
    source.tags, source.ingested_at, source.batch_id, source.file_name, source._loaded_at
);

-- ============================================
-- MERGE: API B Events (Incremental)
-- ============================================

MERGE INTO CLEAN.CLN_API_B_EVENTS AS target
USING (
    SELECT
        payload:id::STRING AS id,
        payload:created_at::TIMESTAMP_NTZ AS created_at,
        payload:modified_at::TIMESTAMP_NTZ AS modified_at,
        payload:title::STRING AS title,
        payload:state::STRING AS state,
        payload:priority::STRING AS priority,
        payload:amount::NUMBER(18, 2) AS amount,
        payload:notes::STRING AS notes,
        payload:user_id::STRING AS user_id,
        payload:user_name::STRING AS user_name,
        ingested_at,
        batch_id,
        file_name,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM RAW.API_B_EVENTS
    WHERE ingested_at > (
        SELECT COALESCE(MAX(_loaded_at), '1900-01-01') FROM CLEAN.CLN_API_B_EVENTS
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:id::STRING 
        ORDER BY payload:modified_at::TIMESTAMP_NTZ DESC NULLS LAST
    ) = 1
) AS source
ON target.id = source.id

WHEN MATCHED AND source.modified_at > target.modified_at THEN UPDATE SET
    target.created_at = source.created_at,
    target.modified_at = source.modified_at,
    target.title = source.title,
    target.state = source.state,
    target.priority = source.priority,
    target.amount = source.amount,
    target.notes = source.notes,
    target.user_id = source.user_id,
    target.user_name = source.user_name,
    target.ingested_at = source.ingested_at,
    target.batch_id = source.batch_id,
    target.file_name = source.file_name,
    target._loaded_at = source._loaded_at

WHEN NOT MATCHED THEN INSERT (
    id, created_at, modified_at, title, state, priority, amount,
    notes, user_id, user_name, ingested_at, batch_id, file_name, _loaded_at
) VALUES (
    source.id, source.created_at, source.modified_at, source.title, source.state,
    source.priority, source.amount, source.notes, source.user_id, source.user_name,
    source.ingested_at, source.batch_id, source.file_name, source._loaded_at
);

-- ============================================
-- Verify incremental update
-- ============================================

SELECT 
    'CLN_API_A_EVENTS' AS table_name,
    COUNT(*) AS total_rows,
    MAX(_loaded_at) AS last_load
FROM CLEAN.CLN_API_A_EVENTS
UNION ALL
SELECT 
    'CLN_API_B_EVENTS' AS table_name,
    COUNT(*) AS total_rows,
    MAX(_loaded_at) AS last_load
FROM CLEAN.CLN_API_B_EVENTS;

