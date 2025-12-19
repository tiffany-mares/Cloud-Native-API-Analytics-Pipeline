-- ============================================
-- Phase 3: RAW Layer Tables
-- Step 12: Create RAW tables (VARIANT-first)
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA RAW;

-- ============================================
-- API A Events - Raw landing table
-- ============================================
CREATE OR REPLACE TABLE RAW.API_A_EVENTS (
  payload VARIANT,
  source STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING,
  file_name STRING
);

-- ============================================
-- API B Events - Raw landing table
-- ============================================
CREATE OR REPLACE TABLE RAW.API_B_EVENTS (
  payload VARIANT,
  source STRING,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING,
  file_name STRING
);

-- ============================================
-- Table documentation
-- ============================================
COMMENT ON TABLE RAW.API_A_EVENTS IS 'Raw JSON payloads from API A source';
COMMENT ON TABLE RAW.API_B_EVENTS IS 'Raw JSON payloads from API B source';

COMMENT ON COLUMN RAW.API_A_EVENTS.payload IS 'Raw JSON payload as VARIANT';
COMMENT ON COLUMN RAW.API_A_EVENTS.source IS 'Source identifier (api_a)';
COMMENT ON COLUMN RAW.API_A_EVENTS.ingested_at IS 'Timestamp when record was loaded';
COMMENT ON COLUMN RAW.API_A_EVENTS.batch_id IS 'Batch identifier from staging path';
COMMENT ON COLUMN RAW.API_A_EVENTS.file_name IS 'Source file name from S3';

-- Verify tables created
SHOW TABLES IN SCHEMA RAW;

