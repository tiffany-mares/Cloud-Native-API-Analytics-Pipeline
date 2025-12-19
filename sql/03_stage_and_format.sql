-- ============================================
-- Phase 3: File Format & External Stage
-- Step 11: Create file format + external stage
-- ============================================

USE DATABASE VIDEO_ANALYTICS;
USE SCHEMA RAW;

-- Create JSON file format for JSONL files
CREATE OR REPLACE FILE FORMAT ff_jsonl
  TYPE = JSON;

-- Create external stage pointing to S3
-- IMPORTANT: Replace <unique> with your bucket suffix
CREATE OR REPLACE STAGE s3_stage_motorola
  URL = 's3://motorola-api-snowflake-staging-<unique>/'
  STORAGE_INTEGRATION = s3_int_motorola
  FILE_FORMAT = ff_jsonl;

-- Verify stage is accessible
LIST @s3_stage_motorola;

