-- ============================================
-- Phase 3: Snowflake Database Setup
-- Step 8: Create DB + Schemas + Warehouse
-- ============================================

-- Create the main analytics database
CREATE OR REPLACE DATABASE VIDEO_ANALYTICS;

-- Create layered schemas following medallion architecture
-- RAW: Landing zone for raw JSON payloads
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.RAW;

-- CLEAN: Typed, deduplicated, standardized data
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.CLEAN;

-- ANALYTICS: Aggregations, KPIs, reporting views
CREATE OR REPLACE SCHEMA VIDEO_ANALYTICS.ANALYTICS;

-- Create dedicated warehouse for pipeline workloads
CREATE OR REPLACE WAREHOUSE PIPELINE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Verify setup
SHOW SCHEMAS IN DATABASE VIDEO_ANALYTICS;
SHOW WAREHOUSES LIKE 'PIPELINE_WH';

