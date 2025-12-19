-- ============================================
-- Phase 3: Snowflake Storage Integration
-- Step 9: Create Storage Integration for S3
-- ============================================

-- IMPORTANT: Replace these placeholders before running:
--   <YOUR_AWS_ACCOUNT_ID>  → Your 12-digit AWS account ID
--   <unique>               → Your unique bucket suffix

-- Create storage integration to connect Snowflake with S3
CREATE OR REPLACE STORAGE INTEGRATION s3_int_motorola
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/SnowflakeS3IntegrationRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://motorola-api-snowflake-staging-<unique>/');

-- ============================================
-- Step 9b: Get values for AWS trust relationship
-- ============================================

-- Run this to get the ARN and External ID:
DESC INTEGRATION s3_int_motorola;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ IMPORTANT: Copy these two values from the DESC output:                  │
-- │                                                                         │
-- │   STORAGE_AWS_IAM_USER_ARN  → Use as Principal in trust policy          │
-- │   STORAGE_AWS_EXTERNAL_ID  → Use as ExternalId condition                │
-- │                                                                         │
-- │ Then proceed to Step 10 to update AWS IAM role trust relationship       │
-- └─────────────────────────────────────────────────────────────────────────┘

