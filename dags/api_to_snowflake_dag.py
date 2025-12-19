"""API to Snowflake Pipeline DAG.

This DAG orchestrates the complete data pipeline:
1. Ingest data from APIs to S3
2. Refresh Snowpipes to load data to RAW
3. Transform RAW → CLEAN
4. Build ANALYTICS layer
5. Run data quality checks
6. Publish metrics

Schedule: Hourly (configurable)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ============================================
# Configuration
# ============================================

SNOWFLAKE_CONN_ID = "snowflake_default"
DAG_ID = "api_to_snowflake_pipeline"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


# ============================================
# Callback Functions
# ============================================

def on_failure_callback(context: dict[str, Any]) -> None:
    """Handle task failure - log and optionally notify.
    
    Args:
        context: Airflow context dictionary with task info
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    error_message = {
        "event": "task_failure",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": str(execution_date),
        "try_number": task_instance.try_number,
        "error": str(exception),
    }
    
    logger.error(f"Task failed: {json.dumps(error_message)}")
    
    # TODO: Add Slack/PagerDuty notification here
    # Example:
    # slack_webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    # if slack_webhook_url:
    #     send_slack_alert(slack_webhook_url, error_message)


def on_success_callback(context: dict[str, Any]) -> None:
    """Handle task success - log completion."""
    task_instance = context.get("task_instance")
    
    logger.info(
        f"Task succeeded: {task_instance.task_id}",
        extra={
            "dag_id": context.get("dag").dag_id,
            "task_id": task_instance.task_id,
            "duration": task_instance.duration,
        }
    )


# ============================================
# Task Functions
# ============================================

def ingest_api_data_to_s3(**context) -> dict:
    """Ingest data from all APIs and upload to S3.
    
    Returns:
        dict: Ingestion results with batch_id and record counts
    """
    from src.ingest_to_s3 import run_ingestion
    from src.utils import setup_logging
    
    setup_logging(level="INFO", json_format=True)
    
    # Get incremental timestamp from previous run if available
    since = None
    prev_run = context.get("prev_execution_date")
    if prev_run:
        since = prev_run.isoformat()
        logger.info(f"Running incremental load since {since}")
    
    # Run ingestion for all sources
    result = run_ingestion(
        sources=["api_a", "api_b"],
        since=since,
    )
    
    # Push results to XCom for downstream tasks
    context["ti"].xcom_push(key="ingestion_result", value=result)
    context["ti"].xcom_push(key="batch_id", value=result["batch_id"])
    
    logger.info(
        f"Ingestion complete: {result['total_records_staged']} records staged",
        extra=result
    )
    
    # Fail task if ingestion failed
    if result["status"] != "success":
        raise Exception(f"Ingestion failed: {result}")
    
    return result


def run_data_quality_checks(**context) -> dict:
    """Validate data quality check results from DQ_CHECK_RESULTS table.
    
    Reads results inserted by the SQL DQ checks and determines pass/fail.
    
    Checks validated:
    - Row count > 0 for each CLEAN table
    - No NULL primary keys
    - Freshness within 6 hours
    - No duplicate primary keys
    
    Returns:
        dict: DQ check results
    """
    import snowflake.connector
    import os
    
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "VIDEO_ANALYTICS"),
    )
    
    cursor = conn.cursor()
    
    try:
        # Get results from most recent DQ run (last 5 minutes)
        cursor.execute("""
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
            WHERE executed_at >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
            ORDER BY executed_at DESC
        """)
        
        results = cursor.fetchall()
        columns = ["check_name", "check_type", "table_name", "result_value", 
                   "threshold_value", "passed", "error_message", "executed_at"]
        
        checks = []
        for row in results:
            check = dict(zip(columns, row))
            check["executed_at"] = str(check["executed_at"])
            checks.append(check)
            
            status = "✓ PASSED" if check["passed"] else "✗ FAILED"
            logger.info(
                f"DQ Check: {check['check_name']} on {check['table_name']} - {status}",
                extra=check
            )
        
        # Get summary
        cursor.execute("""
            SELECT
                COUNT(*) AS total_checks,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
                SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed_checks
            FROM ANALYTICS.DQ_CHECK_RESULTS
            WHERE executed_at >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
        """)
        
        summary = cursor.fetchone()
        total, passed, failed = summary
        
    finally:
        cursor.close()
        conn.close()
    
    result = {
        "total_checks": total or 0,
        "passed": passed or 0,
        "failed": failed or 0,
        "checks": checks,
    }
    
    context["ti"].xcom_push(key="dq_results", value=result)
    
    if failed and failed > 0:
        # Get failed checks for error message
        failed_checks = [c for c in checks if not c["passed"]]
        
        logger.warning(
            f"DQ checks completed with {failed} failures",
            extra={"failed_checks": failed_checks}
        )
        
        # Raise exception for critical failures (null PKs, duplicates)
        critical_failures = [
            c for c in failed_checks 
            if c["check_name"] in ["null_primary_key_check", "duplicate_primary_key_check"]
        ]
        
        if critical_failures:
            error_messages = [c["error_message"] for c in critical_failures]
            raise Exception(f"Critical DQ check failures: {error_messages}")
    else:
        logger.info(f"All {passed} DQ checks passed ✓")
    
    return result


def publish_pipeline_metrics(**context) -> dict:
    """Publish pipeline metrics for monitoring.
    
    Collects and logs metrics from the pipeline run.
    
    Returns:
        dict: Pipeline metrics
    """
    # Get results from previous tasks
    ti = context["ti"]
    ingestion_result = ti.xcom_pull(key="ingestion_result", task_ids="ingest_api_data_to_s3")
    dq_results = ti.xcom_pull(key="dq_results", task_ids="run_dq_checks")
    
    metrics = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "execution_date": str(context["execution_date"]),
        "ingestion": {
            "batch_id": ingestion_result.get("batch_id") if ingestion_result else None,
            "records_fetched": ingestion_result.get("total_records_fetched", 0) if ingestion_result else 0,
            "records_staged": ingestion_result.get("total_records_staged", 0) if ingestion_result else 0,
            "duration_seconds": ingestion_result.get("duration_seconds", 0) if ingestion_result else 0,
        },
        "data_quality": {
            "total_checks": dq_results.get("total_checks", 0) if dq_results else 0,
            "passed": dq_results.get("passed", 0) if dq_results else 0,
            "failed": dq_results.get("failed", 0) if dq_results else 0,
        },
        "status": "success",
    }
    
    logger.info(f"Pipeline metrics: {json.dumps(metrics, indent=2)}")
    
    # TODO: Push to metrics system (Datadog, CloudWatch, etc.)
    # Example:
    # push_to_datadog(metrics)
    
    return metrics


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Ingest API data, load to Snowflake, transform, and validate",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "api", "snowflake", "production"],
    on_failure_callback=on_failure_callback,
) as dag:
    
    # ========================================
    # Start
    # ========================================
    start = EmptyOperator(task_id="start")
    
    # ========================================
    # Task 1: Ingest API Data to S3
    # ========================================
    ingest_task = PythonOperator(
        task_id="ingest_api_data_to_s3",
        python_callable=ingest_api_data_to_s3,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )
    
    # ========================================
    # Task 2: Refresh Snowpipes
    # ========================================
    refresh_pipe_api_a = SnowflakeOperator(
        task_id="refresh_snowpipe_api_a",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE VIDEO_ANALYTICS;
            USE SCHEMA RAW;
            ALTER PIPE pipe_api_a REFRESH;
        """,
        on_failure_callback=on_failure_callback,
    )
    
    refresh_pipe_api_b = SnowflakeOperator(
        task_id="refresh_snowpipe_api_b",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE VIDEO_ANALYTICS;
            USE SCHEMA RAW;
            ALTER PIPE pipe_api_b REFRESH;
        """,
        on_failure_callback=on_failure_callback,
    )
    
    # ========================================
    # Task 3: Wait for Snowpipe (simple delay)
    # ========================================
    wait_for_load = PythonOperator(
        task_id="wait_for_snowpipe_load",
        python_callable=lambda: __import__("time").sleep(30),
        doc="Wait 30 seconds for Snowpipe to process files",
    )
    
    # ========================================
    # Task 4: Run CLEAN Transformations
    # ========================================
    run_clean_sql = SnowflakeOperator(
        task_id="run_clean_sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="/opt/airflow/sql/08_clean_incremental.sql",
        on_failure_callback=on_failure_callback,
    )
    
    # ========================================
    # Task 5: Run ANALYTICS Transformations
    # ========================================
    run_analytics_sql = SnowflakeOperator(
        task_id="run_analytics_sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE VIDEO_ANALYTICS;
            USE WAREHOUSE PIPELINE_WH;
            
            -- Refresh materialized daily summary
            CREATE OR REPLACE TABLE ANALYTICS.ANL_DAILY_SUMMARY AS
            SELECT
                source,
                event_date,
                event_count,
                unique_records,
                CURRENT_TIMESTAMP() AS _refreshed_at
            FROM ANALYTICS.VW_DAILY_EVENT_COUNTS;
        """,
        on_failure_callback=on_failure_callback,
    )
    
    # ========================================
    # Task 6: Data Quality Checks (SQL-based)
    # ========================================
    
    # Run DQ check SQL (inserts results to DQ_CHECK_RESULTS table)
    run_dq_checks_sql = SnowflakeOperator(
        task_id="run_dq_checks_sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="/opt/airflow/sql/10_data_quality_checks.sql",
        on_failure_callback=on_failure_callback,
    )
    
    # Validate DQ results (Python check for failures)
    dq_checks = PythonOperator(
        task_id="validate_dq_results",
        python_callable=run_data_quality_checks,
        on_failure_callback=on_failure_callback,
    )
    
    # ========================================
    # Task 7: Publish Metrics
    # ========================================
    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_pipeline_metrics,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if DQ checks fail
        on_success_callback=on_success_callback,
    )
    
    # ========================================
    # End
    # ========================================
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # ========================================
    # Task Dependencies
    # ========================================
    
    # Ingestion first
    start >> ingest_task
    
    # Refresh both pipes in parallel after ingestion
    ingest_task >> [refresh_pipe_api_a, refresh_pipe_api_b]
    
    # Wait for Snowpipe, then transform
    [refresh_pipe_api_a, refresh_pipe_api_b] >> wait_for_load
    wait_for_load >> run_clean_sql >> run_analytics_sql
    
    # DQ checks after analytics (SQL insert → Python validation)
    run_analytics_sql >> run_dq_checks_sql >> dq_checks
    
    # Always publish metrics and end
    dq_checks >> publish_metrics >> end

