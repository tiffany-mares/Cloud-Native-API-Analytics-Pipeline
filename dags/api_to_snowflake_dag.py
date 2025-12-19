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
    """Run data quality checks on loaded data.
    
    Checks:
    - Freshness: data arrived within expected window
    - Row counts: not zero, within expected range
    - Null checks: primary keys not null
    - Duplicates: no duplicate primary keys
    
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
    
    checks = []
    cursor = conn.cursor()
    
    try:
        # Check 1: Freshness - data loaded in last 2 hours
        cursor.execute("""
            SELECT 
                source,
                freshness_status,
                ingestion_age_minutes
            FROM ANALYTICS.VW_DATA_FRESHNESS
        """)
        freshness_results = cursor.fetchall()
        
        for source, status, age_minutes in freshness_results:
            check = {
                "check": "freshness",
                "source": source,
                "status": status,
                "age_minutes": age_minutes,
                "passed": status != "STALE",
            }
            checks.append(check)
            logger.info(f"Freshness check: {check}")
        
        # Check 2: Row counts - not empty
        for table in ["CLN_API_A_EVENTS", "CLN_API_B_EVENTS"]:
            cursor.execute(f"SELECT COUNT(*) FROM CLEAN.{table}")
            count = cursor.fetchone()[0]
            check = {
                "check": "row_count",
                "table": table,
                "count": count,
                "passed": count > 0,
            }
            checks.append(check)
            logger.info(f"Row count check: {check}")
        
        # Check 3: Null primary keys
        for table, pk_col in [("CLN_API_A_EVENTS", "id"), ("CLN_API_B_EVENTS", "id")]:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM CLEAN.{table} 
                WHERE {pk_col} IS NULL
            """)
            null_count = cursor.fetchone()[0]
            check = {
                "check": "null_pk",
                "table": table,
                "null_count": null_count,
                "passed": null_count == 0,
            }
            checks.append(check)
            logger.info(f"Null PK check: {check}")
        
        # Check 4: Duplicates
        for table, pk_col in [("CLN_API_A_EVENTS", "id"), ("CLN_API_B_EVENTS", "id")]:
            cursor.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_col}, COUNT(*) AS cnt
                    FROM CLEAN.{table}
                    GROUP BY {pk_col}
                    HAVING cnt > 1
                )
            """)
            dup_count = cursor.fetchone()[0]
            check = {
                "check": "duplicates",
                "table": table,
                "duplicate_count": dup_count,
                "passed": dup_count == 0,
            }
            checks.append(check)
            logger.info(f"Duplicate check: {check}")
        
    finally:
        cursor.close()
        conn.close()
    
    # Summarize results
    passed = sum(1 for c in checks if c["passed"])
    failed = sum(1 for c in checks if not c["passed"])
    
    result = {
        "total_checks": len(checks),
        "passed": passed,
        "failed": failed,
        "checks": checks,
    }
    
    context["ti"].xcom_push(key="dq_results", value=result)
    
    if failed > 0:
        logger.warning(f"DQ checks completed with {failed} failures")
        # Optionally raise exception for critical failures
        critical_failures = [c for c in checks if not c["passed"] and c["check"] in ["null_pk", "duplicates"]]
        if critical_failures:
            raise Exception(f"Critical DQ check failures: {critical_failures}")
    else:
        logger.info(f"All {passed} DQ checks passed")
    
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
    # Task 6: Data Quality Checks
    # ========================================
    dq_checks = PythonOperator(
        task_id="run_dq_checks",
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
    
    # DQ checks after analytics
    run_analytics_sql >> dq_checks
    
    # Always publish metrics and end
    dq_checks >> publish_metrics >> end

