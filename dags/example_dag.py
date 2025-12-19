"""Example DAG to verify Airflow setup.

This DAG runs a simple task to confirm Airflow is working correctly.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_hello():
    """Simple test function."""
    print("Hello from Airflow!")
    print("DAG is working correctly.")
    return "success"


def check_imports():
    """Verify src imports work."""
    try:
        from src.utils import setup_logging
        from src.transform import transform_records
        from src.clients import ApiAClient, ApiBClient
        print("✓ All src imports successful!")
        return "imports_ok"
    except ImportError as e:
        print(f"✗ Import error: {e}")
        raise


with DAG(
    dag_id="example_verification_dag",
    default_args=default_args,
    description="Verify Airflow setup is working",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "verification"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
    
    import_check = PythonOperator(
        task_id="check_imports",
        python_callable=check_imports,
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> hello_task >> import_check >> end

