"""
Retail Analytics Data Pipeline DAG — pure ELT.
Two parallel ingestion branches converge into a single load:
  - load_csv_raw: CSV historical bulk load -> type-cast only (transform/csv_ingest.py)
  - api_ingest -> cast_api_raw: DummyJSON daily incremental -> type-cast only (transform/api_cast.py)
Neither branch does any business-rule transformation — no quality bucketing,
no cross-source harmonization. Both feed load_postgres, which lands each
source in its OWN raw table (raw_csv_sales, raw_api_carts) untouched.
dbt owns 100% of the T: staging -> intermediate (normalization, union,
valid/return/quarantine classification) -> marts (star schema). dbt_run
builds it, dbt_test validates it — a failed test fails the DAG run.
PySpark scripts run as subprocesses inside the Airflow worker's own Python env
so each job gets its own isolated JVM gateway. dbt runs from its own venv
(/opt/dbt-venv, built into the image) to keep its pinned deps away from
Airflow's — see Dockerfile.
"""

from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email


def run_script(script_path: str):
    """Run a pipeline script as a subprocess so PySpark's JVM gateway
    stays isolated per task, and surface failures with full output."""
    result = subprocess.run(
        ["python", script_path],
        cwd="/opt/airflow",
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script_path} failed with exit code {result.returncode}")


DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


def run_dbt(command: str):
    """Run a dbt subcommand from dbt's own isolated venv, against the
    project mounted at /opt/airflow/dbt. `dbt deps` first since
    dbt_packages/ isn't committed (gitignored, like node_modules)."""
    for cmd in (["deps"], command.split()):
        result = subprocess.run(
            [DBT_BIN, *cmd],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt {' '.join(cmd)} failed with exit code {result.returncode}")


def alert_on_failure(context):
    task_instance = context["task_instance"]
    subject = f"[Airflow] Retail pipeline failed: {task_instance.task_id}"
    body = (
        f"Task: {task_instance.task_id}\n"
        f"DAG: {task_instance.dag_id}\n"
        f"Execution date: {context['execution_date']}\n"
        f"Log URL: {task_instance.log_url}\n"
    )
    try:
        alert_to = os.environ.get("ALERT_EMAIL_TO")
        if not alert_to:
            print("ALERT_EMAIL_TO not set — skipping email alert")
            return
        send_email(to=[alert_to], subject=subject, html_content=body)
    except Exception as e:
        # SMTP not configured yet — don't let alerting failure mask the real pipeline failure
        print(f"Alert email failed to send: {e}")


default_args = {
    "owner": "arnav",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="retail_analytics_pipeline",
    default_args=default_args,
    description="CSV -> PySpark schema validation -> Postgres star schema",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["retail", "pyspark", "postgres"],
) as dag:

    load_csv_raw = PythonOperator(
        task_id="load_csv_raw",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/csv_ingest.py"},
    )

    api_ingest = PythonOperator(
        task_id="api_ingest",
        python_callable=run_script,
        op_kwargs={"script_path": "ingestion/api_ingest.py"},
    )

    cast_api_raw = PythonOperator(
        task_id="cast_api_raw",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/api_cast.py"},
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/load_postgres.py"},
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
        op_kwargs={"command": "run"},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt,
        op_kwargs={"command": "test"},
    )

    api_ingest >> cast_api_raw
    [load_csv_raw, cast_api_raw] >> load_postgres >> dbt_run >> dbt_test
