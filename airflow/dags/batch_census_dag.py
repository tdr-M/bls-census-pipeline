from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Europe/Bucharest")
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="census_batch_daily",
    description="Fetch Census MRTS to bronze, then transform to silver and upsert gold",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule="0 8 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["census", "bronze", "silver", "gold"],
) as dag:
    bronze = BashOperator(
        task_id="census_bronze",
        bash_command="cd /opt/airflow/project && python -m jobs.batch.census_ingest --since {{ ds[:4] }}",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    silver = BashOperator(
        task_id="census_silver",
        bash_command="cd /opt/airflow/project && python -m jobs.batch.census_silver",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    gold = BashOperator(
        task_id="census_gold",
        bash_command="cd /opt/airflow/project && python -m jobs.batch.census_gold",
        env={"PYTHONPATH": "/opt/airflow/project"},
    )