from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Europe/Bucharest")
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="streaming_bls_silver_gold",
    description="Transform BLS bronze stream → silver (typed) and push latest per series to Postgres (gold)",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bls","streaming","silver","gold"],
) as dag:
    silver = BashOperator(
        task_id="bls_silver_stream",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -m jobs.streaming.bls_silver --max-seconds 60"
        ),
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    gold = BashOperator(
        task_id="bls_gold_latest",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -m jobs.streaming.bls_gold --max-seconds 30"
        ),
        env={
            "PYTHONPATH": "/opt/airflow/project",
            "POSTGRES_CONFIG": "/opt/airflow/project/config/postgres.docker.yaml",
        },
    )
