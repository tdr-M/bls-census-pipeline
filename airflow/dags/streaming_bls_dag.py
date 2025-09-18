from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Europe/Bucharest")
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="streaming_bls",
    description="BLS → Kafka → Spark streaming bronze",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bls", "kafka", "streaming", "bronze"],
) as dag:

    produce = BashOperator(
        task_id="bls_producer",
        bash_command=(
            "cd /opt/airflow/project && "
            "echo BOOT=$KAFKA_BOOTSTRAP && echo KEY=$BLS_API_KEY && "
            "python scripts/produce_bls_stream.py "
            "--config config/streaming_source.yaml "
            "--kafka  config/kafka.yaml "
            "--latest-only"
        ),
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    consume = BashOperator(
        task_id="bls_consumer",
        bash_command=(
            "cd /opt/airflow/project && "
            "echo BOOT=$KAFKA_BOOTSTRAP && "
            "python -m jobs.streaming.bls_consumer "
            "--kafka config/kafka.yaml "
            "--bls  config/streaming_source.yaml "
            "--start latest --max-seconds 60"
        ),
        env={"PYTHONPATH": "/opt/airflow/project"},
    )