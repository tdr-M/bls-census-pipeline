from __future__ import annotations
import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.batch.census_ingest import main as ingest_main
from jobs.batch.census_silver import main as silver_main
from jobs.batch.census_gold import main as gold_main

LOCAL_TZ = pendulum.timezone("Europe/Bucharest")

default_args = {
    "owner": "data-eng",
    "retries": 2,
}

with DAG(
    dag_id="census_batch_daily",
    description="Fetch Census MRTS to bronze, then transform to silver",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule="0 8 * * *",  # every day 08:00 EET/EEST
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["census","bronze","silver"],
) as dag:

    def run_bronze(ds, **_):
        since_year = ds[:4]
        ingest_main(since_year)

    bronze = PythonOperator(
        task_id="census_bronze",
        python_callable=run_bronze,
    )

    silver = PythonOperator(
        task_id="census_silver",
        python_callable=silver_main,
    )

    gold = PythonOperator(
        task_id="census_gold",
        python_callable=gold_main,
    )