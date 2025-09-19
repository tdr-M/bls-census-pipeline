**Economic Data Pipeline — Census & BLS (Batch + Streaming)**

A local, dockerized data pipeline that processes public economic data and serves it to PostgreSQL for analytics and Power BI reporting.
It supports both batch (Census MRTS) and streaming (BLS time-series through Kafka → Spark Structured Streaming).


**What it does**

Streaming path:
BLS API → Python producer → Kafka → Spark consumer writes bronze Parquet → Spark silver typing/dedup → gold upsert into Postgres table gold.bls_series_latest.

Batch path:
Census MRTS API → Spark batch → gold table gold.census_mrts in Postgres

Power BI connects to Postgres (localhost:5433, db econ, role: pbi_read, password: pbi_read) and reads gold.* tables and analytics.* views

*Local storage :*

Bronze: data/bronze/census/ (BATCH) & data/bronze_stream/bls/ (streaming)

Silver: data/silver/census/ (BATCH) & data/silver_stream/bls/ (streaming)

Checkpoints: data/checkpoints/{bls_bronze|bls_silver|bls_gold} *(for streaming)*


**Tech stack**

Python 3.10

PySpark 3.4.4

Kafka 3.4.1 (Bitnami image, single-node KRaft) – host port 29092

Apache Airflow 2.x (dockerized) – used to orchestrate, but jobs also runnable via CLI

PostgreSQL 17 (container name pg-econ) – host port 5433

pgAdmin 8 (optional UI) – host port 5050

Docker / Docker Compose

Power BI Desktop (latest)

**RUN COMMANDS (local)**

*Prereqs: Docker running. Repo cloned.*

**Open bash :**

**kafka :**

cd kafka

docker compose up -d

**postgres & pgAdmin :**

cd ../docker

docker compose up -d

*From repo root to (re)create schemas, tables, views :*

docker exec -i pg-econ psql -U pipeline -d econ -v ON_ERROR_STOP=1 -f - < db/ddl_census.sql

docker exec -i pg-econ psql -U pipeline -d econ -v ON_ERROR_STOP=1 -f - < db/ddl_bls.sql

**airflow :**

*from repo root :*

cd airflow

docker compose up -d

docker exec -it airflow-webserver bash

cd /opt/airflow/project

**BLS messages into Kafka :**

python -m scripts.produce_bls_stream

**Kafka messages to bronze parquet :**

python -m jobs.streaming.bls_consumer

**Type and dedupe to silver :**

python -m jobs.streaming.bls_silver

**Upsert gold (Postgres) :**

python -m jobs.streaming.bls_gold

*from repo root for quick validation :*

docker exec -it pg-econ psql -U pipeline -d econ -c "select count(*) from gold.bls_series_latest;"

**You can also do all of the above by running the streaming DAGs (streaming_bls and streaming_bls_silver_gold) 
from the Airflow UI and for the BATCH path you can also run the census_batch_daily DAG (also from the Airflow UI) or
run the batch jobs from the CLI.**

*from repo root for quick validation (batch/census data):*

docker exec -it pg-econ psql -U pipeline -d econ -c "select count(*) from gold.census_mrts;"

**Connect Power BI**

*Ensure Docker is up & running*

1. Home → Get Data → PostgreSQL

2. Server: localhost:5433 Database: econ

3. Credentials: role: pbi_read, password: pbi_read

4. Select tables/views (e.g., gold.census_mrts, analytics.dim_date) and Load.

***Notes***

Streaming micro-batches are small by design; re-run the producer multiple times for more rows. I also did not use
streaming data for analysis.

If you change schemas or get inconsistent parquet types, stop jobs, clear data/checkpoints/* for the affected stream(s), then re-run.

*Again: Airflow DAGs mirror the CLI steps above; you can trigger them instead of running jobs manually.*
