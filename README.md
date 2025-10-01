# Economic Data Pipeline — Census & BLS (Batch + Streaming)

A local, dockerized **data pipeline** that ingests U.S. economic indicators (Census MRTS for batch, BLS for streaming), processes them with **PySpark**, and serves curated **Gold** tables to **PostgreSQL** for downstream analytics in **Power BI**.

---

## What it does

- **Streaming path (BLS)**  
  - Producer hits the BLS API → sends to **Kafka**  
  - **Spark consumer** lands **bronze** Parquet  
  - **Spark silver**: typing + de-duplication (latest per `series_id, year, period`)  
  - **Gold**: upserts into Postgres table `gold.bls_series_latest`

- **Batch path (Census MRTS)**  
  - Spark batch ingestion of MRTS API → **gold.census_mrts** in Postgres  
  - Analytics schema contains helper views and a date dimension for BI

- **Power BI**  
  - Connects to Postgres at `localhost:5433` (db `econ`, role `pbi_read`)  
  - Reads `gold.*` fact tables + `analytics.*` views for dashboards

**Local storage layout**
- Bronze (batch): `data/bronze/census/`  
- Bronze (stream): `data/bronze_stream/bls/`  
- Silver (batch): `data/silver/census/`  
- Silver (stream): `data/silver_stream/bls/`  
- Checkpoints: `data/checkpoints/{bls_bronze|bls_silver|bls_gold}`

---

## Tech stack

- **Python** 3.10  
- **PySpark** 3.4.4  
- **Kafka** 3.4.1 (Bitnami, single-node KRaft, host port `29092`)  
- **Apache Airflow** 2.x (Dockerized)  
- **PostgreSQL** 17 (`pg-econ`, host port `5433`)  
- **pgAdmin** 8 (UI on port `5050`)  
- **Docker & Docker Compose**  
- **Power BI Desktop**

---

## Run commands (local)

### Prereqs
- Docker running
- Repo cloned

---

### Kafka
```bash
cd kafka
docker compose up -d
```

### Postgres & pgAdmin
```bash
cd ../docker
docker compose up -d
```

Initialize schemas, tables, views:
```bash
docker exec -i pg-econ psql -U pipeline -d econ -v ON_ERROR_STOP=1 -f - < db/ddl_census.sql
docker exec -i pg-econ psql -U pipeline -d econ -v ON_ERROR_STOP=1 -f - < db/ddl_bls.sql
```

### Airflow
```bash
cd airflow
docker compose up -d
docker exec -it airflow-webserver bash
cd /opt/airflow/project
```

---

### Streaming jobs (manual run inside Airflow container)

**Produce BLS messages into Kafka:**
```bash
python -m scripts.produce_bls_stream
```

**Consume Kafka → Bronze parquet:**
```bash
python -m jobs.streaming.bls_consumer
```

**Type and dedupe → Silver parquet:**
```bash
python -m jobs.streaming.bls_silver
```

**Upsert Gold → Postgres:**
```bash
python -m jobs.streaming.bls_gold
```

Validate:
```bash
docker exec -it pg-econ psql -U pipeline -d econ -c "select count(*) from gold.bls_series_latest;"
```

---

### Batch jobs

Either run from CLI or trigger DAG **`census_batch_daily`** from Airflow UI.  
Validate:
```bash
docker exec -it pg-econ psql -U pipeline -d econ -c "select count(*) from gold.census_mrts;"
```

---

## Power BI connection

1. **Home → Get Data → PostgreSQL**
2. **Server:** `localhost:5433` **Database:** `econ`
3. **Credentials:** role `pbi_read`, password `pbi_read`
4. Select tables/views (`gold.census_mrts`, `analytics.dim_date`, etc.) and Load

---

## Notes

- Streaming micro-batches are intentionally small; run the producer multiple times for more rows  
- If schemas drift or parquet type mismatches appear, **stop jobs, clear `data/checkpoints/*`**, then re-run  
- Airflow DAGs mirror the CLI steps above, so you can trigger them instead of manual runs  
