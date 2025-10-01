# Economic Data Pipeline — Census & BLS (Batch + Streaming)

A local, dockerized **data pipeline** that ingests U.S. economic indicators (Census MRTS for **batch**, BLS time-series for **streaming**), processes them with **PySpark**, and curates **Gold** tables in **PostgreSQL** for downstream analytics and **Power BI** dashboards.

---

## Dependency Graph

```mermaid
flowchart LR
  subgraph Producer
    P[BLS Producer (Python)]
  end

  subgraph Kafka
    K[(Kafka Broker 3.4.1)]
  end

  subgraph Spark
    C[Spark Consumer → Bronze (Parquet)]
    S[Spark Silver → Typed + Dedup]
    G[Spark Gold Writer → PostgreSQL]
  end

  subgraph Batch
    CEN[Census MRTS API → Spark Batch Job]
  end

  subgraph Storage
    PG[(PostgreSQL 17: econ DB)]
    FS[(Local Parquet Files: data/bronze, data/silver)]
  end

  subgraph BI
    BI[Power BI Desktop]
  end

  P --> K
  K --> C --> S --> G
  CEN --> G
  G --> PG
  FS --> BI
  PG --> BI
```

---

## Prerequisites

- **Python** 3.10
- **Java** 11
- **PySpark** 3.4.4
- **Apache Kafka** 3.4.1 (Bitnami, single node / KRaft mode)
- **PostgreSQL** 17 + **pgAdmin** 8
- **Apache Airflow** 2.x (Dockerized)
- **Power BI Desktop**
- **Docker & Docker Compose**

---

## Run the Infrastructure

### Start Postgres & pgAdmin
```bash
cd docker
docker compose up -d
# Postgres → localhost:5433 (db: econ, role: pbi_read)
# pgAdmin → http://localhost:5050
```

### Start Kafka
```bash
cd kafka
docker compose up -d
# Broker accessible at:
#   - Inside containers: kafka:9092
#   - From host tools: localhost:29092
```

### Initialize DB Schemas
```bash
docker exec -i pg-econ psql -U pipeline -d econ -f db/ddl_census.sql
docker exec -i pg-econ psql -U pipeline -d econ -f db/ddl_bls.sql
```

---

## Run the Pipeline

### Streaming (BLS)
Inside Airflow container:
```bash
docker exec -it airflow-webserver bash
cd /opt/airflow/project

# Produce BLS ticks into Kafka
python -m scripts.produce_bls_stream

# Consume Kafka → Bronze parquet
python -m jobs.streaming.bls_consumer --max-seconds 60

# Bronze → Silver parquet (typed, deduped)
python -m jobs.streaming.bls_silver --max-seconds 60

# Silver → Postgres Gold (upsert latest per series)
python -m jobs.streaming.bls_gold --max-seconds 60
```

Check results:
```bash
docker exec -it pg-econ psql -U pipeline -d econ -c "SELECT COUNT(*) FROM gold.bls_series_latest;"
```

### Batch (Census MRTS)
Run via Airflow DAG (`census_batch_daily`) or CLI job.  
Validate:
```bash
docker exec -it pg-econ psql -U pipeline -d econ -c "SELECT COUNT(*) FROM gold.census_mrts;"
```

---

## Power BI Connection

- **Server:** `localhost:5433`  
- **Database:** `econ`  
- **User:** `pbi_read` / `pbi_read`  
- Load: `gold.census_mrts`, `analytics.dim_date`

Build measures in DAX (example):
```DAX
Total Value := SUM('gold.census_mrts'[cell_value])

LTM 12M :=
VAR LastDate = MAX('analytics.dim_date'[dt])
RETURN CALCULATE([Total Value],
  DATESINPERIOD('analytics.dim_date'[dt], LastDate, -12, MONTH)
)
```

Example visual: **“LTM Total (12 mo) by category_code”**  
Clustered column chart → Axis = `category_code`, Values = `[LTM 12M]`, Sort descending.

---

## Notes

- Clear state if schemas change: remove `data/checkpoints/*` and old Parquet files.  
- Airflow DAGs mirror the CLI steps for reproducibility.  
- Power BI dashboards show batch & streaming metrics side by side.
