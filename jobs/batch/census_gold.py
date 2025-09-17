from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_batch

from jobs.utils.config_loader import load_yaml
from jobs.utils.logging_setup import setup_logging
from jobs.utils.spark_session import get_spark

LOG = logging.getLogger("jobs.census_gold")

def _pg_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["db"],
        user=cfg["user"], password=cfg["password"]
    )

def _ensure_schema_and_tables(cfg: dict, ddl_path: Path):
    with _pg_conn(cfg) as conn, conn.cursor() as cur:
        sql = ddl_path.read_text(encoding="utf-8")
        cur.execute(sql)
        conn.commit()

def main():
    setup_logging()
    pgc = load_yaml("config/postgres.yaml")["postgres"]
    src = load_yaml("config/sources.yaml")["census"]

    silver_dir = Path(src["output"]["silver_dir"]).resolve()
    ddl_path = Path("db/ddl_census.sql")
    _ensure_schema_and_tables(pgc, ddl_path)

    pg_pkg = pgc.get("jdbc_package", "org.postgresql:postgresql:42.7.3")
    spark = get_spark("census-gold", packages=[pg_pkg])

    df = spark.read.parquet(str(silver_dir))
    df = (df
          .withColumnRenamed("date", "dt")
          .select("time","dt","year","month","is_seasonally_adjusted","category_code",
                  "data_type_code","time_slot_id","cell_value","error_data","ingest_date"))

    jdbc_url = f"jdbc:postgresql://{pgc['host']}:{pgc['port']}/{pgc['db']}"
    staging_table = f"{pgc['staging_schema']}.census_mrts"
    props = {
        "user": pgc["user"],
        "password": pgc["password"],
        "driver": pgc["driver"],
    }

    with _pg_conn(pgc) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {staging_table};")
        conn.commit()

    (df.write
       .mode("append")
       .jdbc(jdbc_url, staging_table, properties=props))

    upsert_sql = f"""
    INSERT INTO {pgc['schema']}.census_mrts
    (time, dt, year, month, is_seasonally_adjusted, category_code, data_type_code,
     time_slot_id, cell_value, error_data, ingest_date)
    SELECT time, dt, year, month, is_seasonally_adjusted, category_code, data_type_code,
           time_slot_id, cell_value, error_data, ingest_date
    FROM {staging_table}
    ON CONFLICT (dt, category_code, data_type_code, time_slot_id, is_seasonally_adjusted)
    DO UPDATE SET
        time        = EXCLUDED.time,
        year        = EXCLUDED.year,
        month       = EXCLUDED.month,
        cell_value  = EXCLUDED.cell_value,
        error_data  = EXCLUDED.error_data,
        ingest_date = EXCLUDED.ingest_date;
    """
    with _pg_conn(pgc) as conn, conn.cursor() as cur:
        cur.execute(upsert_sql)
        conn.commit()

    LOG.info("Gold upsert complete into %s.census_mrts", pgc["schema"])
    spark.stop()

if __name__ == "__main__":
    main()