from __future__ import annotations
import logging, os
from pathlib import Path
import psycopg2
from jobs.utils.config_loader import load_yaml
from jobs.utils.logging_setup import setup_logging
from jobs.utils.spark_session import get_spark
from pyspark.sql import functions as F, Window

LOG = logging.getLogger("jobs.census_gold")

def _pg_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["db"],
        user=cfg["user"], password=cfg["password"]
    )

def _ensure_schema_and_tables(cfg: dict, ddl_path: Path):
    with _pg_conn(cfg) as conn, conn.cursor() as cur:
        cur.execute(ddl_path.read_text(encoding="utf-8"))
        conn.commit()

def main():
    setup_logging()
    pg_cfg_path = os.getenv("POSTGRES_CONFIG", "config/postgres.yaml")
    LOG.info("Using Postgres config: %s", pg_cfg_path)
    pgc = load_yaml(pg_cfg_path)["postgres"]

    src = load_yaml("config/sources.yaml")["census"]
    silver_dir = Path(src["output"]["silver_dir"]).resolve()

    ddl_path = Path("db/ddl_census.sql")
    _ensure_schema_and_tables(pgc, ddl_path)

    pg_pkg = pgc.get("jdbc_package", "org.postgresql:postgresql:42.7.3")
    spark = get_spark("census-gold", packages=[pg_pkg])

    df = (spark.read.parquet(str(silver_dir))
            .withColumnRenamed("date", "dt")
            .select("time","dt","year","month","is_seasonally_adjusted","category_code",
                    "data_type_code","time_slot_id","cell_value","error_data","ingest_date"))

    key_cols = ["dt", "category_code", "data_type_code", "time_slot_id", "is_seasonally_adjusted"]
    df = df.withColumn("ingest_date", F.to_date("ingest_date"))
    w = Window.partitionBy(*key_cols).orderBy(F.col("ingest_date").desc(), F.col("time").desc())
    df_upsert = (df.withColumn("rn", F.row_number().over(w))
                   .filter(F.col("rn") == 1)
                   .drop("rn"))

    jdbc_url = f"jdbc:postgresql://{pgc['host']}:{pgc['port']}/{pgc['db']}"
    staging = f"{pgc['staging_schema']}.census_mrts"
    props = {"user": pgc["user"], "password": pgc["password"], "driver": pgc["driver"]}

    df_upsert.write.mode("overwrite").jdbc(jdbc_url, staging, properties=props)

    upsert_sql = f"""
    INSERT INTO {pgc['schema']}.census_mrts
    (time, dt, year, month, is_seasonally_adjusted, category_code, data_type_code,
     time_slot_id, cell_value, error_data, ingest_date)
    SELECT time, dt, year, month, is_seasonally_adjusted, category_code, data_type_code,
           time_slot_id, cell_value, error_data, ingest_date
    FROM {staging}
    ON CONFLICT (dt, category_code, data_type_code, time_slot_id, is_seasonally_adjusted)
    DO UPDATE SET
      time=EXCLUDED.time, year=EXCLUDED.year, month=EXCLUDED.month,
      cell_value=EXCLUDED.cell_value, error_data=EXCLUDED.error_data,
      ingest_date=EXCLUDED.ingest_date;
    """
    with _pg_conn(pgc) as conn, conn.cursor() as cur:
        cur.execute(upsert_sql)
        conn.commit()

    LOG.info("Gold upsert complete into %s.census_mrts", pgc["schema"])
    spark.stop()

if __name__ == "__main__":
    main()