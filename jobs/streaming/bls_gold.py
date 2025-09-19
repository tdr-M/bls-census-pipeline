from __future__ import annotations
import argparse, os, logging
from typing import List
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.sql.window import Window as W

from jobs.utils.spark_session import get_spark
from jobs.utils.config_loader import load_yaml
from jobs.utils.logging_setup import setup_logging

LOG = logging.getLogger("jobs.bls_gold")

SILVER_SCHEMA = T.StructType([
    T.StructField("ts_ingested", T.TimestampType(), True),
    T.StructField("series_id",   T.StringType(),    True),
    T.StructField("year",        T.IntegerType(),   True),
    T.StructField("period",      T.StringType(),    True),
    T.StructField("periodName",  T.StringType(),    True),
    T.StructField("value",       T.DoubleType(),    True),
    T.StructField("footnotes",   T.ArrayType(T.StringType()), True),
    T.StructField("is_latest",   T.BooleanType(),   True),
    T.StructField("month",       T.IntegerType(),   True),
    T.StructField("dt",          T.DateType(),      True),
])

def _pg_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["db"],
        user=cfg["user"], password=cfg["password"]
    )

DDL = """
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.bls_series_latest (
  series_id   TEXT PRIMARY KEY,
  ts_ingested TIMESTAMPTZ NOT NULL,
  dt          DATE        NOT NULL,
  year        INT         NOT NULL,
  month       INT         NOT NULL,
  period      TEXT        NOT NULL,
  periodname  TEXT        NULL,
  value       DOUBLE PRECISION NOT NULL,
  is_latest   BOOLEAN     NOT NULL,
  footnotes   TEXT[]      NULL
);

CREATE TABLE IF NOT EXISTS {schema}.bls_series_history (
  series_id   TEXT        NOT NULL,
  ts_ingested TIMESTAMPTZ NOT NULL,
  dt          DATE        NOT NULL,
  year        INT         NOT NULL,
  month       INT         NOT NULL,
  period      TEXT        NOT NULL,
  periodname  TEXT        NULL,
  value       DOUBLE PRECISION NOT NULL,
  is_latest   BOOLEAN     NOT NULL,
  footnotes   TEXT[]      NULL,
  PRIMARY KEY (series_id, ts_ingested)
);
"""

INS_HISTORY = """
INSERT INTO {schema}.bls_series_history
(series_id, ts_ingested, dt, year, month, period, periodname, value, is_latest, footnotes)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
"""

UPSERT_LATEST = """
INSERT INTO {schema}.bls_series_latest
(series_id, ts_ingested, dt, year, month, period, periodname, value, is_latest, footnotes)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (series_id) DO UPDATE SET
  ts_ingested = EXCLUDED.ts_ingested,
  dt          = EXCLUDED.dt,
  year        = EXCLUDED.year,
  month       = EXCLUDED.month,
  period      = EXCLUDED.period,
  periodname  = EXCLUDED.periodname,
  value       = EXCLUDED.value,
  is_latest   = EXCLUDED.is_latest,
  footnotes   = EXCLUDED.footnotes;
"""

def _rows_for_pg(df: DataFrame) -> List[tuple]:
    selected = df.select(
        "series_id","ts_ingested","dt","year","month",
        "period","periodName","value","is_latest","footnotes"
    )
    out = []
    for r in selected.collect():
        out.append((
            r["series_id"], r["ts_ingested"], r["dt"], r["year"], r["month"],
            r["period"], r["periodName"], r["value"], r["is_latest"], r["footnotes"]
        ))
    return out

def main():
    setup_logging()

    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir",  default="data/silver_stream/bls")
    ap.add_argument("--checkpoint",  default="data/checkpoints/bls_gold")
    ap.add_argument("--pg",          default=os.getenv("POSTGRES_CONFIG", "config/postgres.docker.yaml"))
    ap.add_argument("--trigger-seconds", type=int, default=5)
    ap.add_argument("--max-seconds", type=int, default=90)
    args = ap.parse_args()

    LOG.info("Using Postgres config: %s", args.pg)
    pgc = load_yaml(args.pg)["postgres"]

    with _pg_conn(pgc) as conn, conn.cursor() as cur:
        cur.execute(DDL.format(schema=pgc["schema"]))
        conn.commit()

    spark = get_spark("bls-gold-stream", packages=[pgc.get("jdbc_package","org.postgresql:postgresql:42.7.3")])

    #stream silver parquet output
    silver = (spark.readStream
                    .schema(SILVER_SCHEMA)
                    .option("maxFilesPerTrigger", 1)
                    .parquet(args.silver_dir))

    def _process_batch(df: DataFrame, epoch_id: int):
        if df.rdd.isEmpty():
            LOG.info("[gold] epoch=%s empty batch; skip", epoch_id); return

        win = W.partitionBy("series_id").orderBy(F.col("ts_ingested").desc())
        latest = (df
                  .withColumn("rn", F.row_number().over(win))
                  .where("rn = 1")
                  .drop("rn")
                  .dropDuplicates(["series_id", "ts_ingested"]))

        rows = _rows_for_pg(latest)
        if not rows:
            LOG.info("[gold] epoch=%s no rows after dedup; skip", epoch_id); return

        with _pg_conn(pgc) as conn, conn.cursor() as cur:
            execute_batch(cur, INS_HISTORY.format(schema=pgc["schema"]), rows, page_size=500)
            execute_batch(cur, UPSERT_LATEST.format(schema=pgc["schema"]), rows, page_size=500)
            conn.commit()
        LOG.info("[gold] epoch=%s wrote history=%d and upserted latest=%d", epoch_id, len(rows), len(rows))

    q = (silver.writeStream
                 .foreachBatch(_process_batch)
                 .option("checkpointLocation", args.checkpoint)
                 .trigger(processingTime=f"{args.trigger_seconds} seconds")
                 .start())

    q.awaitTermination(args.max_seconds)
    q.stop()
    spark.stop()

if __name__ == "__main__":
    main()