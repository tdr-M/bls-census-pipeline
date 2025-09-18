from __future__ import annotations
import argparse
import os
import psycopg2
from pyspark.sql import functions as F, types as T, Window as W
from pyspark.sql import DataFrame
from jobs.utils.spark_session import get_spark
from jobs.utils.config_loader import load_yaml

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
    series_id TEXT PRIMARY KEY,
    ts_ingested TIMESTAMPTZ,
    dt DATE,
    year INT,
    month INT,
    period TEXT,
    periodName TEXT,
    value DOUBLE PRECISION,
    is_latest BOOLEAN,
    footnotes TEXT[]
);
"""

UPSERT_SQL = """
INSERT INTO {schema}.bls_series_latest
(series_id, ts_ingested, dt, year, month, period, periodName, value, is_latest, footnotes)
VALUES (%(series_id)s, %(ts_ingested)s, %(dt)s, %(year)s, %(month)s, %(period)s,
        %(periodName)s, %(value)s, %(is_latest)s, %(footnotes)s)
ON CONFLICT (series_id) DO UPDATE SET
  ts_ingested = EXCLUDED.ts_ingested,
  dt          = EXCLUDED.dt,
  year        = EXCLUDED.year,
  month       = EXCLUDED.month,
  period      = EXCLUDED.period,
  periodName  = EXCLUDED.periodName,
  value       = EXCLUDED.value,
  is_latest   = EXCLUDED.is_latest,
  footnotes   = EXCLUDED.footnotes;
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir",  default="data/silver/bls_stream")
    ap.add_argument("--checkpoint",  default="data/checkpoints/bls_gold")
    ap.add_argument("--pg",          default="config/postgres.docker.yaml")
    ap.add_argument("--max-seconds", type=int, default=60)
    args = ap.parse_args()

    pgc = load_yaml(args.pg)["postgres"]
    spark = get_spark("bls-gold-stream", packages=[pgc.get("jdbc_package","org.postgresql:postgresql:42.7.3")])

    #target table
    with _pg_conn(pgc) as conn, conn.cursor() as cur:
        cur.execute(DDL.format(schema=pgc["schema"]))
        conn.commit()

    #read silver stream with explicit schema
    silver = (
        spark.readStream
             .schema(SILVER_SCHEMA)
             .option("maxFilesPerTrigger", 1)
             .parquet(args.silver_dir)
    )

    def _process_batch(df: DataFrame, epoch_id: int):
        if df.rdd.isEmpty():
            return
        w = W.partitionBy("series_id").orderBy(F.col("ts_ingested").desc())
        latest = (df.withColumn("rn", F.row_number().over(w))
                    .where("rn = 1")
                    .drop("rn")
                    .select("series_id","ts_ingested","dt","year","month",
                            "period","periodName","value","is_latest","footnotes"))

        #collect small micro-batches and upsert
        rows = [r.asDict(recursive=True) for r in latest.collect()]
        if not rows:
            return
        with _pg_conn(pgc) as conn, conn.cursor() as cur:
            cur.executemany(UPSERT_SQL.format(schema=pgc["schema"]), rows)
            conn.commit()

    q = (silver.writeStream
                 .foreachBatch(_process_batch)
                 .option("checkpointLocation", args.checkpoint)
                 .trigger(processingTime="5 seconds")
                 .start())

    q.awaitTermination(args.max_seconds)
    q.stop()
    spark.stop()

if __name__ == "__main__":
    main()