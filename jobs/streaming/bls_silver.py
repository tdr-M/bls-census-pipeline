from __future__ import annotations

import argparse
import time
from pathlib import Path

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window as W

from jobs.utils.spark_session import get_spark


BRONZE_SCHEMA = T.StructType([
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


def _cast_and_enrich(df: DataFrame) -> DataFrame:
    month_col = F.when(F.col("period").startswith("M"),
                       F.substring("period", 2, 2).cast("int"))
    return (
        df.select(
            F.to_timestamp("ts_ingested").alias("ts_ingested"),
            F.col("series_id").cast("string").alias("series_id"),
            F.col("year").cast("int").alias("year"),
            F.col("period").cast("string").alias("period"),
            F.col("periodName").cast("string").alias("periodName"),
            F.col("value").cast("double").alias("value"),
            F.col("footnotes").cast("array<string>").alias("footnotes"),
            F.col("is_latest").cast("boolean").alias("is_latest"),
        )
        .withColumn("month", month_col)
        .withColumn("dt", F.to_date(F.make_date("year", "month", F.lit(1))))
    )


def _wait_for_first_parquet(bronze_dir: str, timeout_s: int = 60) -> bool:
    """Return True if at least one non-metadata parquet file appears within timeout."""
    p = Path(bronze_dir)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for child in p.glob("*.parquet"):
            if child.name.startswith("part-"):
                return True
        time.sleep(1)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-stream-dir", default="data/bronze_stream/bls")
    ap.add_argument("--silver-dir",        default="data/silver_stream/bls")
    ap.add_argument("--checkpoint",        default="data/checkpoints/bls_silver")
    ap.add_argument("--trigger-seconds",   type=int, default=5)
    ap.add_argument("--max-seconds",       type=int, default=90)
    args = ap.parse_args()

    spark = get_spark("bls-silver-stream")

    bronze_path = args.bronze_stream_dir

    _wait_for_first_parquet(bronze_path, timeout_s=60)

    #infer the schema from the parquet file
    static_sample = spark.read.parquet(bronze_path).limit(1)
    stream_schema = static_sample.schema if len(static_sample.head(1)) else None

    if stream_schema is None:
        stream_schema = T.StructType([
            T.StructField("ts_ingested", T.StringType(), True),
            T.StructField("series_id",   T.StringType(), True),
            T.StructField("year",        T.StringType(), True),
            T.StructField("period",      T.StringType(), True),
            T.StructField("periodName",  T.StringType(), True),
            T.StructField("value",       T.StringType(), True),
            T.StructField("footnotes",   T.ArrayType(T.StringType()), True),
            T.StructField("is_latest",   T.StringType(), True),
        ])

    bronze = (
        spark.readStream
             .schema(stream_schema)
             .parquet(bronze_path)
    )

    typed = _cast_and_enrich(bronze)

    def _process_batch(df: DataFrame, epoch_id: int):
        if df.rdd.isEmpty():
            return
        win = W.partitionBy("series_id", "year", "period").orderBy(F.col("ts_ingested").desc())
        latest = (df
                  .withColumn("rn", F.row_number().over(win))
                  .where("rn = 1")
                  .drop("rn"))
        (latest
            .coalesce(1)
            .write
            .mode("append")
            .parquet(args.silver_dir))

    q = (typed.writeStream
              .foreachBatch(_process_batch)
              .option("checkpointLocation", args.checkpoint)
              .trigger(processingTime=f"{args.trigger_seconds} seconds")
              .start())

    q.awaitTermination(args.max_seconds)
    q.stop()
    spark.stop()


if __name__ == "__main__":
    main()