from __future__ import annotations

import argparse
from pyspark.sql import functions as F, types as T, Window as W
from pyspark.sql import DataFrame
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
])


def _cast_and_enrich(df: DataFrame) -> DataFrame:
    month_col = F.when(F.col("period").startswith("M"),
                       F.substring("period", 2, 2).cast("int"))
    return (
        df.select(
            F.col("ts_ingested").cast("timestamp").alias("ts_ingested"),
            F.col("series_id").cast("string").alias("series_id"),
            F.col("year").cast("int").alias("year"),
            F.col("period").cast("string").alias("period"),
            F.col("periodName").cast("string").alias("periodName"),
            F.col("value").cast("double").alias("value"),
            F.col("footnotes"),
            F.col("is_latest").cast("boolean").alias("is_latest"),
        )
        .withColumn("month", month_col)
        .withColumn("dt", F.to_date(F.make_date("year", "month", F.lit(1))))
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-stream-dir", default="data/bronze_stream/bls")
    ap.add_argument("--silver-dir",       default="data/silver/bls_stream")
    ap.add_argument("--checkpoint",       default="data/checkpoints/bls_silver")
    ap.add_argument("--max-seconds",      type=int, default=90)
    args = ap.parse_args()

    spark = get_spark("bls-silver-stream")

    #read the bronze stream
    bronze = (
        spark.readStream
             .schema(BRONZE_SCHEMA)
             .option("maxFilesPerTrigger", 1)
             .parquet(args.bronze_stream_dir)
    )

    typed = _cast_and_enrich(bronze)

    #per-microbatch -- write parquet append
    def _process_batch(df: DataFrame, epoch_id: int):
        if df.rdd.isEmpty():
            return
        win = W.partitionBy("series_id", "year", "period") \
               .orderBy(F.col("ts_ingested").desc())
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
              .trigger(processingTime="5 seconds")
              .start())

    q.awaitTermination(args.max_seconds)
    q.stop()
    spark.stop()


if __name__ == "__main__":
    main()