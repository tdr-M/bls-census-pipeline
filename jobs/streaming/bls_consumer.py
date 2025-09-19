from __future__ import annotations
import argparse, os
from pyspark.sql import functions as F, types as T
from jobs.utils.spark_session import get_spark
from jobs.utils.config_loader import load_yaml


_JSON_SCHEMA = T.StructType([
    T.StructField("ts_ingested", T.StringType(), True),
    T.StructField("series_id",   T.StringType(), True),
    T.StructField("year",        T.StringType(), True),
    T.StructField("period",      T.StringType(), True),
    T.StructField("periodName",  T.StringType(), True),
    T.StructField("value",       T.StringType(), True),
    T.StructField("footnotes",   T.ArrayType(T.StringType()), True),
    T.StructField("is_latest",   T.StringType(), True),  # parse as string, normalize below
])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default="config/kafka.yaml")
    ap.add_argument("--bls",   default="config/streaming_source.yaml")
    ap.add_argument("--start", default="latest")  # earliest|latest
    ap.add_argument("--bootstrap", default="")    # optional override
    ap.add_argument("--max-seconds", type=int, default=60)
    args = ap.parse_args()

    kfk = load_yaml(args.kafka)["kafka"]
    bls = load_yaml(args.bls)["bls"]
    out = bls.get("output", {})
    bronze_dir = out.get("bronze_dir_stream", "data/bronze_stream/bls")
    chk = out.get("checkpoint_bronze", "data/checkpoints/bls_bronze")

    bootstrap = (
        args.bootstrap
        or os.getenv("KAFKA_BOOTSTRAP")
        or kfk.get("bootstrap_host", "host.docker.internal:29092")
    )
    topic = kfk["topic"]
    print(f"[consumer] bootstrap={bootstrap} topic={topic}")
    print(f"[consumer] bronze_dir={bronze_dir} checkpoint={chk}")

    spark = get_spark(
        "bls-consumer",
        packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4"]
    )

    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", args.start)
            .load()
    )

    j = F.from_json(F.col("value").cast("string"), _JSON_SCHEMA)

    df = (
        raw.select(j.alias("j")).select("j.*")
           .select(
               F.col("ts_ingested").cast("string").alias("ts_ingested"),
               F.col("series_id").cast("string").alias("series_id"),
               F.col("year").cast("int").alias("year"),
               F.col("period").cast("string").alias("period"),
               F.col("periodName").cast("string").alias("periodName"),
               F.col("value").cast("double").alias("value"),
               F.col("footnotes").cast("array<string>").alias("footnotes"),
               F.when(F.col("is_latest").cast("boolean").isNotNull(),
                      F.col("is_latest").cast("boolean"))
                .when(F.lower(F.col("is_latest")).isin("true","t","1","y","yes"), F.lit(True))
                .otherwise(F.lit(False))
                .alias("is_latest"),
           )
    )

    q = (
        df.writeStream
          .format("parquet")
          .option("path", bronze_dir)
          .option("checkpointLocation", chk)
          .option("compression", "snappy")
          .outputMode("append")
          .trigger(processingTime="10 seconds")
          .start()
    )

    if args.max_seconds > 0:
        q.awaitTermination(args.max_seconds)
        q.stop()
    else:
        q.awaitTermination()

if __name__ == "__main__":
    main()