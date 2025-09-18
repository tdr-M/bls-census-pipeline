from __future__ import annotations
import argparse, os
from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark
from jobs.utils.config_loader import load_yaml

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default="config/kafka.yaml")
    ap.add_argument("--bls", default="config/streaming_sources.yaml")
    ap.add_argument("--start", default="latest")
    ap.add_argument("--max-seconds", type=int, default=60)
    args = ap.parse_args()

    kfk = load_yaml(args.kafka)["kafka"]
    bls = load_yaml(args.bls)["bls"]
    out = bls.get("output", {})
    bronze_dir = out.get("bronze_dir_stream", "data/bronze_stream/bls")
    chk = out.get("checkpoint_bronze", "data/checkpoints/bls_bronze")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP") or kfk.get("bootstrap_host", "host.docker.internal:29092")
    topic = kfk["topic"]
    print(f"[consumer] bootstrap={bootstrap} topic={topic}")

    spark = get_spark("bls-consumer",
        packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4"])

    raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", args.start)
        .load())

    j = F.from_json(F.col("value").cast("string"), schema="""
        ts_ingested STRING, series_id STRING, year STRING, period STRING,
        periodName STRING, value STRING, footnotes ARRAY<STRING>, is_latest BOOLEAN
    """)

    df = raw.select(
        j.ts_ingested.alias("ts_ingested"),
        j.series_id.alias("series_id"),
        j.year.alias("year"),
        j.period.alias("period"),
        j.periodName.alias("periodName"),
        j.value.alias("value"),
        j.footnotes.alias("footnotes"),
        j.is_latest.alias("is_latest")
    )

    q = (df.writeStream
            .format("parquet")
            .option("path", bronze_dir)
            .option("checkpointLocation", chk)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start())

    if args.max_seconds > 0:
        q.awaitTermination(args.max_seconds)
        q.stop()
    else:
        q.awaitTermination()

if __name__ == "__main__":
    main()