from __future__ import annotations
import argparse
import logging
from pathlib import Path

from jobs.utils.config_loader import load_yaml
from jobs.utils.logging_setup import setup_logging
from jobs.utils.spark_session import get_spark
from jobs.transformations.census_silver import transform_census_bronze_to_silver

LOG = logging.getLogger("jobs.census_silver")

def main():
    setup_logging()
    cfg = load_yaml("config/sources.yaml")["census"]
    bronze_dir = Path(cfg["output"]["bronze_dir"])
    silver_dir = Path(cfg["output"]["silver_dir"])

    spark = get_spark("census-silver")

    df_bronze = spark.read.parquet(str(bronze_dir))

    df_silver = transform_census_bronze_to_silver(df_bronze)

    if "ingest_date" in df_silver.columns:
        partition_cols = ["ingest_date"]
    else:
        partition_cols = ["year", "month"]

    out_path = str(silver_dir)
    (
        df_silver
        .repartition(*partition_cols)
        .write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(out_path)
    )

    LOG.info("Silver written: %s (partitions: %s)", out_path, partition_cols)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Census bronze → silver")
    _ = parser.parse_args()
    main()