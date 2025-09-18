from __future__ import annotations
import os
import platform
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession


def _norm(p: Optional[str]) -> str:
    return str(Path(p).resolve()).replace("\\", "/") if p else ""


def get_spark(
    app_name: str,
    packages: Optional[List[str]] = None,
    conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Build a SparkSession that works on both Windows (host) and Linux (containers).
    - On Linux (Airflow/Kafka containers), we do NOT touch HADOOP_HOME.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
    )

    #Hadoop is important only for windows hosts!!!!
    if platform.system().lower().startswith("win"):
        hh = _norm(os.environ.get("HADOOP_HOME"))
        if hh:
            os.environ["HADOOP_HOME"] = hh

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    if conf:
        for k, v in conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    return spark