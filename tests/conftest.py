import os
import sys
from pathlib import Path
import pytest

from jobs.utils.spark_session import get_spark

@pytest.fixture(scope="session")
def spark():
    hadoop_home = os.getenv("HADOOP_HOME")
    bin_path = f"{hadoop_home}/bin"

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + bin_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_PYTHON"] = sys.executable

    spark = get_spark("tests")
    yield spark
    spark.stop()