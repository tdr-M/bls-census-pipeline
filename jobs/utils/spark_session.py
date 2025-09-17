import os, sys
from pathlib import Path
from pyspark.sql import SparkSession

def _norm(p: str) -> str:
    return str(Path(p).resolve()).replace("\\", "/")

def get_spark(app: str = "bls-census-pipeline",
              packages: list[str] | None = None,
              extra_confs: dict[str, str] | None = None) -> SparkSession:
    py_exec = _norm(sys.executable)
    hadoop_home = _norm(os.environ.get("HADOOP_HOME"))
    bin_path = f"{hadoop_home}/bin"

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + bin_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exec
    os.environ["PYSPARK_PYTHON"] = py_exec
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    log4j = Path("config/log4j2.properties")
    jvm_opts = [f"-Djava.library.path={bin_path}", f"-Dhadoop.home.dir={hadoop_home}"]
    if log4j.exists():
        jvm_opts.append(f"-Dlog4j2.configurationFile=file:{_norm(str(log4j))}")

    builder = (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", " ".join(jvm_opts))
        .config("spark.executor.extraJavaOptions", " ".join(jvm_opts))
        .config("spark.driver.extraLibraryPath", bin_path)
        .config("spark.executor.extraLibraryPath", bin_path)
        .config("spark.pyspark.driver.python", py_exec)
        .config("spark.pyspark.python", py_exec)
        .config("spark.executorEnv.PYSPARK_PYTHON", py_exec)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", py_exec)
        .config("spark.executorEnv.PATH", os.environ["PATH"])
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)

    return builder.getOrCreate()