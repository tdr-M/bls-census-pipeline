from pyspark.sql import SparkSession

def get_spark(app: str = "bls-census-pipeline"):
    return (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=file:config/log4j2.properties")
        .config("spark.executor.extraJavaOptions", "-Dlog4j2.configurationFile=file:config/log4j2.properties")
        .getOrCreate()
    )