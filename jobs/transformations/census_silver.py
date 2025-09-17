from __future__ import annotations
from pyspark.sql import DataFrame, functions as F, types as T

REQUIRED_COLS = {
    "data_type_code",
    "time_slot_id",
    "seasonally_adj",
    "category_code",
    "cell_value",
    "error_data",
    "time",
}

def _bool_from_seasonal(col: F.Column) -> F.Column:
    c = F.lower(F.trim(col))
    return F.when(c.isin("yes", "y", "true", "t", "1"), F.lit(True)) \
            .when(c.isin("no", "n", "false", "f", "0"), F.lit(False)) \
            .otherwise(F.lit(None).cast("boolean"))

def _ym_to_date(col: F.Column) -> F.Column:
    y = F.substring(col, 1, 4)
    m = F.substring(col, 6, 2)
    return F.to_date(F.concat_ws("-", y, m, F.lit("01")), "yyyy-MM-dd")

def transform_census_bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    missing = REQUIRED_COLS - set(df_bronze.columns)
    if missing:
        raise ValueError(f"Bronze missing columns: {sorted(missing)}")

    df = df_bronze

    df = df.withColumn("time_slot_id", F.col("time_slot_id").cast(T.IntegerType()))
    df = df.withColumn(
        "cell_value",
        F.regexp_replace(F.col("cell_value"), ",", "").cast(T.DoubleType()),
    )
    df = df.withColumn("is_seasonally_adjusted", _bool_from_seasonal(F.col("seasonally_adj")))
    df = df.withColumn("date", _ym_to_date(F.col("time")))
    df = df.withColumn("year", F.year("date"))
    df = df.withColumn("month", F.month("date"))

    ingest_col = "ingest_date" if "ingest_date" in df.columns else None

    ordered_cols = [
        "time", "date", "year", "month",
        "is_seasonally_adjusted",
        "category_code", "data_type_code", "time_slot_id",
        "cell_value", "error_data",
    ] + ([ingest_col] if ingest_col else [])

    return df.select(*ordered_cols)