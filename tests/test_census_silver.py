from datetime import date
from pyspark.sql import Row, functions as F, types as T
from jobs.transformations.census_silver import transform_census_bronze_to_silver

def test_transform_basic(spark):
    rows = [
        {
            "data_type_code": "SM",
            "time_slot_id": "1",
            "seasonally_adj": "Yes",
            "category_code": "44X",
            "cell_value": "1234.5",
            "error_data": "N",
            "time": "2024-01",
            "ingest_date": "2025-09-17",
        },
        {
            "data_type_code": "SM",
            "time_slot_id": "2",
            "seasonally_adj": "No",
            "category_code": "722",
            "cell_value": "999",
            "error_data": "N",
            "time": "2024-02",
            "ingest_date": "2025-09-17",
        },
    ]
    df_bronze = spark.createDataFrame(rows)

    df = transform_census_bronze_to_silver(df_bronze)

    assert {"time", "date", "year", "month", "is_seasonally_adjusted", "category_code", "data_type_code",
            "time_slot_id", "cell_value", "error_data", "ingest_date"}.issubset(df.columns)

    dtypes = dict(df.dtypes)
    assert dtypes["time_slot_id"] == "int"
    assert dtypes["cell_value"] == "double"

    out = { (r.time, r.year, r.month): (r.is_seasonally_adjusted, r.cell_value) for r in df.collect() }
    assert out[("2024-01", 2024, 1)] == (True, 1234.5)
    assert out[("2024-02", 2024, 2)] == (False, 999.0)