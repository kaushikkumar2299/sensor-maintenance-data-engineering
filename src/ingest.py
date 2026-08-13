from pyspark.sql import SparkSession

from src.config import RAW_DATA_PATH

from src.schema import sensor_schema

# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("Sensor Maintenance Project")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    return spark


# ============================================================
# READ RAW SENSOR DATA
# ============================================================

def read_sensor_data(spark):

    sensor_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(sensor_schema)
        .load(str(RAW_DATA_PATH))
    )

    return sensor_df