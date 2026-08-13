from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import col, sum, avg, count , round
from pathlib import Path

# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = PROJECT_ROOT / "data" / "sensor_maintenance_data.csv"

SILVER_OUTPUT_PATH = (
    PROJECT_ROOT
    / "output"
    / "silver"
    / "sensor_maintenance"
)

GOLD_EQUIPMENT_OUTPUT_PATH = (
    PROJECT_ROOT
    / "output"
    / "gold"
    / "equipment_summary"
)

GOLD_FAILURE_OUTPUT_PATH = (
    PROJECT_ROOT
    / "output"
    / "gold"
    / "failure_summary"
)

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Sensor Maintenance Project")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)


# ============================================================
# 2. READ RAW SENSOR DATA
# ============================================================

sensor_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(str(RAW_DATA_PATH))
)


# ============================================================
# 3. INITIAL DATA PROFILING
# ============================================================

# ------------------------------------------------------------
# 3.1 View sample records
# ------------------------------------------------------------

# sensor_df.show(10)


# ------------------------------------------------------------
# 3.2 Inspect schema
# ------------------------------------------------------------

# sensor_df.printSchema()


# ------------------------------------------------------------
# 3.3 Check total records and partitions
# ------------------------------------------------------------

# print("Total rows:", sensor_df.count())

# print(
#     "Total partitions:",
#     sensor_df.rdd.getNumPartitions()
# )


# ------------------------------------------------------------
# 3.4 Check NULL values
# ------------------------------------------------------------

# null_counts = sensor_df.select(
#     *[
#         sum(
#             col(c).isNull().cast("int")
#         ).alias(c)
#         for c in sensor_df.columns
#     ]
# )

# null_counts.show()


# ------------------------------------------------------------
# 3.5 Check exact duplicate rows
# ------------------------------------------------------------

# print("Total rows:", sensor_df.count())

# print(
#     "Distinct rows:",
#     sensor_df.distinct().count()
# )


# ------------------------------------------------------------
# 3.6 Check unique Sensor and Equipment IDs
# ------------------------------------------------------------

# print(
#     "Distinct Sensor IDs:",
#     sensor_df.select("Sensor_ID").distinct().count()
# )

# print(
#     "Distinct Equipment IDs:",
#     sensor_df.select("Equipment_ID").distinct().count()
# )


# ------------------------------------------------------------
# 3.7 Check number of observations per Sensor
# ------------------------------------------------------------

# sensor_df.groupBy("Sensor_ID") \
#     .count() \
#     .orderBy("Sensor_ID") \
#     .show()


# ------------------------------------------------------------
# 3.8 Check number of observations per Equipment
# ------------------------------------------------------------

# sensor_df.groupBy("Equipment_ID") \
#     .count() \
#     .orderBy("Equipment_ID") \
#     .show(50)


# ------------------------------------------------------------
# 3.9 Equipment Criticality distribution
# ------------------------------------------------------------

# sensor_df.groupBy(
#     "Equipment_ID",
#     "Equipment Criticality"
# ).count() \
#  .orderBy("Equipment_ID") \
#  .show(50)


# ------------------------------------------------------------
# 3.10 Fault Status distribution
# ------------------------------------------------------------

# sensor_df.groupBy("Fault Status") \
#     .count() \
#     .show()


# ------------------------------------------------------------
# 3.11 Failure Type distribution
# ------------------------------------------------------------

# sensor_df.groupBy("Failure Type") \
#     .count() \
#     .show()


# ------------------------------------------------------------
# 3.12 Fault Status vs Failure Type
# ------------------------------------------------------------

# sensor_df.groupBy(
#     "Fault Status",
#     "Failure Type"
# ).count() \
#  .orderBy(
#      "Fault Status",
#      "Failure Type"
#  ).show()


# ------------------------------------------------------------
# 3.13 Fault Detected vs Predictive Maintenance Trigger
# ------------------------------------------------------------

# sensor_df.groupBy(
#     "Fault Detected",
#     "Predictive Maintenance Trigger"
# ).count() \
#  .orderBy(
#      "Fault Detected",
#      "Predictive Maintenance Trigger"
#  ).show()


# ------------------------------------------------------------
# 3.14 Failure Type vs Predictive Maintenance Trigger
# ------------------------------------------------------------

# sensor_df.groupBy(
#     "Failure Type",
#     "Predictive Maintenance Trigger"
# ).count() \
#  .orderBy(
#      "Failure Type",
#      "Predictive Maintenance Trigger"
#  ).show()


# ============================================================
# 4. SILVER LAYER - COLUMN STANDARDIZATION
# ============================================================

silver_df = sensor_df

for old_col in sensor_df.columns:

    new_col = (
        old_col
        .lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "pct")
        .replace("°", "")
        .replace("²", "2")
    )

    silver_df = silver_df.withColumnRenamed(
        old_col,
        new_col
    )


# Fix specific column names that still need cleanup

silver_df = (
    silver_df
    .withColumnRenamed(
        "vibration_m/s2",
        "vibration_ms2"
    )
    .withColumnRenamed(
        "repair_time_hrs",
        "repair_time_hours"
    )
    .withColumnRenamed(
        "maintenance_costs_usd",
        "maintenance_cost_usd"
    )
)


# ============================================================
# 5. SILVER DATA VALIDATION
# ============================================================

# Numeric column statistics

# silver_df.select(
#     "voltage_v",
#     "current_a",
#     "temperature_c",
#     "power_w",
#     "humidity_pct",
#     "vibration_ms2",
#     "repair_time_hours",
#     "maintenance_cost_usd"
# ).summary().show()


# Validate fault_detected values

# silver_df.groupBy(
#     "fault_detected"
# ).count().show()


# Validate predictive maintenance trigger values

# silver_df.groupBy(
#     "predictive_maintenance_trigger"
# ).count().show()

# ============================================================
# 6. SILVER DATA VALIDATION - CATEGORICAL
# ============================================================

# categorical_columns = [
#     "operational_status",
#     "fault_status",
#     "failure_type",
#     "maintenance_type",
#     "failure_history",
#     "external_factors",
#     "equipment_relationship",
#     "equipment_criticality"
# ]

# for c in categorical_columns:
#     print(f"\nDistinct values for {c}:")

#     silver_df.groupBy(c) \
#         .count() \
#         .orderBy(c) \
#         .show()
# ============================================================
# 7. SILVER BUSINESS TRANSFORMATIONS
# ============================================================

silver_df = (
    silver_df

    .withColumn(
        "fault_flag",
        when(col("fault_detected") == 1, "Yes")
        .otherwise("No")
    )

    .withColumn(
        "maintenance_trigger_flag",
        when(col("predictive_maintenance_trigger") == 1, "Yes")
        .otherwise("No")
    )

    .withColumn(
        "temperature_difference_c",
        col("temperature_c") - col("ambient_temperature_c")
    )
)
# silver_df.select(
#     "sensor_id",
#     "equipment_id",
#     "temperature_c",
#     "ambient_temperature_c",
#     "temperature_difference_c",
#     "fault_flag",
#     "maintenance_trigger_flag"
# ).show(20)
# ============================================================
# 8. WRITE SILVER DATA
# ============================================================

silver_df.write \
    .mode("overwrite") \
    .parquet(str(SILVER_OUTPUT_PATH))
# ============================================================
# 9. GOLD LAYER - EQUIPMENT MAINTENANCE SUMMARY
# ============================================================

gold_equipment_summary = (
    silver_df
    .groupBy(
        "equipment_id",
        "equipment_criticality"
    )
    .agg(
        count("*").alias("total_observations"),

        sum("fault_detected").alias("total_faults"),

        round(
            sum("fault_detected") / count("*") * 100,
            2
        ).alias("fault_rate_pct"),

        sum("predictive_maintenance_trigger")
        .alias("maintenance_triggers"),

        round(
            avg("temperature_c"),
            2
        ).alias("avg_temperature_c"),

        round(
            avg("vibration_ms2"),
            2
        ).alias("avg_vibration_ms2"),

        round(
            avg("maintenance_cost_usd"),
            2
        ).alias("avg_maintenance_cost_usd"),

        round(
            avg("repair_time_hours"),
            2
        ).alias("avg_repair_time_hours")
    )
    .orderBy("equipment_id")
)

gold_equipment_summary.show(50, truncate=False)

# ============================================================
# 10. WRITE GOLD EQUIPMENT SUMMARY
# ============================================================

gold_equipment_summary.write \
    .mode("overwrite") \
    .parquet(str(GOLD_EQUIPMENT_OUTPUT_PATH))
# ============================================================
# 11. GOLD LAYER - FAILURE TYPE SUMMARY
# ============================================================

gold_failure_summary = (
    silver_df
    .groupBy("failure_type")
    .agg(
        count("*").alias("total_records"),

        sum("fault_detected")
        .alias("total_faults"),

        sum("predictive_maintenance_trigger")
        .alias("maintenance_triggers"),

        round(
            avg("temperature_c"),
            2
        ).alias("avg_temperature_c"),

        round(
            avg("vibration_ms2"),
            2
        ).alias("avg_vibration_ms2"),

        round(
            avg("repair_time_hours"),
            2
        ).alias("avg_repair_time_hours"),

        round(
            avg("maintenance_cost_usd"),
            2
        ).alias("avg_maintenance_cost_usd")
    )
    .orderBy("failure_type")
)
# ============================================================
# 12. WRITE GOLD FAILURE SUMMARY
# ============================================================

gold_failure_summary.write \
    .mode("overwrite") \
    .parquet(str(GOLD_FAILURE_OUTPUT_PATH))

# ============================================================
# 13. SPARK SQL ANALYSIS
# ============================================================

silver_df.createOrReplaceTempView("sensor_maintenance")

sql_result = spark.sql("""
    SELECT
        equipment_criticality,
        COUNT(*) AS total_records,
        SUM(fault_detected) AS total_faults,
        ROUND(
            SUM(fault_detected) * 100.0 / COUNT(*),
            2
        ) AS fault_rate_pct,
        SUM(predictive_maintenance_trigger) AS maintenance_triggers
    FROM sensor_maintenance
    GROUP BY equipment_criticality
    ORDER BY equipment_criticality
""")

sql_result.show(truncate=False)
# ============================================================
# 14. STOP SPARK
# ============================================================
input("Press Enter to stop Spark...")
spark.stop()