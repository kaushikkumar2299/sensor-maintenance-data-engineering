from src.config import (
    SILVER_OUTPUT_PATH,
    GOLD_EQUIPMENT_OUTPUT_PATH,
    GOLD_FAILURE_OUTPUT_PATH
)
from src.ingest import (create_spark_session, 
                 read_sensor_data
)
from src.silver import (
    standardize_columns,
    apply_silver_business_transformations
)
from src.gold import (
    create_equipment_summary,
    create_failure_summary
)
from src.sql_analysis import run_sql_analysis

INSPECT_SPARK_UI = False

spark = create_spark_session()

# ============================================================
# 2. READ RAW SENSOR DATA
# ============================================================

sensor_df = read_sensor_data(spark)
# sensor_df.printSchema()

# =======================================
# =====================
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

silver_df = standardize_columns(sensor_df)


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

silver_df = apply_silver_business_transformations(silver_df)
# ============================================================
# 8. WRITE SILVER DATA
# ============================================================

silver_df.write \
    .mode("overwrite") \
    .parquet(str(SILVER_OUTPUT_PATH))
# ============================================================
# 9. GOLD LAYER - EQUIPMENT MAINTENANCE SUMMARY
# ============================================================

# ============================================================
# GOLD LAYER - EQUIPMENT MAINTENANCE SUMMARY
# ============================================================

gold_equipment_summary = create_equipment_summary(silver_df)

gold_equipment_summary.show(50, truncate=False)


# ============================================================
# 10. WRITE GOLD EQUIPMENT SUMMARY
# ============================================================

gold_equipment_summary.write \
    .mode("overwrite") \
    .parquet(str(GOLD_EQUIPMENT_OUTPUT_PATH))

gold_equipment_summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/bi/equipment_summary")
# ============================================================
# GOLD LAYER - FAILURE TYPE SUMMARY
# ============================================================

gold_failure_summary = create_failure_summary(silver_df)

gold_failure_summary.show(truncate=False)

# ============================================================
# 12. WRITE GOLD FAILURE SUMMARY
# ============================================================

gold_failure_summary.write \
    .mode("overwrite") \
    .parquet(str(GOLD_FAILURE_OUTPUT_PATH))

gold_failure_summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/bi/failure_summary")


# ============================================================
# SPARK SQL ANALYSIS
# ============================================================

sql_result = run_sql_analysis(silver_df, spark)

sql_result.show(truncate=False)
# ============================================================
# 14. STOP SPARK
# ============================================================
if INSPECT_SPARK_UI:
    input("Press Enter to stop Spark...")

spark.stop()