from pyspark.sql.functions import col, when

def standardize_columns(sensor_df):

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

    return silver_df

def apply_silver_business_transformations(silver_df):

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

    return silver_df