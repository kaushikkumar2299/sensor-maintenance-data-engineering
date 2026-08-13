from pyspark.sql.functions import sum, avg, count, round


def create_equipment_summary(silver_df):

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

    return gold_equipment_summary

def create_failure_summary(silver_df):

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

    return gold_failure_summary