def run_sql_analysis(silver_df, spark):

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

    return sql_result