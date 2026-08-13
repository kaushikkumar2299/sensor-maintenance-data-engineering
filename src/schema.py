from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType
)


sensor_schema = StructType([
    StructField("Sensor_ID", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Voltage (V)", IntegerType(), True),
    StructField("Current (A)", DoubleType(), True),
    StructField("Temperature (°C)", IntegerType(), True),
    StructField("Power (W)", DoubleType(), True),
    StructField("Humidity (%)", IntegerType(), True),
    StructField("Vibration (m/s²)", DoubleType(), True),
    StructField("Equipment_ID", StringType(), True),
    StructField("Operational Status", StringType(), True),
    StructField("Fault Status", StringType(), True),
    StructField("Failure Type", StringType(), True),
    StructField("Last Maintenance Date", DateType(), True),
    StructField("Maintenance Type", StringType(), True),
    StructField("Failure History", StringType(), True),
    StructField("Repair Time (hrs)", IntegerType(), True),
    StructField("Maintenance Costs (USD)", IntegerType(), True),
    StructField("Ambient Temperature (°C)", IntegerType(), True),
    StructField("Ambient Humidity (%)", IntegerType(), True),
    StructField("External Factors", StringType(), True),
    StructField("X", IntegerType(), True),
    StructField("Y", IntegerType(), True),
    StructField("Z", IntegerType(), True),
    StructField("Equipment Relationship", StringType(), True),
    StructField("Equipment Criticality", StringType(), True),
    StructField("Fault Detected", IntegerType(), True),
    StructField("Predictive Maintenance Trigger", IntegerType(), True)
])