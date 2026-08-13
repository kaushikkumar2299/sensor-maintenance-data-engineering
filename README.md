# Sensor Maintenance Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline using PySpark to process manufacturing sensor and maintenance data.

The pipeline reads raw sensor data from CSV, standardizes and transforms the data into a Silver layer, creates aggregated Gold datasets for maintenance and failure analysis, writes the processed datasets in Parquet format, and performs analytical queries using Spark SQL.

The project follows a modular structure where configuration, ingestion, transformation, aggregation, and SQL analysis are separated into reusable Python modules.

---

## Dataset

The dataset contains 500 manufacturing sensor observations collected from 50 pieces of equipment.

It includes operational and maintenance information such as:

- Voltage, current, power, and humidity readings
- Equipment and ambient temperature
- Vibration measurements
- Equipment operational status and criticality
- Fault status and failure type
- Maintenance type, repair time, and maintenance cost
- Historical failure information
- Predictive maintenance trigger indicators

The dataset is used to build a data pipeline that prepares sensor data for equipment reliability, failure, and predictive maintenance analysis.

### Dataset Profile

- Total records: **500**
- Total columns: **27**
- Unique sensors: **500**
- Unique equipment: **50**
- Observations per equipment: **10**
- Null values: **0**
- Exact duplicate rows: **0**

The dataset grain represents one sensor observation associated with one piece of equipment at a particular timestamp.

---

## Tech Stack

- Python
- PySpark 4.2.0
- Apache Spark
- Spark SQL
- Parquet
- Git
- GitHub

---

## Pipeline Architecture

```text
Raw CSV
   ↓
ingest.py
   ↓
Raw Spark DataFrame
   ↓
silver.py
   ↓
Standardized and Transformed Silver DataFrame
   ↓
Silver Parquet Output
   ↓
gold.py
   ↓
Equipment Summary + Failure Summary
   ↓
Gold Parquet Outputs
   ↓
sql_analysis.py
   ↓
Spark SQL Analysis
```

The pipeline is orchestrated by `main.py`, which calls each module in sequence and manages the end-to-end execution.

---

## Data Pipeline

### Raw Layer

The raw manufacturing sensor dataset is ingested from CSV using PySpark with schema inference enabled.

During development, exploratory profiling was performed to understand:

- Dataset schema
- Row and partition counts
- Null values
- Duplicate records
- Sensor and equipment cardinality
- Fault and failure distributions
- Relationships between failure types and predictive maintenance triggers
- Numerical value distributions
- Categorical value distributions

The raw CSV acts as the source dataset for the pipeline.

---

### Silver Layer

The Silver layer prepares the raw data for downstream analytics.

Transformations include:

- Standardizing column names
- Creating human-readable fault and maintenance flags
- Calculating the difference between equipment and ambient temperature
- Writing the standardized and transformed dataset in Parquet format

Examples of standardized column names:

```text
Sensor_ID                 → sensor_id
Equipment_ID              → equipment_id
Temperature (°C)          → temperature_c
Vibration (m/s²)          → vibration_ms2
Repair Time (hrs)         → repair_time_hours
Maintenance Costs (USD)   → maintenance_cost_usd
```

The following derived columns are created:

### `fault_flag`

Converts the numeric fault indicator into a readable value:

```text
1 → Yes
0 → No
```

### `maintenance_trigger_flag`

Converts the predictive maintenance trigger into a readable value:

```text
1 → Yes
0 → No
```

### `temperature_difference_c`

Calculates the difference between equipment temperature and ambient temperature:

```text
temperature_c - ambient_temperature_c
```

The transformed Silver dataset is written to:

```text
output/silver/sensor_maintenance/
```

---

### Gold Layer

The Gold layer creates business-ready aggregated datasets from the Silver data.

Two Gold datasets are produced.

### Equipment Maintenance Summary

Aggregates reliability and maintenance metrics by equipment, including:

- Equipment ID
- Equipment criticality
- Total observations
- Total faults
- Fault rate percentage
- Predictive maintenance triggers
- Average temperature
- Average vibration
- Average maintenance cost
- Average repair time

The dataset is written to:

```text
output/gold/equipment_summary/
```

### Failure Summary

Aggregates metrics by failure type, including:

- Failure type
- Total records
- Total faults
- Predictive maintenance triggers
- Average temperature
- Average vibration
- Average maintenance cost
- Average repair time

The dataset is written to:

```text
output/gold/failure_summary/
```

---

## Spark SQL Analysis

The Silver DataFrame is registered as a temporary Spark SQL view to enable SQL-based analysis of the transformed sensor data.

The Spark SQL analysis groups the data by equipment criticality and calculates:

- Total records
- Total detected faults
- Fault rate percentage
- Predictive maintenance trigger counts

This demonstrates the use of both the PySpark DataFrame API and Spark SQL within the same data pipeline.

---

## Project Structure

```text
sensor_maintenance_data_engineering/
│
├── data/
│   └── sensor_maintenance_data.csv
│
├── output/
│   ├── silver/
│   │   └── sensor_maintenance/
│   │
│   └── gold/
│       ├── equipment_summary/
│       └── failure_summary/
│
├── src/
│   ├── config.py
│   ├── ingest.py
│   ├── silver.py
│   ├── gold.py
│   ├── sql_analysis.py
│   └── main.py
│
├── README.md
├── requirements.txt
└── .gitignore
```

The `output/` directory is excluded from Git because it contains generated pipeline outputs that can be recreated by running the application.

---

## Source Modules

### `config.py`

Defines reusable project paths for:

- Raw input data
- Silver output
- Gold equipment summary
- Gold failure summary

The project root is determined dynamically using Python's `pathlib`, avoiding hard-coded local project paths.

### `ingest.py`

Responsible for:

- Creating the Spark session
- Configuring Spark
- Reading the raw sensor dataset

### `silver.py`

Responsible for:

- Standardizing column names
- Creating fault flags
- Creating maintenance trigger flags
- Calculating equipment-to-ambient temperature differences

### `gold.py`

Responsible for creating:

- Equipment-level maintenance summary
- Failure-type summary

### `sql_analysis.py`

Responsible for running Spark SQL analysis on the transformed Silver dataset.

### `main.py`

Acts as the pipeline entry point and orchestrates the complete workflow:

```text
Create Spark Session
        ↓
Read Raw Data
        ↓
Silver Transformation
        ↓
Write Silver Data
        ↓
Create Gold Aggregations
        ↓
Write Gold Data
        ↓
Run Spark SQL Analysis
        ↓
Stop Spark Session
```

---

## Setup and Installation

### Prerequisites

Ensure the following are installed:

- Python
- Java
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/kaushikkumar2299/sensor-maintenance-data-engineering.git
cd sensor-maintenance-data-engineering
```

### 2. Create a Virtual Environment

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Pipeline

Run the pipeline from the project root:

```bash
python -m src.main
```

The pipeline reads the raw CSV dataset, performs the Silver and Gold transformations, writes the processed datasets in Parquet format, and executes the Spark SQL analysis.

---

## Spark UI Inspection

The pipeline contains the following setting in `src/main.py`:

```python
INSPECT_SPARK_UI = True
```

When this value is set to `True`, the Spark application remains active after processing so the Spark UI can be inspected at:

```text
http://localhost:4040
```

After inspecting the Spark UI, press Enter in the terminal to stop the Spark application.

To allow the pipeline to terminate automatically, change the setting to:

```python
INSPECT_SPARK_UI = False
```

---

## Generated Outputs

Running the pipeline generates the following datasets.

### Silver

```text
output/silver/sensor_maintenance/
```

Contains standardized and transformed sensor-level data stored in Parquet format.

### Gold - Equipment Summary

```text
output/gold/equipment_summary/
```

Contains equipment-level reliability and maintenance metrics.

### Gold - Failure Summary

```text
output/gold/failure_summary/
```

Contains failure-type-level maintenance and fault metrics.

The `output/` directory is excluded from Git using `.gitignore` because these datasets are generated by the pipeline and can be recreated by running the application.

---

## Key Insights

Initial profiling and analysis of the sensor maintenance dataset identified:

- 500 sensor observations across 50 pieces of equipment
- Each equipment ID contains 10 observations
- 167 observations contain detected faults
- 333 observations contain no detected fault
- 125 observations trigger predictive maintenance
- Failure types include Overload, Overheating, and None
- All Overload observations are associated with a predictive maintenance trigger in the provided dataset
- Equipment criticality is evenly distributed between High and Medium
- The dataset contains no null values
- The dataset contains no exact duplicate rows

Spark SQL analysis by equipment criticality produced:

```text
Criticality   Records   Faults   Fault Rate   Triggers
High          250       84       33.60%       125
Medium        250       83       33.20%       0
```

The Gold layer provides equipment-level and failure-level metrics that can be used for maintenance monitoring and downstream analytics.

The relationship between `Overload` and the predictive maintenance trigger is specific to this dataset and should be considered carefully if the data is later used for machine learning, since it may introduce target leakage.

---

## Engineering Concepts Demonstrated

This project demonstrates:

- PySpark DataFrame processing
- Spark transformations and actions
- Modular data pipeline development
- Raw-to-Silver-to-Gold data processing
- Data profiling
- Column standardization
- Derived column creation
- Aggregations
- Spark SQL
- Parquet storage
- Spark partition inspection
- Spark DAG and stage inspection
- Git version control
- GitHub repository management

---

## Future Improvements

Potential extensions to the project include:

- Defining an explicit Spark schema instead of relying on schema inference
- Adding automated data quality validation rules
- Adding structured logging and error handling
- Adding automated tests for transformation logic
- Implementing incremental data processing
- Adding partitioned Parquet output for larger datasets
- Orchestrating the pipeline using a workflow scheduler such as Apache Airflow
- Loading Gold datasets into a cloud data warehouse
- Building a BI dashboard using the Gold datasets
- Processing larger sensor datasets using distributed Spark execution