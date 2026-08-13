from pathlib import Path


# ============================================================
# PROJECT PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "sensor_maintenance_data.csv"
)

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