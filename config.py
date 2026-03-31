import os

# Local CSV source
DATA_DIR = os.getenv("DATA_DIR", "your-data-dir")

# GCP / BigQuery
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-proect-id")
GCP_REGION = os.getenv("GCP_REGION", "your-gcp-region")

# BigQuery datasets
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET", "dev_raw")
BQ_STG_DATASET = os.getenv("BQ_STG_DATASET", "dev_stg")
BQ_MART_DATASET = os.getenv("BQ_MART_DATASET", "dev_mart")
BQ_RAW_TABLE = os.getenv("BQ_RAW_TABLE", "raw_wildlife_strike")
