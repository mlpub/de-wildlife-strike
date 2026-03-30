from jobs.ingestion_local_bq import ingest_latest_csv_to_bigquery


def run_ingestion() -> str:
    return ingest_latest_csv_to_bigquery()
