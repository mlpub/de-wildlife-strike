from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

import config


def _require(value: str, name: str) -> str:
    if not value:
        raise ValueError(f"Missing required config: {name}")
    return value


def _find_latest_csv(data_dir: str) -> Path:
    directory = Path(data_dir).expanduser().resolve()
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Local CSV folder not found: {directory}")

    candidates = [
        path
        for path in directory.glob("*.csv")
        if path.is_file()
    ]

    if not candidates:
        raise FileNotFoundError(f"No CSV found in local folder: {directory}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def ingest_latest_csv_to_bigquery() -> str:
    project_id = _require(config.GCP_PROJECT_ID, "GCP_PROJECT_ID")
    raw_dataset = config.BQ_RAW_DATASET
    raw_table = config.BQ_RAW_TABLE

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    csv_path = _find_latest_csv(config.DATA_DIR)

    bq_client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{raw_dataset}"
    staging_table = f"{dataset_ref}.{raw_table}_stage_{batch_id}"
    target_table = f"{dataset_ref}.{raw_table}"

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {csv_path} into staging table {staging_table}")
    with csv_path.open("rb") as csv_file:
        load_job = bq_client.load_table_from_file(
            file_obj=csv_file,
            destination=staging_table,
            job_config=load_config,
        )
    load_job.result()

    create_target_sql = f"""
    CREATE TABLE IF NOT EXISTS `{target_table}` AS
    SELECT
      s.*,
      CAST(NULL AS STRING) AS batch_id,
      CAST(NULL AS STRING) AS source_file_name,
      CAST(NULL AS TIMESTAMP) AS ingestion_timestamp
    FROM `{staging_table}` s
    WHERE 1 = 0
    """
    bq_client.query(create_target_sql).result()

    insert_sql = f"""
    INSERT INTO `{target_table}`
    SELECT
      s.*,
      @batch_id AS batch_id,
      @source_file_name AS source_file_name,
      CURRENT_TIMESTAMP() AS ingestion_timestamp
    FROM `{staging_table}` s
    """
    insert_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
            bigquery.ScalarQueryParameter("source_file_name", "STRING", csv_path.name),
        ]
    )
    bq_client.query(insert_sql, job_config=insert_config).result()

    row_count_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{target_table}`
    WHERE batch_id = @batch_id
    """
    count_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )
    row_count_result = bq_client.query(row_count_sql, job_config=count_config).result()
    loaded_rows = next(row_count_result).cnt

    bq_client.delete_table(staging_table, not_found_ok=True)
    print(f"Loaded {loaded_rows} rows into {target_table} for batch_id={batch_id}")
    return batch_id
