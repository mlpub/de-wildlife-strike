from prefect import flow, task

from jobs.ingest import run_ingestion
from jobs.run_dbt import run_dbt


@task
def ingest_task() -> str:
    return run_ingestion()


@task
def dbt_task(batch_id: str) -> None:
    run_dbt(batch_id)


@flow
def wildlife_pipeline() -> None:
    batch_id = ingest_task()
    dbt_task(batch_id)


if __name__ == "__main__":
    wildlife_pipeline()
