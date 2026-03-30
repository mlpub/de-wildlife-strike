output "raw_dataset_id" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "stg_dataset_id" {
  value = google_bigquery_dataset.stg.dataset_id
}

output "mart_dataset_id" {
  value = google_bigquery_dataset.mart.dataset_id
}

output "artifact_registry_repository" {
  value = google_artifact_registry_repository.wildlife.repository_id
}

output "dashboard_url" {
  value = google_cloud_run_v2_service.dashboard.uri
}
