terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  name_prefix     = "wildlife-${var.env}"
  raw_dataset_id  = "${var.env}_raw"
  stg_dataset_id  = "${var.env}_stg"
  mart_dataset_id = "${var.env}_mart"
}

resource "google_project_service" "required" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com"
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "wildlife" {
  depends_on = [google_project_service.required]

  project       = var.project_id
  location      = var.region
  repository_id = "wildlife"
  format        = "DOCKER"

  description = "Docker repository for wildlife dashboard image"
}

resource "google_bigquery_dataset" "raw" {
  depends_on = [google_project_service.required]

  dataset_id                 = local.raw_dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "stg" {
  depends_on = [google_project_service.required]

  dataset_id                 = local.stg_dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "mart" {
  depends_on = [google_project_service.required]

  dataset_id                 = local.mart_dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_service_account" "dashboard" {
  account_id   = "${local.name_prefix}-dashboard-sa"
  display_name = "Dashboard Service Account"
}

resource "google_project_iam_member" "dashboard_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_project_iam_member" "dashboard_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_bigquery_dataset_iam_member" "dashboard_mart_viewer" {
  dataset_id = google_bigquery_dataset.mart.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_artifact_registry_repository_iam_member" "dashboard_repo_reader_dashboard_sa" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.wildlife.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_artifact_registry_repository_iam_member" "dashboard_repo_reader_cloud_run_service_agent" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.wildlife.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:service-${data.google_project.current.number}@serverless-robot-prod.iam.gserviceaccount.com"
}

resource "google_cloud_run_v2_service" "dashboard" {
  name     = "${local.name_prefix}-dashboard"
  location = var.region

  template {
    service_account = google_service_account.dashboard.email

    containers {
      image = var.dashboard_image

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "BQ_MART_DATASET"
        value = google_bigquery_dataset.mart.dataset_id
      }
    }
  }

  ingress = "INGRESS_TRAFFIC_ALL"
}

resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
