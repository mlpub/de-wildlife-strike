variable "project_id" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "Primary region for resources"
  type        = string
  default     = "your-gcp-region"
}

variable "env" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "dashboard_image" {
  description = "Container image URI for Streamlit dashboard"
  type        = string

  validation {
    condition     = !strcontains(var.dashboard_image, "your-gcp-project-id")
    error_message = "dashboard_image must not contain the placeholder 'your-gcp-project-id'."
  }
}
