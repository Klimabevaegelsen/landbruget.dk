# variables.tf | Variable Definitions for Mage AI on GCP

#--------------------------------------------------------------
# Project Configuration
#--------------------------------------------------------------

variable "project_id" {
  type        = string
  description = "The GCP project ID where resources will be deployed"
  default     = "unique-gcp-project-id"
}

variable "region" {
  type        = string
  description = "The GCP region where resources will be deployed"
  default     = "europe-west1"
}

variable "zone" {
  type        = string
  description = "The GCP zone for zonal resources"
  default     = "europe-west1-b"
}

variable "user_email" {
  type        = string
  description = "Email address of the user to grant project-level access for Cloud Run (not used directly as it's fetched from Secret Manager)"
  default     = "placeholder-not-used"  # This is not used since we fetch from Secret Manager
}

#--------------------------------------------------------------
# Application Configuration
#--------------------------------------------------------------

variable "app_name" {
  type        = string
  description = "Name of the Mage AI application (used for resource naming)"
  default     = "mage-data-prep"
}

variable "docker_image" {
  type        = string
  description = "Docker image to deploy to Cloud Run"
  default     = "mageai/mageai:latest"
}

variable "repository" {
  type        = string
  description = "Name of the Artifact Registry repository"
  default     = "mage-data-prep"
}

variable "container_cpu" {
  type        = string
  description = "CPU allocation for the Cloud Run container (e.g., '1000m' = 1 vCPU)"
  default     = "1000m"
}

variable "container_memory" {
  type        = string
  description = "Memory allocation for the Cloud Run container"
  default     = "1G"
}

#--------------------------------------------------------------
# Database Configuration
#--------------------------------------------------------------

variable "database_user" {
  type        = string
  description = "Username for the PostgreSQL database (not used directly as it's fetched from Secret Manager)"
  default     = "placeholder-not-used"  # This is not used since we fetch from Secret Manager
}

variable "database_password" {
  type        = string
  description = "Password for the PostgreSQL database (not used directly as it's fetched from Secret Manager)"
  sensitive   = true
  default     = "placeholder-not-used"  # This is not used since we fetch from Secret Manager
}

#--------------------------------------------------------------
# Load Balancer Configuration
#--------------------------------------------------------------

variable "use_load_balancer" {
  type        = bool
  description = "Whether to deploy a load balancer in front of Cloud Run. Set to false to use the default Cloud Run URL and save costs."
  default     = false  # Default to not using a load balancer for cost savings
}

variable "domain" {
  type        = string
  description = "Domain name for the load balancer (required if SSL is enabled)"
  default     = ""
}

variable "ssl" {
  type        = bool
  description = "Whether to enable HTTPS and provision a managed certificate for the domain"
  default     = false
}
