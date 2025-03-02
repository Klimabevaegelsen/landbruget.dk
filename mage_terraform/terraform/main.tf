# main.tf - Mage AI on Google Cloud Platform

terraform {
  required_version = ">= 0.14"

  required_providers {
    # Cloud Run support was added on 3.3.0
    google = ">= 3.3"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Get project information
data "google_project" "project" {
}

# Fetch database password from Secret Manager
data "google_secret_manager_secret_version" "db_password" {
  secret = "mage-database-password"
  version = "latest"
}

# Fetch database username from Secret Manager
data "google_secret_manager_secret_version" "db_user" {
  secret = "mage-database-user"
  version = "latest"
}

# Fetch user email from Secret Manager
data "google_secret_manager_secret_version" "user_email" {
  secret = "mage-user-email"
  version = "latest"
}

# Fetch service account from Secret Manager
data "google_secret_manager_secret_version" "service_account" {
  secret = "mage-service-account"
  version = "latest"
}

# #############################################
# #               Enable API's                #
# #############################################

# Enable required Google Cloud APIs
resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudrun" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "resourcemanager" {
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "vpcaccess" {
  service            = "vpcaccess.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "sqladmin" {
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

# #############################################
# #            Cloud Run Service              #
# #############################################

# Create the Cloud Run service for Mage AI
resource "google_cloud_run_service" "run_service" {
  name     = var.app_name
  location = var.region

  template {
    spec {
      containers {
        image = var.docker_image
        ports {
          container_port = 6789
        }
        resources {
          limits = {
            cpu    = var.container_cpu
            memory = var.container_memory
          }
        }
        
        # Infrastructure configuration
        env {
          name  = "FILESTORE_IP_ADDRESS"
          value = module.nfs.internal_ip
        }
        env {
          name  = "FILE_SHARE_NAME"
          value = "share/mage"
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "GCP_SERVICE_NAME"
          value = var.app_name
        }
        env {
          name  = "MAGE_DATABASE_CONNECTION_URL"
          value = "postgresql://${data.google_secret_manager_secret_version.db_user.secret_data}:${data.google_secret_manager_secret_version.db_password.secret_data}@/${var.app_name}-db?host=/cloudsql/${google_sql_database_instance.instance.connection_name}"
        }
        env {
          name  = "ULIMIT_NO_FILE"
          value = 16384
        }
        env {
          name  = "PLATFORM"
          value = "linux/amd64"
        }
        
        # Mage environment configuration
        env {
          name  = "MAGE_ENVIRONMENT"
          value = "production"
        }
        
        # Authentication configuration
        env {
          name  = "AUTHENTICATION_MODE"
          value = "GOOGLE_OAUTH2"
        }
        env {
          name  = "REQUIRE_USER_AUTHENTICATION"
          value = "1"
        }
        env {
          name  = "GOOGLE_CLIENT_ID"
          value_from {
            secret_key_ref {
              name = "mage-oauth-client-id"
              key  = "latest"
            }
          }
        }
        env {
          name  = "GOOGLE_CLIENT_SECRET"
          value_from {
            secret_key_ref {
              name = "mage-oauth-client-secret"
              key  = "latest"
            }
          }
        }
        
        # Git integration configuration
        env {
          name  = "GIT_REPO_LINK"
          value = "https://github.com/klimabevaegelsen/landbrugsdata.git"
        }
        env {
          name  = "GIT_BRANCH"
          value = "main"
        }
        env {
          name  = "GIT_SYNC_ON_START"
          value = "1"
        }
        env {
          name  = "GIT_SYNC_ON_PIPELINE_COMPLETION"
          value = "1"
        }
        env {
          name  = "GIT_USERNAME"
          value_from {
            secret_key_ref {
              name = "git-username"
              key  = "latest"
            }
          }
        }
        env {
          name  = "GIT_EMAIL"
          value_from {
            secret_key_ref {
              name = "git-email"
              key  = "latest"
            }
          }
        }
        env {
          name  = "GIT_AUTH_TOKEN"
          value_from {
            secret_key_ref {
              name = "git-auth-token"
              key  = "latest"
            }
          }
        }
      }
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale"         = "1"
        "run.googleapis.com/cloudsql-instances"    = google_sql_database_instance.instance.connection_name
        "run.googleapis.com/cpu-throttling"        = false
        "run.googleapis.com/execution-environment" = "gen2"
        "run.googleapis.com/vpc-access-connector"  = google_vpc_access_connector.connector.id
        "run.googleapis.com/vpc-access-egress"     = "private-ranges-only"
        # This will be set by Google Cloud automatically
        # "run.googleapis.com/oauth2-id-token-audience" = "https://${var.app_name}-${substr(data.google_project.project.number, 0, 5)}${substr(var.region, 0, 2)}.a.run.app"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  metadata {
    annotations = {
      "run.googleapis.com/launch-stage" = "BETA"
      "run.googleapis.com/ingress"      = "all"
    }
  }

  autogenerate_revision_name = true
  depends_on = [google_project_service.cloudrun]

  # Ignore changes to annotations managed by Google Cloud
  lifecycle {
    ignore_changes = [
      metadata[0].annotations["run.googleapis.com/urls"],
      metadata[0].annotations["run.googleapis.com/launch-stage"],
      template[0].metadata[0].annotations["run.googleapis.com/oauth2-id-token-audience"]
    ]
  }
}

# #############################################
# #            IAM Permissions                #
# #############################################

# Allow public access to the service
resource "google_cloud_run_service_iam_member" "run_all_users" {
  service  = google_cloud_run_service.run_service.name
  location = google_cloud_run_service.run_service.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Grant additional Cloud Run roles to the user
resource "google_project_iam_member" "project_cloud_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "user:${data.google_secret_manager_secret_version.user_email.secret_data}"
}

resource "google_project_iam_member" "project_cloud_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "user:${data.google_secret_manager_secret_version.user_email.secret_data}"
}

# Grant Secret Manager access to Cloud Run service account
resource "google_project_iam_member" "cloud_run_secret_access" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${data.google_secret_manager_secret_version.service_account.secret_data}"
}

# #############################################
# #               Outputs                     #
# #############################################

# Display the service IP
output "service_ip" {
  value = var.use_load_balancer ? module.lb-http[0].external_ip : google_cloud_run_service.run_service.status[0].url
  description = "The URL to access the service. If load balancer is enabled, this is the load balancer IP, otherwise it's the Cloud Run URL."
}

