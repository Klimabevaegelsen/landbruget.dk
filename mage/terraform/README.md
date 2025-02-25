# Mage AI on Google Cloud Platform

This directory contains Terraform configurations to deploy Mage AI on Google Cloud Platform.

## Architecture

The deployment includes:
- Cloud Run service running Mage AI
- Cloud SQL PostgreSQL database
- Filestore for persistent storage
- Load balancer with optional SSL
- Secret Manager for sensitive information
- VPC connector for private networking

## Prerequisites

- Google Cloud Platform account
- Terraform 0.14+
- Google Cloud SDK
- Required Google Cloud APIs enabled:
  - Cloud Run
  - Cloud SQL
  - Filestore
  - Secret Manager
  - VPC Access
  - IAM
  - Resource Manager
  - Artifact Registry

## Setup

### 1. Required Secrets

The following secrets must exist in Google Secret Manager:

- `mage-database-user` - Username for the PostgreSQL database
- `mage-database-password` - Password for the PostgreSQL database
- `mage-user-email` - Email address for granting Cloud Run access
- `mage-service-account` - Service account for accessing Secret Manager
- `mage-oauth-client-id` - OAuth client ID for Mage AI authentication
- `mage-oauth-client-secret` - OAuth client secret for Mage AI authentication
- `git-username` - Git username for Mage AI Git integration
- `git-email` - Git email for Mage AI Git integration
- `git-auth-token` - Git authentication token for Mage AI Git integration

### 2. Configure Terraform Variables

Copy the example variables file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` to set your project-specific values.

### 3. Initialize Terraform

```bash
terraform init
```

### 4. Deploy

```bash
terraform plan
terraform apply
```

## Variables

See `variables.tf` for a complete list of configurable variables.

## Outputs

- `service_ip`: The IP address of the load balancer

## Maintenance

### Updating Mage AI

To update the Mage AI version, change the `docker_image` variable in `terraform.tfvars` and run `terraform apply`.

### Scaling

Adjust the `container_cpu` and `container_memory` variables to scale the Cloud Run service.

## Security Considerations

- All sensitive information is stored in Secret Manager
- The database is only accessible from the Cloud Run service
- The Filestore instance is only accessible from the Cloud Run service
- The load balancer can be configured with SSL for secure communication 

## Local Development

For local development and making changes to Mage.ai content, please refer to the [CONTRIBUTING.md](../../backend/mage/CONTRIBUTING.md) file in the `backend/mage` directory.

### Development Workflow

1. Developers make changes to Mage.ai content locally using Docker
2. Changes are committed to a feature branch
3. A pull request is created for review
4. Once approved, changes are merged to the main branch
5. The Terraform configuration in this directory is used to deploy the changes to the production environment

This workflow ensures that all changes to Mage.ai content are properly reviewed and tested before being deployed to production. 