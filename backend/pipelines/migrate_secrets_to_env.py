#!/usr/bin/env python3
"""
Script to sync secrets between Google Secret Manager and GitHub Actions.
Requires:
- google-cloud-secret-manager
- PyGithub
- python-dotenv
"""

import logging
import os
from typing import Dict, Optional

from dotenv import load_dotenv
from github import Github
from github.Repository import Repository
from google.cloud import secretmanager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_google_secrets(project_id: str) -> Dict[str, str]:
    """Get all secrets from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secrets = {}

    try:
        # List all secrets in the project
        parent = f"projects/{project_id}"
        for secret in client.list_secrets(request={"parent": parent}):
            secret_name = secret.name.split("/")[-1]

            # Get the latest version of each secret
            version_name = f"{secret.name}/versions/latest"
            version = client.access_secret_version(request={"name": version_name})

            # Decode the secret value
            secret_value = version.payload.data.decode("UTF-8")
            secrets[secret_name] = secret_value

        logger.info(f"Retrieved {len(secrets)} secrets from Google Secret Manager")
        return secrets

    except Exception as e:
        logger.error(f"Error getting secrets from Google Secret Manager: {e}")
        raise


def get_github_repo(token: str, repo_name: str) -> Optional[Repository]:
    """Get GitHub repository object."""
    try:
        g = Github(token)
        return g.get_repo(repo_name)
    except Exception as e:
        logger.error(f"Error connecting to GitHub: {e}")
        raise


def update_github_secrets(repo: Repository, secrets: Dict[str, str]) -> None:
    """Update GitHub repository secrets."""
    try:
        # Get existing secrets first
        existing_secrets = {secret.name for secret in repo.get_secrets()}

        # Update each secret
        for name, value in secrets.items():
            # Convert secret name to GitHub format (uppercase with underscores)
            github_name = name.upper().replace("-", "_")

            try:
                if github_name in existing_secrets:
                    logger.info(f"Updating existing secret: {github_name}")
                    repo.create_secret(github_name, value)
                else:
                    logger.info(f"Creating new secret: {github_name}")
                    repo.create_secret(github_name, value)
            except Exception as e:
                logger.error(f"Error updating secret {github_name}: {e}")
                continue

        logger.info("Finished updating GitHub secrets")

    except Exception as e:
        logger.error(f"Error updating GitHub secrets: {e}")
        raise


def main():
    """Main function to sync secrets."""
    load_dotenv()

    # Get required environment variables
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    github_token = os.getenv("GITHUB_TOKEN")
    github_repo = os.getenv("GITHUB_REPOSITORY")  # Should be in format "owner/repo"

    if not all([project_id, github_token, github_repo]):
        logger.error("Missing required environment variables")
        logger.error("Required: GOOGLE_CLOUD_PROJECT, GITHUB_TOKEN, GITHUB_REPOSITORY")
        return

    try:
        # Get secrets from Google Secret Manager
        google_secrets = get_google_secrets(project_id)

        # Get GitHub repository
        repo = get_github_repo(github_token, github_repo)

        # Update GitHub secrets
        update_github_secrets(repo, google_secrets)

        logger.info("Secret sync completed successfully")

    except Exception as e:
        logger.error(f"Error during secret sync: {e}")
        return


if __name__ == "__main__":
    main()
