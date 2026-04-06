"""
Bootstrap secrets from Google Cloud Secret Manager.

Call init_secrets() once at pipeline startup, after load_dotenv().
Local .env values always take priority — Secret Manager only fills gaps.
Activated when GCP_PROJECT env var is set.
"""

import logging
import os

logger = logging.getLogger("landbruget.secrets")

_initialized = False

# Mapping: env var name -> GCP Secret Manager secret name
# (only needed when they differ; same-name secrets are looked up directly)
_ENV_TO_SECRET = {
    "FVM_USERNAME": "fvm_username",
    "FVM_PASSWORD": "fvm_password",  # pragma: allowlist secret
    "VETSTAT_CERTIFICATE_PASSWORD": "vetstat-certificate-password",  # pragma: allowlist secret
    "DATAFORDELER_SFTP_HOST": "datafordeler-sftp-host",
    "DATAFORDELER_SFTP_USERNAME": "datafordeler-sftp-username",
    "DATAFORDELER_GRAPHQL_API_KEY": "datafordeler-graphql-api-key",  # pragma: allowlist secret
    "SSH_KEY_BRUGERDATAFORDELEREN": "ssh-brugerdatafordeleren",
    "R2_ACCOUNT_ID": "r2-account-id",
    "R2_ACCESS_KEY_ID": "r2-access-key-id",
    "R2_SECRET_ACCESS_KEY": "r2-secret-access-key",  # pragma: allowlist secret
    "R2_ENDPOINT": "r2-endpoint",
    "R2_BUCKET": "r2-bucket",
    "STORAGE_BUCKET": "r2-bucket",
    "CLOUDFLARE_API_TOKEN": "cloudflare-api-token",
    "DB_HOST": "db-host",
    "DB_NAME": "db-name",
    "DB_USER": "db-user",
    "DB_PASSWORD": "db-password",  # pragma: allowlist secret
}


def init_secrets(project: str | None = None) -> int:
    """
    Load missing env vars from Google Cloud Secret Manager.

    Args:
        project: GCP project ID. Defaults to GCP_PROJECT env var.

    Returns:
        Number of secrets loaded.
    """
    global _initialized
    if _initialized:
        return 0

    project = project or os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.debug("No GCP_PROJECT set — skipping Secret Manager bootstrap")
        return 0

    # Only import when needed so local dev without google-cloud works fine
    try:
        from google.cloud import secretmanager
    except ImportError:
        logger.debug("google-cloud-secret-manager not installed — skipping")
        return 0

    client = secretmanager.SecretManagerServiceClient()
    loaded = 0

    for env_key, secret_name in _ENV_TO_SECRET.items():
        if env_key in os.environ:
            continue

        try:
            name = f"projects/{project}/secrets/{secret_name}/versions/latest"
            resp = client.access_secret_version(request={"name": name})
            os.environ[env_key] = resp.payload.data.decode("UTF-8")
            loaded += 1
            logger.debug("Loaded %s from Secret Manager", env_key)
        except Exception:
            logger.debug("Secret %s not found in Secret Manager", secret_name)

    _initialized = True

    if loaded:
        logger.info("Loaded %d secrets from Secret Manager (project: %s)", loaded, project)

    return loaded
