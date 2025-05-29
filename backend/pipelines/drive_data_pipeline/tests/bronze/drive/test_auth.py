"""Tests for Google Drive authentication."""

import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from drive_data_pipeline.bronze.drive.auth import get_drive_service
from googleapiclient.discovery import Resource


@pytest.fixture
def mock_credentials():
    """Create a temporary mock credentials file."""
    content = """
    {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC7VJTUt9Us8cKj\\nMzEfYyjiWA4R4/M2bS1GB4t7NXp98C3SC6dVMvDuictGeurT8jNbvJZHtCSuYEvu\\nNMoSfm76oqFvAp8Gy0iz5sxjZmSnXyCdPEovGhLa0VzMaQ8s+CLOyS56YyCFGeJZ\\n-----END PRIVATE KEY-----\\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com"
    }
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(content)
        temp_path = f.name
    
    yield Path(temp_path)
    
    # Clean up
    os.unlink(temp_path)


@pytest.fixture
def mock_build():
    """Mock for googleapiclient.discovery.build."""
    with mock.patch("drive_data_pipeline.bronze.drive.auth.build") as mock_build:
        mock_drive_service = mock.MagicMock(spec=Resource)
        mock_build.return_value = mock_drive_service
        yield mock_build


@pytest.fixture
def mock_service_account():
    """Mock for service_account.Credentials."""
    with mock.patch("drive_data_pipeline.bronze.drive.auth.service_account.Credentials") as mock_creds:
        mock_creds.from_service_account_file.return_value = mock.MagicMock()
        yield mock_creds


def test_get_drive_service_with_valid_credentials(mock_credentials, mock_build, mock_service_account):
    """Test get_drive_service with valid credentials."""
    # Call the function
    result = get_drive_service(mock_credentials)
    
    # Verify service_account.Credentials.from_service_account_file was called
    mock_service_account.from_service_account_file.assert_called_once_with(
        str(mock_credentials),
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    
    # Verify build was called
    mock_build.assert_called_once()
    
    # Verify the result
    assert result is mock_build.return_value


def test_get_drive_service_with_nonexistent_file():
    """Test get_drive_service with a nonexistent credentials file."""
    with pytest.raises(FileNotFoundError):
        get_drive_service(Path("/nonexistent/path.json"))


def test_get_drive_service_with_invalid_credentials(mock_credentials, mock_service_account):
    """Test get_drive_service with invalid credentials."""
    # Mock service_account.Credentials.from_service_account_file to raise an exception
    mock_service_account.from_service_account_file.side_effect = ValueError("Invalid credentials")
    
    # Call the function and verify it raises a ValueError
    with pytest.raises(ValueError):
        get_drive_service(mock_credentials) 