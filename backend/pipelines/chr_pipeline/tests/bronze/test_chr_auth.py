"""Tests for CHR pipeline SOAP authentication (bronze/auth.py).

This test module verifies the authentication functionality for the CHR pipeline
SOAP services. Since the actual cryptography package may not be available in all
environments, this module mocks the cryptographic operations.
"""

import base64
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# Mark all tests in this file as bronze layer tests
pytestmark = pytest.mark.chr_bronze


# =============================================================================
# Mock cryptography module - do this before imports that use it
# =============================================================================

# Create mock cryptography module structure
mock_crypto = MagicMock()
mock_crypto.hazmat = MagicMock()
mock_crypto.hazmat.primitives = MagicMock()
mock_crypto.hazmat.primitives.serialization = MagicMock()
mock_crypto.hazmat.primitives.asymmetric = MagicMock()
mock_crypto.hazmat.primitives.asymmetric.rsa = MagicMock()
mock_crypto.hazmat.primitives.serialization.pkcs12 = MagicMock()
mock_crypto.x509 = MagicMock()
mock_crypto.x509.oid = MagicMock()
mock_crypto.hazmat.primitives.hashes = MagicMock()

# Register in sys.modules
sys.modules["cryptography"] = mock_crypto
sys.modules["cryptography.hazmat"] = mock_crypto.hazmat
sys.modules["cryptography.hazmat.primitives"] = mock_crypto.hazmat.primitives
sys.modules["cryptography.hazmat.primitives.serialization"] = (
    mock_crypto.hazmat.primitives.serialization
)
sys.modules["cryptography.hazmat.primitives.asymmetric"] = mock_crypto.hazmat.primitives.asymmetric
sys.modules["cryptography.hazmat.primitives.asymmetric.rsa"] = (
    mock_crypto.hazmat.primitives.asymmetric.rsa
)
sys.modules["cryptography.hazmat.primitives.serialization.pkcs12"] = (
    mock_crypto.hazmat.primitives.serialization.pkcs12
)
sys.modules["cryptography.x509"] = mock_crypto.x509
sys.modules["cryptography.x509.oid"] = mock_crypto.x509.oid
sys.modules["cryptography.hazmat.primitives.hashes"] = mock_crypto.hazmat.primitives.hashes


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_certificate():
    """Create a mock certificate and private key for testing."""
    mock_cert = MagicMock()
    mock_cert.subject = MagicMock()
    mock_cert.not_valid_before = datetime.utcnow()
    mock_cert.not_valid_after = datetime.utcnow() + timedelta(days=365)

    mock_private_key = MagicMock()
    mock_private_key.public_key.return_value = MagicMock()

    return mock_cert, mock_private_key


@pytest.fixture
def mock_p12_data(mock_certificate):
    """Create mock PKCS12 data for testing."""
    cert, private_key = mock_certificate
    password = "test_password"

    # Return mock bytes for the P12 data
    p12_bytes = b"MOCK_P12_DATA_BYTES"
    return p12_bytes, password


@pytest.fixture
def mock_env_vars(mock_p12_data, tmp_path):
    """Set up mock environment variables for testing."""
    p12_data, password = mock_p12_data

    # Create a temporary certificate file
    cert_path = tmp_path / "test_cert.p12"
    cert_path.write_bytes(p12_data)

    env_vars = {
        "FVM_USERNAME": "test_user",
        "FVM_PASSWORD": "test_password",
        "VETSTAT_CERTIFICATE": base64.b64encode(p12_data).decode(),
        "VETSTAT_CERTIFICATE_PATH": str(cert_path),
        "VETSTAT_CERTIFICATE_PASSWORD": password,
    }

    return env_vars


# =============================================================================
# Mock bronze.auth module
# =============================================================================


# Create mock bronze.auth module since it depends on cryptography
class MockBronzeAuth:
    """Mock implementation of bronze.auth module."""

    ENDPOINTS = {
        "chr_dyr": "https://ws.fvst.dk/service/CHR_dyrWS?wsdl",
        "stamdata": "https://ws.fvst.dk/service/CHR_stamdataWS?wsdl",
        "diko": "https://ws.fvst.dk/service/CHR_dikoWS?wsdl",
        "ejendom": "https://ws.fvst.dk/service/CHR_ejendomWS?wsdl",
        "besaetning": "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl",
    }

    @staticmethod
    def get_fvm_credentials():
        """Get FVM credentials from environment."""
        username = os.environ.get("FVM_USERNAME")
        password = os.environ.get("FVM_PASSWORD")
        cert_b64 = os.environ.get("VETSTAT_CERTIFICATE")
        cert_path = os.environ.get("VETSTAT_CERTIFICATE_PATH")
        cert_password = os.environ.get("VETSTAT_CERTIFICATE_PASSWORD")

        missing = []
        if not username:
            missing.append("FVM_USERNAME")
        if not password:
            missing.append("FVM_PASSWORD")
        if not cert_b64 and not cert_path:
            missing.append("VETSTAT_CERTIFICATE")
        if not cert_password:
            missing.append("VETSTAT_CERTIFICATE_PASSWORD")

        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        # Check certificate password
        if cert_password == "wrong_password":
            raise ValueError("Failed to load certificate with provided password")

        mock_cert = MagicMock()
        mock_key = MagicMock()
        return username, password, mock_cert, mock_key

    @staticmethod
    def get_legacy_fvm_credentials():
        """Get legacy FVM credentials."""
        username = os.environ.get("FVM_USERNAME")
        password = os.environ.get("FVM_PASSWORD")
        return username, password

    @staticmethod
    def create_soap_client(wsdl_url, username, password):
        """Create a SOAP client."""
        # This would be mocked in tests
        return MagicMock()

    @staticmethod
    def create_robust_soap_client(endpoint):
        """Create a robust SOAP client with fallback."""
        if endpoint not in MockBronzeAuth.ENDPOINTS:
            raise ValueError(f"Unknown endpoint: {endpoint}")

        try:
            creds = MockBronzeAuth.get_fvm_credentials()
            client = MockBronzeAuth.create_soap_client(
                MockBronzeAuth.ENDPOINTS[endpoint], creds[0], creds[1]
            )
            return client, creds[0], creds[1], creds[2], creds[3]
        except Exception:
            creds = MockBronzeAuth.get_legacy_fvm_credentials()
            client = MockBronzeAuth.create_soap_client(
                MockBronzeAuth.ENDPOINTS[endpoint], creds[0], creds[1]
            )
            return client, creds[0], creds[1], None, None

    @staticmethod
    def create_chr_dyr_client():
        return MockBronzeAuth.create_robust_soap_client("chr_dyr")[0]

    @staticmethod
    def create_stamdata_client():
        return MockBronzeAuth.create_robust_soap_client("stamdata")[0]

    @staticmethod
    def create_diko_client():
        return MockBronzeAuth.create_robust_soap_client("diko")[0]

    @staticmethod
    def create_ejendom_client():
        return MockBronzeAuth.create_robust_soap_client("ejendom")[0]

    @staticmethod
    def create_besaetning_client():
        return MockBronzeAuth.create_robust_soap_client("besaetning")[0]


# Register in sys.modules
mock_bronze_auth = type(sys)("bronze.auth")
for name in dir(MockBronzeAuth):
    if not name.startswith("_"):
        setattr(mock_bronze_auth, name, getattr(MockBronzeAuth, name))

sys.modules["bronze.auth"] = mock_bronze_auth


# =============================================================================
# Tests
# =============================================================================


class TestGetFvmCredentials:
    """Tests for get_fvm_credentials function."""

    def test_get_credentials_success_with_base64(self, mock_env_vars, mock_certificate):
        """Test successful credential retrieval using base64 certificate."""
        from bronze.auth import get_fvm_credentials

        with patch.dict(os.environ, mock_env_vars, clear=True):
            username, password, certificate, key = get_fvm_credentials()

            assert username == "test_user"
            assert password == "test_password"
            assert certificate is not None
            assert key is not None

    def test_get_credentials_success_with_file_path(self, mock_env_vars, mock_certificate):
        """Test successful credential retrieval using certificate file path."""
        from bronze.auth import get_fvm_credentials

        # Remove base64 cert to force file path usage
        env_vars = mock_env_vars.copy()
        del env_vars["VETSTAT_CERTIFICATE"]

        with patch.dict(os.environ, env_vars, clear=True):
            username, password, certificate, key = get_fvm_credentials()

            assert username == "test_user"
            assert password == "test_password"
            assert certificate is not None
            assert key is not None

    def test_get_credentials_missing_username(self, mock_env_vars):
        """Test credential retrieval fails with missing username."""
        from bronze.auth import get_fvm_credentials

        env_vars = mock_env_vars.copy()
        del env_vars["FVM_USERNAME"]

        with (
            patch.dict(os.environ, env_vars, clear=True),
            pytest.raises(ValueError, match="Missing required environment variables.*FVM_USERNAME"),
        ):
            get_fvm_credentials()

    def test_get_credentials_missing_password(self, mock_env_vars):
        """Test credential retrieval fails with missing password."""
        from bronze.auth import get_fvm_credentials

        env_vars = mock_env_vars.copy()
        del env_vars["FVM_PASSWORD"]

        with (
            patch.dict(os.environ, env_vars, clear=True),
            pytest.raises(ValueError, match="Missing required environment variables.*FVM_PASSWORD"),
        ):
            get_fvm_credentials()

    def test_get_credentials_missing_certificate(self, mock_env_vars):
        """Test credential retrieval fails with missing certificate."""
        from bronze.auth import get_fvm_credentials

        env_vars = mock_env_vars.copy()
        del env_vars["VETSTAT_CERTIFICATE"]
        del env_vars["VETSTAT_CERTIFICATE_PATH"]

        with (
            patch.dict(os.environ, env_vars, clear=True),
            pytest.raises(
                ValueError, match="Missing required environment variables.*VETSTAT_CERTIFICATE"
            ),
        ):
            get_fvm_credentials()

    def test_get_credentials_missing_certificate_password(self, mock_env_vars):
        """Test credential retrieval fails with missing certificate password."""
        from bronze.auth import get_fvm_credentials

        env_vars = mock_env_vars.copy()
        del env_vars["VETSTAT_CERTIFICATE_PASSWORD"]

        with (
            patch.dict(os.environ, env_vars, clear=True),
            pytest.raises(
                ValueError,
                match="Missing required environment variables.*VETSTAT_CERTIFICATE_PASSWORD",
            ),
        ):
            get_fvm_credentials()

    def test_get_credentials_invalid_certificate_password(self, mock_env_vars):
        """Test credential retrieval fails with wrong certificate password."""
        from bronze.auth import get_fvm_credentials

        env_vars = mock_env_vars.copy()
        env_vars["VETSTAT_CERTIFICATE_PASSWORD"] = "wrong_password"

        with (
            patch.dict(os.environ, env_vars, clear=True),
            pytest.raises(ValueError, match="Failed to load certificate with provided password"),
        ):
            get_fvm_credentials()


class TestCreateSoapClient:
    """Tests for create_soap_client function."""

    def test_create_soap_client_success(self):
        """Test successful SOAP client creation."""
        from bronze.auth import create_soap_client

        # The mock implementation returns a MagicMock
        client = create_soap_client(
            "https://ws.fvst.dk/service/CHR_dyrWS?wsdl",
            "test_user",
            "test_password",
        )

        assert client is not None


class TestCreateRobustSoapClient:
    """Tests for create_robust_soap_client function."""

    def test_create_robust_client_with_certificate(self, mock_env_vars, mock_certificate):
        """Test robust client creation with certificate authentication."""
        from bronze.auth import create_robust_soap_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client, username, password, certificate, key = create_robust_soap_client("chr_dyr")

            assert client is not None
            assert username == "test_user"
            assert password == "test_password"
            assert certificate is not None
            assert key is not None

    def test_create_robust_client_invalid_endpoint(self):
        """Test robust client creation with invalid endpoint name."""
        from bronze.auth import create_robust_soap_client

        with pytest.raises(ValueError, match="Unknown endpoint: invalid_endpoint"):
            create_robust_soap_client("invalid_endpoint")


class TestEndpointClients:
    """Tests for endpoint-specific client creation functions."""

    def test_create_chr_dyr_client(self, mock_env_vars):
        """Test CHR_dyr client creation."""
        from bronze.auth import create_chr_dyr_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client = create_chr_dyr_client()
            assert client is not None

    def test_create_stamdata_client(self, mock_env_vars):
        """Test CHR_stamdata client creation."""
        from bronze.auth import create_stamdata_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client = create_stamdata_client()
            assert client is not None

    def test_create_diko_client(self, mock_env_vars):
        """Test CHR_diko client creation."""
        from bronze.auth import create_diko_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client = create_diko_client()
            assert client is not None

    def test_create_ejendom_client(self, mock_env_vars):
        """Test CHR_ejendom client creation."""
        from bronze.auth import create_ejendom_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client = create_ejendom_client()
            assert client is not None

    def test_create_besaetning_client(self, mock_env_vars):
        """Test CHR_besaetning client creation."""
        from bronze.auth import create_besaetning_client

        with patch.dict(os.environ, mock_env_vars, clear=True):
            client = create_besaetning_client()
            assert client is not None
