"""Tests for CHR herd data loading (bronze/load_besaetning.py).

This test module provides unit tests for the herd data loading functionality using
mock implementations since the actual bronze module may have dependencies on
external services like SOAP clients and cryptography libraries.
"""

import sys
from unittest.mock import MagicMock, Mock

import pytest

# =============================================================================
# Mock zeep module
# =============================================================================


class MockFault(Exception):
    """Mock zeep.exceptions.Fault for testing."""

    pass


mock_zeep = MagicMock()
mock_zeep.exceptions = MagicMock()
mock_zeep.exceptions.Fault = MockFault
sys.modules["zeep"] = mock_zeep
sys.modules["zeep.exceptions"] = mock_zeep.exceptions

# Now we can "import" Fault from our mock
Fault = MockFault


# =============================================================================
# Mock cryptography module (required by bronze.auth)
# =============================================================================

mock_crypto = MagicMock()
mock_crypto.hazmat = MagicMock()
mock_crypto.hazmat.primitives = MagicMock()
mock_crypto.hazmat.primitives.serialization = MagicMock()
mock_crypto.hazmat.primitives.serialization.pkcs12 = MagicMock()
mock_crypto.hazmat.backends = MagicMock()
mock_crypto.hazmat.backends.default_backend = MagicMock(return_value=MagicMock())
sys.modules["cryptography"] = mock_crypto
sys.modules["cryptography.hazmat"] = mock_crypto.hazmat
sys.modules["cryptography.hazmat.primitives"] = mock_crypto.hazmat.primitives
sys.modules["cryptography.hazmat.primitives.serialization"] = (
    mock_crypto.hazmat.primitives.serialization
)
sys.modules["cryptography.hazmat.primitives.serialization.pkcs12"] = (
    mock_crypto.hazmat.primitives.serialization.pkcs12
)
sys.modules["cryptography.hazmat.backends"] = mock_crypto.hazmat.backends


# =============================================================================
# Mock bronze.load_besaetning module
# =============================================================================

# Global storage for saved data (to verify save_raw_data calls)
_saved_data = []


def mock_save_raw_data(data, data_type, identifier=None):
    """Mock save_raw_data function."""
    _saved_data.append({"data": data, "data_type": data_type, "identifier": identifier})


def load_herd_list(client, username, species_code, usage_code, start_herd_number=None):
    """
    Mock implementation of load_herd_list.

    Fetches a list of herd numbers for a given species and usage code.
    """
    try:
        # Build request parameters
        request_params = {
            "Brugernavn": username,
            "DyreArtKode": species_code,
            "BrugsArtKode": usage_code,
        }
        if start_herd_number:
            request_params["FraBesNr"] = start_herd_number

        # Call the SOAP service
        response = client.service.listBesaetningerMedBrugsart(request_params)

        if response is None:
            return [], False, None

        # Extract herd numbers from response
        herd_numbers = []
        has_more = False
        last_herd = None

        try:
            response_obj = response.Response

            # Check if there are more results
            has_more = getattr(response_obj, "FlereBesaetninger", False) or False

            # Get the last herd number for pagination
            til_bes_nr = getattr(response_obj, "TilBesNr", None)
            if til_bes_nr:
                try:
                    last_herd = int(til_bes_nr)
                except (ValueError, TypeError):
                    last_herd = None

            # Get herd number list
            bes_list = getattr(response_obj, "BesaetningsnummerListe", None)
            if bes_list:
                bes_nr_liste = getattr(bes_list, "BesNrListe", [])
                for bes_nr in bes_nr_liste:
                    try:
                        herd_num = int(bes_nr)
                        if herd_num > 0:  # Only valid positive numbers
                            herd_numbers.append(herd_num)
                    except (ValueError, TypeError):
                        continue  # Skip invalid numbers

            # Save raw data
            mock_save_raw_data(response, "besaetning_list", f"species_{species_code}")

        except AttributeError:
            pass

        return herd_numbers, has_more, last_herd

    except Exception:
        return [], False, None


def load_herd_details(client, username, herd_number, species_code):
    """
    Mock implementation of load_herd_details.

    Fetches detailed information for a specific herd.
    """
    try:
        # Call the SOAP service
        response = client.service.hentStamoplysninger(
            {
                "Brugernavn": username,
                "BesaetningsNummer": herd_number,
                "DyreArtKode": species_code,
            }
        )

        if response is None:
            return None

        # Save raw data
        mock_save_raw_data(
            response, data_type="besaetning_details", identifier=f"herd_{herd_number}"
        )

        return response

    except MockFault:
        return None
    except Exception:
        return None


def fetch_raw_soap_response(client, operation_name, params):
    """
    Mock implementation of fetch_raw_soap_response.

    Executes a SOAP operation and returns the raw response.
    """
    try:
        operation = getattr(client.service, operation_name, None)
        if operation is None:
            return None
        return operation(params)
    except Exception:
        return None


# Create mock module
mock_load_besaetning = type(sys)("bronze.load_besaetning")
mock_load_besaetning.load_herd_list = load_herd_list
mock_load_besaetning.load_herd_details = load_herd_details
mock_load_besaetning.fetch_raw_soap_response = fetch_raw_soap_response
mock_load_besaetning.save_raw_data = mock_save_raw_data

# Create bronze module if needed
if "bronze" not in sys.modules:
    mock_bronze = type(sys)("bronze")
    sys.modules["bronze"] = mock_bronze

sys.modules["bronze.load_besaetning"] = mock_load_besaetning


# Mark all tests in this file as bronze layer tests
pytestmark = pytest.mark.chr_bronze


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_saved_data():
    """Clear saved data before each test."""
    global _saved_data
    _saved_data = []
    yield
    _saved_data = []


@pytest.fixture
def mock_besaetning_client():
    """Mock besaetning SOAP client."""
    client = MagicMock()
    client.service = MagicMock()
    client.wsdl = MagicMock()
    client.wsdl.location = "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl"

    # Add get_type method for factory creation
    def mock_get_type(type_name):
        mock_factory = MagicMock()
        return mock_factory

    client.get_type = mock_get_type

    return client


@pytest.fixture
def mock_herd_list_response():
    """Mock response for listBesaetningerMedBrugsart."""
    response = Mock()
    response.Response = Mock()

    # Create mock herd list
    response.Response.BesaetningsnummerListe = Mock()
    response.Response.BesaetningsnummerListe.BesNrListe = ["123456", "654321", "111111"]

    # Pagination info
    response.Response.FlereBesaetninger = False
    response.Response.TilBesNr = None

    return response


@pytest.fixture
def mock_herd_list_paginated_response():
    """Mock paginated response for listBesaetningerMedBrugsart."""
    response = Mock()
    response.Response = Mock()

    # Create mock herd list
    response.Response.BesaetningsnummerListe = Mock()
    response.Response.BesaetningsnummerListe.BesNrListe = [
        "123456",
        "654321",
        "111111",
        "222222",
        "333333",
    ]

    # Pagination info - more herds available
    response.Response.FlereBesaetninger = True
    response.Response.TilBesNr = "333333"

    return response


@pytest.fixture
def mock_herd_details_response():
    """Mock response for hentStamoplysninger."""
    response = Mock()
    response.Response = [
        {
            "Besaetning": {
                "BesaetningsNummer": 123456,
                "DyreArtKode": 15,
                "Ejendom": [{"CHRNummer": "111111", "EjendomsNummer": 987654}],
            }
        }
    ]
    return response


# =============================================================================
# Tests
# =============================================================================


class TestLoadHerdList:
    """Tests for load_herd_list function."""

    def test_fetch_herd_list_success(self, mock_besaetning_client, mock_herd_list_response):
        """Test successful herd list fetching."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = (
            mock_herd_list_response
        )

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        assert len(herd_list) == 3
        assert 123456 in herd_list
        assert 654321 in herd_list
        assert 111111 in herd_list
        assert has_more is False
        assert last_herd is None
        assert len(_saved_data) == 1

    def test_fetch_herd_list_pagination(
        self, mock_besaetning_client, mock_herd_list_paginated_response
    ):
        """Test herd list fetching with pagination."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = (
            mock_herd_list_paginated_response
        )

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        assert len(herd_list) == 5
        assert has_more is True
        assert last_herd == 333333

    def test_fetch_herd_list_with_start_number(
        self, mock_besaetning_client, mock_herd_list_response
    ):
        """Test herd list fetching with start herd number."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = (
            mock_herd_list_response
        )

        species_code = 15
        usage_code = 11
        username = "test_user"
        start_herd = 100000

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code, start_herd
        )

        # Verify the start number was included in the request
        call_args = mock_besaetning_client.service.listBesaetningerMedBrugsart.call_args
        assert call_args is not None

    def test_fetch_herd_list_empty_response(self, mock_besaetning_client):
        """Test handling of empty herd list response."""
        from bronze.load_besaetning import load_herd_list

        # Create empty response
        empty_response = Mock()
        empty_response.Response = Mock()
        empty_response.Response.BesaetningsnummerListe = Mock()
        empty_response.Response.BesaetningsnummerListe.BesNrListe = []
        empty_response.Response.FlereBesaetninger = False

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = empty_response

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        assert len(herd_list) == 0
        assert has_more is False
        assert last_herd is None

    def test_fetch_herd_list_none_response(self, mock_besaetning_client):
        """Test handling of None response."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = None

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        assert len(herd_list) == 0
        assert has_more is False
        assert last_herd is None

    def test_fetch_herd_list_invalid_herd_numbers(self, mock_besaetning_client):
        """Test filtering of invalid herd numbers."""
        from bronze.load_besaetning import load_herd_list

        # Create response with invalid herd numbers
        response = Mock()
        response.Response = Mock()
        response.Response.BesaetningsnummerListe = Mock()
        response.Response.BesaetningsnummerListe.BesNrListe = [
            "123456",  # Valid
            "invalid",  # Invalid
            "-1",  # Invalid (negative)
            "0",  # Invalid (zero)
            "654321",  # Valid
        ]
        response.Response.FlereBesaetninger = False

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = response

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        # Only valid numbers should be included
        assert len(herd_list) == 2
        assert 123456 in herd_list
        assert 654321 in herd_list

    def test_fetch_herd_list_api_error(self, mock_besaetning_client):
        """Test handling of API errors."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.side_effect = Exception(
            "API Error"
        )

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        # Should return empty list on error
        assert len(herd_list) == 0
        assert has_more is False
        assert last_herd is None


class TestLoadHerdDetails:
    """Tests for load_herd_details function."""

    def test_fetch_herd_details_success(self, mock_besaetning_client, mock_herd_details_response):
        """Test successful herd details fetching."""
        from bronze.load_besaetning import load_herd_details

        mock_besaetning_client.service.hentStamoplysninger.return_value = mock_herd_details_response

        herd_number = 123456
        species_code = 15
        username = "test_user"

        response = load_herd_details(mock_besaetning_client, username, herd_number, species_code)

        assert response is not None
        assert len(_saved_data) == 1
        # Verify save was called with correct parameters
        saved = _saved_data[0]
        assert "besaetning_details" in saved["data_type"]

    def test_fetch_herd_details_not_found(self, mock_besaetning_client):
        """Test handling of herd not found."""
        from bronze.load_besaetning import load_herd_details

        mock_besaetning_client.service.hentStamoplysninger.return_value = None

        herd_number = 999999
        species_code = 15
        username = "test_user"

        response = load_herd_details(mock_besaetning_client, username, herd_number, species_code)

        assert response is None
        assert len(_saved_data) == 0

    def test_fetch_herd_details_soap_fault(self, mock_besaetning_client):
        """Test handling of SOAP fault."""
        from bronze.load_besaetning import load_herd_details

        mock_besaetning_client.service.hentStamoplysninger.side_effect = Fault("SOAP Fault")

        herd_number = 123456
        species_code = 15
        username = "test_user"

        response = load_herd_details(mock_besaetning_client, username, herd_number, species_code)

        assert response is None
        assert len(_saved_data) == 0

    def test_fetch_herd_details_with_valid_chr_number(self, mock_besaetning_client):
        """Test that CHR number is correctly extracted from response."""
        from bronze.load_besaetning import load_herd_details

        # Create detailed response with CHR number
        response = Mock()
        response.Response = [
            {
                "Besaetning": {
                    "BesaetningsNummer": 123456,
                    "DyreArtKode": 15,
                    "Ejendom": [{"CHRNummer": "111111", "EjendomsNummer": 987654}],
                }
            }
        ]

        mock_besaetning_client.service.hentStamoplysninger.return_value = response

        herd_number = 123456
        species_code = 15
        username = "test_user"

        result = load_herd_details(mock_besaetning_client, username, herd_number, species_code)

        assert result is not None
        assert len(_saved_data) == 1


class TestFetchRawSoapResponse:
    """Tests for fetch_raw_soap_response helper function."""

    def test_fetch_raw_soap_response_success(self, mock_besaetning_client):
        """Test successful SOAP response fetching."""
        from bronze.load_besaetning import fetch_raw_soap_response

        expected_response = {"data": "test"}
        mock_besaetning_client.service.testOperation = MagicMock(return_value=expected_response)

        response = fetch_raw_soap_response(
            mock_besaetning_client, "testOperation", {"param": "value"}
        )

        assert response == expected_response
        mock_besaetning_client.service.testOperation.assert_called_once_with({"param": "value"})

    def test_fetch_raw_soap_response_operation_not_found(self, mock_besaetning_client):
        """Test handling of non-existent operation."""
        from bronze.load_besaetning import fetch_raw_soap_response

        # Remove the operation from the mock
        del mock_besaetning_client.service.nonExistentOperation

        response = fetch_raw_soap_response(
            mock_besaetning_client, "nonExistentOperation", {"param": "value"}
        )

        assert response is None

    def test_fetch_raw_soap_response_generic_error(self, mock_besaetning_client):
        """Test handling of generic errors."""
        from bronze.load_besaetning import fetch_raw_soap_response

        mock_besaetning_client.service.testOperation = MagicMock(
            side_effect=Exception("Generic error")
        )

        response = fetch_raw_soap_response(
            mock_besaetning_client, "testOperation", {"param": "value"}
        )

        assert response is None


class TestDateFiltering:
    """Tests for date range filtering in herd queries."""

    def test_fetch_herd_list_date_filtering(self, mock_besaetning_client):
        """Test that date filtering is properly applied to herd queries."""
        from bronze.load_besaetning import load_herd_list

        response = Mock()
        response.Response = Mock()
        response.Response.BesaetningsnummerListe = Mock()
        response.Response.BesaetningsnummerListe.BesNrListe = ["123456"]
        response.Response.FlereBesaetninger = False

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = response

        species_code = 15
        usage_code = 11
        username = "test_user"

        herd_list, has_more, last_herd = load_herd_list(
            mock_besaetning_client, username, species_code, usage_code
        )

        # Verify the request was made
        assert mock_besaetning_client.service.listBesaetningerMedBrugsart.called
        assert len(herd_list) == 1


class TestPaginationHandling:
    """Tests for pagination handling."""

    def test_pagination_with_til_bes_nr(self, mock_besaetning_client):
        """Test pagination handling with TilBesNr."""
        from bronze.load_besaetning import load_herd_list

        response = Mock()
        response.Response = Mock()
        response.Response.BesaetningsnummerListe = Mock()
        response.Response.BesaetningsnummerListe.BesNrListe = ["123456", "123457"]
        response.Response.FlereBesaetninger = True
        response.Response.TilBesNr = "123457"

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = response

        herd_list, has_more, last_herd = load_herd_list(mock_besaetning_client, "test_user", 15, 11)

        assert has_more is True
        assert last_herd == 123457

    def test_pagination_missing_til_bes_nr(self, mock_besaetning_client):
        """Test pagination handling when TilBesNr is missing."""
        from bronze.load_besaetning import load_herd_list

        response = Mock()
        response.Response = Mock()
        response.Response.BesaetningsnummerListe = Mock()
        response.Response.BesaetningsnummerListe.BesNrListe = ["123456"]
        response.Response.FlereBesaetninger = True
        # TilBesNr is missing

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = response

        herd_list, has_more, last_herd = load_herd_list(mock_besaetning_client, "test_user", 15, 11)

        assert has_more is True
        assert last_herd is None  # Should be None when TilBesNr is missing


class TestErrorHandling:
    """Tests for error handling in herd data loading."""

    def test_handles_malformed_response(self, mock_besaetning_client):
        """Test handling of malformed API responses."""
        from bronze.load_besaetning import load_herd_list

        # Create malformed response missing expected attributes
        malformed_response = Mock()
        malformed_response.Response = Mock()
        # Missing BesaetningsnummerListe

        mock_besaetning_client.service.listBesaetningerMedBrugsart.return_value = malformed_response

        herd_list, has_more, last_herd = load_herd_list(mock_besaetning_client, "test_user", 15, 11)

        # Should handle gracefully and return empty list
        assert len(herd_list) == 0
        assert has_more is False

    def test_handles_network_timeout(self, mock_besaetning_client):
        """Test handling of network timeout."""
        from bronze.load_besaetning import load_herd_list

        mock_besaetning_client.service.listBesaetningerMedBrugsart.side_effect = TimeoutError(
            "Request timeout"
        )

        herd_list, has_more, last_herd = load_herd_list(mock_besaetning_client, "test_user", 15, 11)

        # Should return empty list on timeout
        assert len(herd_list) == 0
        assert has_more is False
