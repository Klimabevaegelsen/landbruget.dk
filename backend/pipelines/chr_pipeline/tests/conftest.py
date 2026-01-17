"""CHR Pipeline-specific pytest fixtures.

This module provides fixtures specific to CHR (Centrale Husdyrbrugsregister)
pipeline testing, including SOAP client mocks, sample API responses, and
CHR-specific test data.
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest

# Add chr_pipeline directory to sys.path to allow imports like:
#   from silver.X import Y
#   from bronze.X import Y
chr_pipeline_dir = Path(__file__).resolve().parent.parent
if str(chr_pipeline_dir) not in sys.path:
    sys.path.insert(0, str(chr_pipeline_dir))

# Optional zeep import - only needed for type hints
try:
    from zeep import Client
    from zeep.wsdl.definitions import Binding, Operation, Service

    ZEEP_AVAILABLE = True
except ImportError:
    ZEEP_AVAILABLE = False
    Client = None


@pytest.fixture(scope="function")
def mock_soap_client() -> MagicMock:
    """Mock SOAP client for CHR API.

    Provides a mock zeep Client that simulates SOAP API interactions
    without making actual network calls. Configured to work with CHR
    service endpoints.

    Scope: function - Each test gets its own mock to avoid interference.

    Returns:
        MagicMock configured to simulate zeep.Client behavior.
    """
    # Create mock with or without spec depending on zeep availability
    if ZEEP_AVAILABLE:
        mock_client = MagicMock(spec=Client)
    else:
        mock_client = MagicMock()

    # Mock the service operations
    mock_service = MagicMock()
    mock_client.service = mock_service

    # Common CHR operations (based on actual WSDL)
    mock_service.hentDyrListe = MagicMock()
    mock_service.hentBesaetning = MagicMock()
    mock_service.hentEjendom = MagicMock()
    mock_service.hentStamdata = MagicMock()
    mock_service.hentDiko = MagicMock()

    # Mock wsdl operations for introspection
    mock_client.wsdl = MagicMock()
    mock_client.wsdl.services = {"CHRService": mock_service}

    return mock_client


@pytest.fixture(scope="session")
def mock_chr_responses() -> dict[str, Any]:
    """Sample CHR API responses for different endpoints.

    Provides realistic sample responses from various CHR SOAP services
    for use in testing without making actual API calls.

    Scope: session - Response templates are immutable and can be shared.

    Returns:
        Dictionary mapping endpoint names to sample response data.
    """
    return {
        "hentDyrListe": {
            "success": {
                "animalList": [
                    {
                        "animalId": "DK123456789",
                        "species": "CATTLE",
                        "birthDate": "2023-01-15",
                        "gender": "FEMALE",
                        "herdNumber": "123456",
                        "currentLocation": "123456",
                    },
                    {
                        "animalId": "DK987654321",
                        "species": "CATTLE",
                        "birthDate": "2023-03-22",
                        "gender": "MALE",
                        "herdNumber": "123456",
                        "currentLocation": "123456",
                    },
                ],
                "totalCount": 2,
            },
            "empty": {"animalList": [], "totalCount": 0},
            "error": None,  # Simulates SOAP fault
        },
        "hentBesaetning": {
            "success": {
                "herdNumber": "123456",
                "cvrNumber": "12345678",
                "herdType": "CATTLE",
                "address": {
                    "street": "Landevej 123",
                    "postalCode": "8000",
                    "city": "Aarhus C",
                },
                "coordinates": {"latitude": 56.1629, "longitude": 10.2039},
                "registrationDate": "2020-01-01",
                "status": "ACTIVE",
            },
            "not_found": None,
        },
        "hentEjendom": {
            "success": {
                "propertyId": "123456",
                "cvrNumber": "12345678",
                "address": {
                    "street": "Landevej 123",
                    "postalCode": "8000",
                    "city": "Aarhus C",
                },
                "bfeNumbers": ["0730-123456-1", "0730-123456-2"],
                "totalArea": 125.5,
            },
            "multiple_properties": [
                {
                    "propertyId": "123456",
                    "cvrNumber": "12345678",
                    "totalArea": 125.5,
                },
                {
                    "propertyId": "123457",
                    "cvrNumber": "12345678",
                    "totalArea": 78.3,
                },
            ],
        },
        "hentStamdata": {
            "success": {
                "animalId": "DK123456789",
                "species": "CATTLE",
                "breed": "HOLSTEIN",
                "birthDate": "2023-01-15",
                "gender": "FEMALE",
                "motherId": "DK111111111",
                "fatherId": "DK222222222",
                "colorCode": "01",
                "earTagNumber": "DK123456789",
            }
        },
        "hentDiko": {
            "success": {
                "animalId": "DK123456789",
                "movements": [
                    {
                        "movementDate": "2024-03-15",
                        "fromHerd": "123456",
                        "toHerd": "654321",
                        "movementType": "SALE",
                        "transportDocument": "TD123456",
                    },
                    {
                        "movementDate": "2024-01-10",
                        "fromHerd": "111111",
                        "toHerd": "123456",
                        "movementType": "PURCHASE",
                        "transportDocument": "TD789012",
                    },
                ],
            }
        },
        "vetstat": {
            "success": """<?xml version="1.0" encoding="UTF-8"?>
<VetStatData>
    <Visit>
        <VisitDate>2024-03-15</VisitDate>
        <VetPractice>VetKlinik Aarhus</VetPractice>
        <HerdNumber>123456</HerdNumber>
        <Diagnosis>
            <Code>J.01</Code>
            <Description>Respiratory infection</Description>
        </Diagnosis>
        <Treatment>
            <Medicine>Penicillin</Medicine>
            <Dosage>5ml</Dosage>
            <AnimalCount>3</AnimalCount>
        </Treatment>
    </Visit>
    <Visit>
        <VisitDate>2024-02-20</VisitDate>
        <VetPractice>VetKlinik Aarhus</VetPractice>
        <HerdNumber>123456</HerdNumber>
        <Diagnosis>
            <Code>M.02</Code>
            <Description>Lameness</Description>
        </Diagnosis>
        <Treatment>
            <Medicine>Anti-inflammatory</Medicine>
            <Dosage>10ml</Dosage>
            <AnimalCount>1</AnimalCount>
        </Treatment>
    </Visit>
</VetStatData>"""
        },
    }


@pytest.fixture(scope="function")
def chr_date_range() -> dict[str, date]:
    """Date range fixture for CHR testing.

    Provides a standard date range suitable for CHR data queries.
    Defaults to the last 90 days, which is common for CHR data collection.

    Scope: function - Allows tests to modify dates without affecting others.

    Returns:
        Dictionary with 'start' and 'end' date objects.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=90)

    return {"start": start_date, "end": end_date}


@pytest.fixture(scope="function")
def sample_herd_numbers() -> list[str]:
    """Sample CHR herd numbers for testing.

    Scope: function - Allows tests to modify list without affecting others.

    Returns:
        List of valid 6-digit CHR herd numbers.
    """
    return ["123456", "654321", "111111", "999999", "500001"]


@pytest.fixture(scope="function")
def mock_chr_credentials() -> dict[str, str]:
    """Mock CHR API credentials.

    Provides fake credentials for testing authentication logic without
    exposing real credentials.

    Scope: function - Each test gets its own copy.

    Returns:
        Dictionary with credential keys.
    """
    return {
        "username": "test_user",
        "password": "test_password",
        "certificate_path": "/tmp/test_cert.p12",
        "certificate_password": "test_cert_pass",
    }


@pytest.fixture(scope="function")
def mock_soap_response_factory() -> callable:
    """Factory for creating mock SOAP responses.

    Provides a callable that creates properly structured mock SOAP responses
    matching the zeep library's response format.

    Scope: function - Each test gets its own factory.

    Returns:
        Callable that creates mock SOAP responses.
    """

    def create_response(data: dict[str, Any]) -> Mock:
        """Create a mock SOAP response with the given data.

        Args:
            data: Dictionary of response data.

        Returns:
            Mock object that behaves like a zeep response.
        """
        response = Mock()

        # Allow attribute-style access
        for key, value in data.items():
            setattr(response, key, value)

        return response

    return create_response


@pytest.fixture(scope="function")
def configured_mock_soap_client(
    mock_soap_client: MagicMock, mock_chr_responses: dict[str, Any]
) -> MagicMock:
    """Pre-configured mock SOAP client with sample responses.

    Combines mock_soap_client with mock_chr_responses to create a fully
    configured client ready for testing without additional setup.

    Scope: function - Each test gets fresh configuration.

    Args:
        mock_soap_client: Base mock SOAP client.
        mock_chr_responses: Sample response data.

    Returns:
        Configured MagicMock with responses set up.
    """
    # Configure hentDyrListe
    mock_soap_client.service.hentDyrListe.return_value = mock_chr_responses["hentDyrListe"][
        "success"
    ]

    # Configure hentBesaetning
    mock_soap_client.service.hentBesaetning.return_value = mock_chr_responses["hentBesaetning"][
        "success"
    ]

    # Configure hentEjendom
    mock_soap_client.service.hentEjendom.return_value = mock_chr_responses["hentEjendom"]["success"]

    # Configure hentStamdata
    mock_soap_client.service.hentStamdata.return_value = mock_chr_responses["hentStamdata"][
        "success"
    ]

    # Configure hentDiko
    mock_soap_client.service.hentDiko.return_value = mock_chr_responses["hentDiko"]["success"]

    return mock_soap_client


@pytest.fixture(scope="function")
def sample_animal_movements() -> list[dict[str, Any]]:
    """Sample animal movement data for testing.

    Provides realistic animal movement records for testing movement tracking
    and analysis logic.

    Scope: function - Allows tests to modify without affecting others.

    Returns:
        List of animal movement dictionaries.
    """
    return [
        {
            "animal_id": "DK123456789",
            "movement_date": date(2024, 3, 15),
            "from_herd": "123456",
            "to_herd": "654321",
            "movement_type": "SALE",
            "species": "CATTLE",
            "transport_document": "TD123456",
        },
        {
            "animal_id": "DK987654321",
            "movement_date": date(2024, 3, 14),
            "from_herd": "654321",
            "to_herd": "111111",
            "movement_type": "TRANSPORT",
            "species": "CATTLE",
            "transport_document": "TD123457",
        },
        {
            "animal_id": "DK555555555",
            "movement_date": date(2024, 2, 28),
            "from_herd": "111111",
            "to_herd": "123456",
            "movement_type": "PURCHASE",
            "species": "PIG",
            "transport_document": "TD123458",
        },
    ]


@pytest.fixture(scope="function")
def sample_veterinary_visits() -> list[dict[str, Any]]:
    """Sample veterinary visit data for testing.

    Provides realistic veterinary visit records for testing VetStat
    data processing.

    Scope: function - Allows tests to modify without affecting others.

    Returns:
        List of veterinary visit dictionaries.
    """
    return [
        {
            "visit_date": date(2024, 3, 15),
            "herd_number": "123456",
            "vet_practice": "VetKlinik Aarhus",
            "diagnosis_code": "J.01",
            "diagnosis_description": "Respiratory infection",
            "medicine": "Penicillin",
            "dosage": "5ml",
            "animal_count": 3,
        },
        {
            "visit_date": date(2024, 2, 20),
            "herd_number": "123456",
            "vet_practice": "VetKlinik Aarhus",
            "diagnosis_code": "M.02",
            "diagnosis_description": "Lameness",
            "medicine": "Anti-inflammatory",
            "dosage": "10ml",
            "animal_count": 1,
        },
    ]


@pytest.fixture(scope="session")
def chr_species_codes() -> dict[str, str]:
    """CHR species codes mapping.

    Maps species codes used in CHR API to their Danish and English names.

    Scope: session - Immutable reference data.

    Returns:
        Dictionary mapping species codes to names.
    """
    return {
        "CATTLE": {"da": "Kvæg", "en": "Cattle"},
        "PIG": {"da": "Svin", "en": "Pig"},
        "SHEEP": {"da": "Får", "en": "Sheep"},
        "GOAT": {"da": "Ged", "en": "Goat"},
        "HORSE": {"da": "Hest", "en": "Horse"},
        "POULTRY": {"da": "Fjerkræ", "en": "Poultry"},
    }


@pytest.fixture(scope="session")
def chr_movement_types() -> dict[str, str]:
    """CHR movement type codes.

    Maps movement type codes to their descriptions.

    Scope: session - Immutable reference data.

    Returns:
        Dictionary mapping movement type codes to descriptions.
    """
    return {
        "SALE": "Sale to another herd",
        "PURCHASE": "Purchase from another herd",
        "TRANSPORT": "Temporary transport",
        "SLAUGHTER": "Transport to slaughterhouse",
        "DEATH": "Death on farm",
        "EXPORT": "Export from Denmark",
        "IMPORT": "Import to Denmark",
    }


def pytest_configure(config):
    """Configure pytest with custom markers for CHR pipeline tests.

    Args:
        config: Pytest config object.
    """
    config.addinivalue_line("markers", "chr_bronze: mark test as CHR bronze layer test")
    config.addinivalue_line("markers", "chr_silver: mark test as CHR silver layer test")
    config.addinivalue_line("markers", "chr_gold: mark test as CHR gold layer test")
    config.addinivalue_line("markers", "chr_integration: mark test as CHR integration test")
    config.addinivalue_line("markers", "chr_api: mark test as requiring CHR API interaction")
    config.addinivalue_line("markers", "slow: mark test as slow running (>1 second)")
