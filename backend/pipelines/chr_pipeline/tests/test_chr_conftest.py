"""Tests for CHR pipeline-specific fixtures.

This test file validates that the CHR-specific fixtures in conftest.py work correctly.
"""

from datetime import date
from unittest.mock import Mock

import pytest


def test_mock_soap_client(mock_soap_client):
    """Test that mock_soap_client provides expected SOAP interface."""
    # Should have service attribute
    assert hasattr(mock_soap_client, "service")

    # Service should have CHR operations
    service = mock_soap_client.service
    assert hasattr(service, "hentDyrListe")
    assert hasattr(service, "hentBesaetning")
    assert hasattr(service, "hentEjendom")
    assert hasattr(service, "hentStamdata")
    assert hasattr(service, "hentDiko")

    # Should have wsdl attribute
    assert hasattr(mock_soap_client, "wsdl")


def test_mock_chr_responses(mock_chr_responses):
    """Test that mock_chr_responses contains expected data."""
    assert isinstance(mock_chr_responses, dict)

    # Should have key endpoints
    assert "hentDyrListe" in mock_chr_responses
    assert "hentBesaetning" in mock_chr_responses
    assert "hentEjendom" in mock_chr_responses
    assert "hentStamdata" in mock_chr_responses
    assert "hentDiko" in mock_chr_responses
    assert "vetstat" in mock_chr_responses

    # hentDyrListe should have success and error cases
    assert "success" in mock_chr_responses["hentDyrListe"]
    assert "empty" in mock_chr_responses["hentDyrListe"]

    # Success response should have animal list
    success_response = mock_chr_responses["hentDyrListe"]["success"]
    assert "animalList" in success_response
    assert isinstance(success_response["animalList"], list)


def test_chr_date_range(chr_date_range):
    """Test that chr_date_range provides valid date range."""
    assert isinstance(chr_date_range, dict)
    assert "start" in chr_date_range
    assert "end" in chr_date_range

    # Should be date objects
    assert isinstance(chr_date_range["start"], date)
    assert isinstance(chr_date_range["end"], date)

    # End should be after start
    assert chr_date_range["end"] >= chr_date_range["start"]

    # Should be roughly 90 days (default range)
    delta = chr_date_range["end"] - chr_date_range["start"]
    assert 80 <= delta.days <= 100  # Allow some flexibility


def test_sample_herd_numbers(sample_herd_numbers):
    """Test that sample_herd_numbers contains valid CHR numbers."""
    assert isinstance(sample_herd_numbers, list)
    assert len(sample_herd_numbers) > 0

    for herd_num in sample_herd_numbers:
        # Should be 6-digit string
        assert isinstance(herd_num, str)
        assert len(herd_num) == 6
        assert herd_num.isdigit()


def test_mock_chr_credentials(mock_chr_credentials):
    """Test that mock_chr_credentials provides expected keys."""
    assert isinstance(mock_chr_credentials, dict)

    # Should have required credential fields
    assert "username" in mock_chr_credentials
    assert "password" in mock_chr_credentials
    assert "certificate_path" in mock_chr_credentials
    assert "certificate_password" in mock_chr_credentials

    # Values should be strings
    for key, value in mock_chr_credentials.items():
        assert isinstance(value, str)


def test_mock_soap_response_factory(mock_soap_response_factory):
    """Test that mock_soap_response_factory creates valid responses."""
    # Should be callable
    assert callable(mock_soap_response_factory)

    # Create a test response
    test_data = {"field1": "value1", "field2": 123, "field3": ["a", "b", "c"]}

    response = mock_soap_response_factory(test_data)

    # Should be a Mock object
    assert isinstance(response, Mock)

    # Should support attribute access
    assert response.field1 == "value1"
    assert response.field2 == 123
    assert response.field3 == ["a", "b", "c"]


def test_configured_mock_soap_client(configured_mock_soap_client, mock_chr_responses):
    """Test that configured_mock_soap_client has responses set up."""
    client = configured_mock_soap_client

    # Should be configured with responses
    assert client.service.hentDyrListe() == mock_chr_responses["hentDyrListe"]["success"]
    assert client.service.hentBesaetning() == mock_chr_responses["hentBesaetning"]["success"]
    assert client.service.hentEjendom() == mock_chr_responses["hentEjendom"]["success"]
    assert client.service.hentStamdata() == mock_chr_responses["hentStamdata"]["success"]
    assert client.service.hentDiko() == mock_chr_responses["hentDiko"]["success"]


def test_sample_animal_movements(sample_animal_movements):
    """Test that sample_animal_movements contains valid data."""
    assert isinstance(sample_animal_movements, list)
    assert len(sample_animal_movements) > 0

    for movement in sample_animal_movements:
        # Should have required fields
        assert "animal_id" in movement
        assert "movement_date" in movement
        assert "from_herd" in movement
        assert "to_herd" in movement
        assert "movement_type" in movement
        assert "species" in movement

        # Herd numbers should be 6 digits
        assert len(movement["from_herd"]) == 6
        assert len(movement["to_herd"]) == 6

        # Movement date should be a date object
        assert isinstance(movement["movement_date"], date)


def test_sample_veterinary_visits(sample_veterinary_visits):
    """Test that sample_veterinary_visits contains valid data."""
    assert isinstance(sample_veterinary_visits, list)
    assert len(sample_veterinary_visits) > 0

    for visit in sample_veterinary_visits:
        # Should have required fields
        assert "visit_date" in visit
        assert "herd_number" in visit
        assert "vet_practice" in visit
        assert "diagnosis_code" in visit
        assert "medicine" in visit

        # Herd number should be 6 digits
        assert len(visit["herd_number"]) == 6

        # Visit date should be a date object
        assert isinstance(visit["visit_date"], date)

        # Animal count should be positive
        assert visit["animal_count"] > 0


def test_chr_species_codes(chr_species_codes):
    """Test that chr_species_codes provides valid mappings."""
    assert isinstance(chr_species_codes, dict)

    # Should have common species
    assert "CATTLE" in chr_species_codes
    assert "PIG" in chr_species_codes
    assert "SHEEP" in chr_species_codes

    # Each species should have Danish and English names
    for species, names in chr_species_codes.items():
        assert "da" in names
        assert "en" in names
        assert isinstance(names["da"], str)
        assert isinstance(names["en"], str)


def test_chr_movement_types(chr_movement_types):
    """Test that chr_movement_types provides valid mappings."""
    assert isinstance(chr_movement_types, dict)

    # Should have common movement types
    assert "SALE" in chr_movement_types
    assert "PURCHASE" in chr_movement_types
    assert "TRANSPORT" in chr_movement_types
    assert "SLAUGHTER" in chr_movement_types

    # Each type should have a description
    for movement_type, description in chr_movement_types.items():
        assert isinstance(description, str)
        assert len(description) > 0


def test_vetstat_xml_response(mock_chr_responses):
    """Test that VetStat XML response is valid XML."""
    xml_response = mock_chr_responses["vetstat"]["success"]

    # Should be a string
    assert isinstance(xml_response, str)

    # Should contain XML declaration
    assert "<?xml" in xml_response

    # Should contain VetStat elements
    assert "VetStatData" in xml_response
    assert "Visit" in xml_response
    assert "Diagnosis" in xml_response
    assert "Treatment" in xml_response

    # Basic XML validation - should be parseable
    import xml.etree.ElementTree as ET

    try:
        ET.fromstring(xml_response)
    except ET.ParseError as e:
        pytest.fail(f"Invalid XML in vetstat response: {e}")
