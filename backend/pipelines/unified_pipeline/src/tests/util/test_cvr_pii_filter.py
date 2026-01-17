"""
Tests for CVR PII Filter.

Tests for: unified_pipeline/util/cvr_pii_filter.py

Covers:
- PII field removal from deltager (personal) data
- CPR number detection (verified entity numbers are not CPR)
- Sensitive data filtering
- Business information preservation
- Validation of filtering completeness
"""

import copy

import pytest

from unified_pipeline.util.cvr_pii_filter import (
    filter_cvr_pii,
    filter_deltager_data,
    filter_deltager_relations,
    get_pii_filtering_summary,
    validate_pii_filtering,
)


class TestFilterCVRPII:
    """Tests for the main PII filtering function."""

    def test_filter_empty_cvr_data(self):
        """Test filtering empty CVR data."""
        empty_data = {}
        result = filter_cvr_pii(empty_data)

        assert result == {}

    def test_filter_cvr_data_without_deltager(self):
        """Test filtering CVR data without deltager relations."""
        cvr_data = {
            "cvrNummer": 31373077,
            "virksomhedMetadata": {"nyesteNavn": {"navn": "Test Company"}},
        }
        result = filter_cvr_pii(cvr_data)

        # Should preserve all data when no deltager present
        assert result["cvrNummer"] == 31373077
        assert result["virksomhedMetadata"]["nyesteNavn"]["navn"] == "Test Company"

    def test_filter_cvr_data_preserves_company_info(self):
        """Test that company-level information is preserved."""
        cvr_data = {
            "cvrNummer": 31373077,
            "virksomhedMetadata": {"nyesteNavn": {"navn": "Test Company"}},
            "beliggenhedsadresse": [{"vejnavn": "Testvej", "husnr": "1"}],
            "telefonNummer": [{"nummer": "12345678"}],
            "elektroniskPost": [{"email": "info@company.dk"}],
        }
        result = filter_cvr_pii(cvr_data)

        # Company contact info should be preserved
        assert "beliggenhedsadresse" in result
        assert "telefonNummer" in result
        assert "elektroniskPost" in result

    def test_filter_modifies_copy_not_original(self):
        """Test that filtering doesn't modify original data."""
        original_data = {
            "cvrNummer": 31373077,
            "deltagerRelation": [{"deltager": {"telefonNummer": [{"nummer": "12345678"}]}}],
        }

        # Make a copy to compare later
        original_copy = copy.deepcopy(original_data)

        filtered = filter_cvr_pii(original_data)

        # Original should be unchanged
        assert original_data == original_copy


class TestFilterDeltagerRelations:
    """Tests for filtering deltager relations."""

    def test_filter_empty_relations(self):
        """Test filtering empty deltager relations list."""
        result = filter_deltager_relations([])
        assert result == []

    def test_filter_single_deltager_relation(self):
        """Test filtering single deltager relation."""
        relations = [
            {
                "deltager": {
                    "enhedsNummer": 12345678,
                    "telefonNummer": [{"nummer": "12345678"}],
                    "navne": [{"navn": "John Doe"}],
                }
            }
        ]

        result = filter_deltager_relations(relations)

        # Personal phone should be removed, but entity number and name preserved
        assert len(result) == 1
        assert "enhedsNummer" in result[0]["deltager"]
        assert "navne" in result[0]["deltager"]
        assert "telefonNummer" not in result[0]["deltager"]

    def test_filter_multiple_deltager_relations(self):
        """Test filtering multiple deltager relations."""
        relations = [
            {"deltager": {"telefonNummer": [{"nummer": "11111111"}]}},
            {"deltager": {"elektroniskPost": [{"email": "person@example.com"}]}},
            {"deltager": {"postadresse": [{"vejnavn": "Privatvej"}]}},
        ]

        result = filter_deltager_relations(relations)

        # All PII should be removed
        assert len(result) == 3
        for relation in result:
            assert "telefonNummer" not in relation["deltager"]
            assert "elektroniskPost" not in relation["deltager"]
            assert "postadresse" not in relation["deltager"]


class TestFilterDeltagerData:
    """Tests for filtering individual deltager data."""

    def test_remove_personal_addresses(self):
        """Test removal of personal address fields."""
        deltager = {
            "enhedsNummer": 12345678,
            "beliggenhedsadresse": [{"vejnavn": "Privatvej", "husnr": "1"}],
            "postadresse": [{"postnummer": "2400", "bynavn": "København NV"}],
        }

        result = filter_deltager_data(deltager)

        # Personal addresses should be removed
        assert "beliggenhedsadresse" not in result
        assert "postadresse" not in result
        # Entity number should be preserved
        assert result["enhedsNummer"] == 12345678

    def test_remove_personal_contact_info(self):
        """Test removal of personal contact information."""
        deltager = {
            "enhedsNummer": 12345678,
            "telefonNummer": [{"nummer": "12345678"}],
            "telefaxNummer": [{"nummer": "87654321"}],
            "sekundaertTelefonNummer": [{"nummer": "11111111"}],
            "sekundaertTelefaxNummer": [{"nummer": "22222222"}],
            "elektroniskPost": [{"email": "person@example.com"}],
            "obligatoriskEmail": [{"email": "required@example.com"}],
        }

        result = filter_deltager_data(deltager)

        # All contact info should be removed
        assert "telefonNummer" not in result
        assert "telefaxNummer" not in result
        assert "sekundaertTelefonNummer" not in result
        assert "sekundaertTelefaxNummer" not in result
        assert "elektroniskPost" not in result
        assert "obligatoriskEmail" not in result

    def test_preserve_entity_number(self):
        """Test that entity numbers are preserved (not CPR)."""
        deltager = {
            "enhedsNummer": 12345678,
            "enhedstype": "PERSON",
            "telefonNummer": [{"nummer": "12345678"}],
        }

        result = filter_deltager_data(deltager)

        # Entity number and type should be preserved
        assert result["enhedsNummer"] == 12345678
        assert result["enhedstype"] == "PERSON"
        # But personal contact removed
        assert "telefonNummer" not in result

    def test_preserve_names(self):
        """Test that names are preserved (public business information)."""
        deltager = {
            "enhedsNummer": 12345678,
            "navne": [{"navn": "John Doe", "periode": {"gyldigFra": "2020-01-01"}}],
            "telefonNummer": [{"nummer": "12345678"}],
        }

        result = filter_deltager_data(deltager)

        # Names should be preserved (public business information)
        assert "navne" in result
        assert result["navne"][0]["navn"] == "John Doe"
        # But personal contact removed
        assert "telefonNummer" not in result

    def test_preserve_business_registration_data(self):
        """Test that business registration data is preserved."""
        deltager = {
            "enhedsNummer": 12345678,
            "enhedstype": "PERSON",
            "organisationstype": "Personligt ejet mindre virksomhed",
            "navne": [{"navn": "John Doe"}],
            "telefonNummer": [{"nummer": "12345678"}],
        }

        result = filter_deltager_data(deltager)

        # Business registration fields should be preserved
        assert result["enhedsNummer"] == 12345678
        assert result["enhedstype"] == "PERSON"
        assert result["organisationstype"] == "Personligt ejet mindre virksomhed"
        assert "navne" in result


class TestPIIFilteringSummary:
    """Tests for PII filtering summary generation."""

    def test_summary_empty_data(self):
        """Test summary with empty data."""
        original = {}
        filtered = {}

        summary = get_pii_filtering_summary(original, filtered)

        assert summary["deltager_relations_processed"] == 0
        assert summary["personal_addresses_removed"] == 0
        assert summary["personal_contacts_removed"] == 0

    def test_summary_counts_removed_addresses(self):
        """Test that summary counts removed addresses."""
        original = {
            "deltagerRelation": [
                {
                    "deltager": {
                        "beliggenhedsadresse": [{"vejnavn": "Vej 1"}],
                        "postadresse": [{"postnummer": "2400"}],
                    }
                }
            ]
        }
        filtered = {"deltagerRelation": [{"deltager": {}}]}

        summary = get_pii_filtering_summary(original, filtered)

        assert summary["deltager_relations_processed"] == 1
        assert summary["personal_addresses_removed"] == 2
        assert "beliggenhedsadresse" in summary["fields_removed"]
        assert "postadresse" in summary["fields_removed"]

    def test_summary_counts_removed_contacts(self):
        """Test that summary counts removed contact information."""
        original = {
            "deltagerRelation": [
                {
                    "deltager": {
                        "telefonNummer": [{"nummer": "12345678"}],
                        "elektroniskPost": [{"email": "test@example.com"}],
                    }
                }
            ]
        }
        filtered = {"deltagerRelation": [{"deltager": {}}]}

        summary = get_pii_filtering_summary(original, filtered)

        assert summary["personal_contacts_removed"] == 2
        assert "telefonNummer" in summary["fields_removed"]
        assert "elektroniskPost" in summary["fields_removed"]

    def test_summary_multiple_relations(self):
        """Test summary with multiple deltager relations."""
        original = {
            "deltagerRelation": [
                {"deltager": {"telefonNummer": [{"nummer": "11111111"}]}},
                {"deltager": {"telefonNummer": [{"nummer": "22222222"}]}},
                {"deltager": {"beliggenhedsadresse": [{"vejnavn": "Vej"}]}},
            ]
        }
        filtered = {"deltagerRelation": [{}, {}, {}]}

        summary = get_pii_filtering_summary(original, filtered)

        assert summary["deltager_relations_processed"] == 3
        assert summary["personal_contacts_removed"] == 2
        assert summary["personal_addresses_removed"] == 1


class TestValidatePIIFiltering:
    """Tests for PII filtering validation."""

    def test_validate_completely_filtered_data(self):
        """Test validation of properly filtered data."""
        filtered = {
            "cvrNummer": 31373077,
            "deltagerRelation": [
                {
                    "deltager": {
                        "enhedsNummer": 12345678,
                        "navne": [{"navn": "John Doe"}],
                    }
                }
            ],
        }

        validation = validate_pii_filtering(filtered)

        assert validation["is_valid"] is True
        assert len(validation["issues"]) == 0

    def test_validate_detects_personal_addresses(self):
        """Test validation detects remaining personal addresses."""
        filtered = {
            "deltagerRelation": [{"deltager": {"beliggenhedsadresse": [{"vejnavn": "Privatvej"}]}}]
        }

        validation = validate_pii_filtering(filtered)

        assert validation["is_valid"] is False
        assert len(validation["issues"]) > 0
        assert any("beliggenhedsadresse" in issue for issue in validation["issues"])

    def test_validate_detects_personal_contacts(self):
        """Test validation detects remaining personal contacts."""
        filtered = {"deltagerRelation": [{"deltager": {"telefonNummer": [{"nummer": "12345678"}]}}]}

        validation = validate_pii_filtering(filtered)

        assert validation["is_valid"] is False
        assert len(validation["issues"]) > 0
        assert any("telefonNummer" in issue for issue in validation["issues"])

    def test_validate_detects_all_pii_fields(self):
        """Test validation detects all types of PII fields."""
        filtered = {
            "deltagerRelation": [
                {
                    "deltager": {
                        "beliggenhedsadresse": [{"vejnavn": "Vej"}],
                        "postadresse": [{"postnummer": "2400"}],
                        "telefonNummer": [{"nummer": "12345678"}],
                        "telefaxNummer": [{"nummer": "87654321"}],
                        "sekundaertTelefonNummer": [{"nummer": "11111111"}],
                        "sekundaertTelefaxNummer": [{"nummer": "22222222"}],
                        "elektroniskPost": [{"email": "test@example.com"}],
                        "obligatoriskEmail": [{"email": "required@example.com"}],
                    }
                }
            ]
        }

        validation = validate_pii_filtering(filtered)

        assert validation["is_valid"] is False
        # Should detect all 8 PII field types
        assert len(validation["issues"]) == 8

    def test_validate_allows_company_level_data(self):
        """Test validation allows company-level addresses and contacts."""
        filtered = {
            "cvrNummer": 31373077,
            "beliggenhedsadresse": [{"vejnavn": "Firmavej", "husnr": "1"}],
            "telefonNummer": [{"nummer": "12345678"}],
            "elektroniskPost": [{"email": "info@company.dk"}],
            "deltagerRelation": [],
        }

        validation = validate_pii_filtering(filtered)

        # Company-level data is allowed
        assert validation["is_valid"] is True
        # But should have warnings about preserved company data
        assert len(validation["warnings"]) > 0

    def test_validate_empty_deltager_relations(self):
        """Test validation with no deltager relations."""
        filtered = {"cvrNummer": 31373077, "deltagerRelation": []}

        validation = validate_pii_filtering(filtered)

        assert validation["is_valid"] is True
        assert len(validation["issues"]) == 0


class TestRealWorldScenarios:
    """Tests based on real CVR data patterns."""

    def test_filter_agricultural_company_owner(self):
        """Test filtering of agricultural company owner data."""
        cvr_data = {
            "cvrNummer": 31373077,
            "virksomhedMetadata": {"nyesteNavn": {"navn": "Test Farm"}},
            "deltagerRelation": [
                {
                    "organisationstype": "Personligt ejet mindre virksomhed",
                    "deltager": {
                        "enhedsNummer": 12345678,
                        "enhedstype": "PERSON",
                        "navne": [{"navn": "John Farmer"}],
                        "beliggenhedsadresse": [{"vejnavn": "Farmvej", "husnr": "42"}],
                        "telefonNummer": [{"nummer": "12345678"}],
                    },
                }
            ],
        }

        result = filter_cvr_pii(cvr_data)

        # Company name and CVR should be preserved
        assert result["cvrNummer"] == 31373077
        assert result["virksomhedMetadata"]["nyesteNavn"]["navn"] == "Test Farm"

        # Owner's business registration should be preserved
        deltager = result["deltagerRelation"][0]["deltager"]
        assert deltager["enhedsNummer"] == 12345678
        assert deltager["navne"][0]["navn"] == "John Farmer"

        # Owner's personal contact info should be removed
        assert "beliggenhedsadresse" not in deltager
        assert "telefonNummer" not in deltager

    def test_filter_multiple_owners(self):
        """Test filtering with multiple company owners."""
        cvr_data = {
            "cvrNummer": 31373077,
            "deltagerRelation": [
                {
                    "deltager": {
                        "enhedsNummer": 11111111,
                        "navne": [{"navn": "Owner 1"}],
                        "telefonNummer": [{"nummer": "11111111"}],
                    }
                },
                {
                    "deltager": {
                        "enhedsNummer": 22222222,
                        "navne": [{"navn": "Owner 2"}],
                        "elektroniskPost": [{"email": "owner2@example.com"}],
                    }
                },
            ],
        }

        result = filter_cvr_pii(cvr_data)

        # Both owners should have PII removed but business data preserved
        assert len(result["deltagerRelation"]) == 2

        for relation in result["deltagerRelation"]:
            deltager = relation["deltager"]
            # Business data preserved
            assert "enhedsNummer" in deltager
            assert "navne" in deltager
            # Personal contact removed
            assert "telefonNummer" not in deltager
            assert "elektroniskPost" not in deltager

    def test_entity_numbers_are_not_cpr(self):
        """Test that entity numbers are confirmed not to be CPR numbers."""
        deltager = {
            "enhedsNummer": 12345678,  # 8 digits, not 10 like CPR
            "enhedstype": "PERSON",
            "navne": [{"navn": "Test Person"}],
            "telefonNummer": [{"nummer": "87654321"}],
        }

        result = filter_deltager_data(deltager)

        # Entity number should be kept (confirmed not CPR)
        assert "enhedsNummer" in result
        assert result["enhedsNummer"] == 12345678
        # But personal phone removed
        assert "telefonNummer" not in result
