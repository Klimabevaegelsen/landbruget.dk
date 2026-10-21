"""Tests for primary address selection when coordinate accuracy is unavailable."""

from unified_pipeline.util.primary_address_selector import select_primary_address_for_company_table


def test_null_coordinate_quality_is_lowest_grade_and_keeps_geocoded_address():
    lower_accuracy = {
        "full_address": "B address",
        "address_type": "beliggenhedsadresse",
        "is_current": True,
        "dawa_enriched": True,
        "coordinate_quality": None,
        "latitude": 55.6,
        "longitude": 12.5,
    }
    known_accuracy = {
        **lower_accuracy,
        "full_address": "A address",
        "coordinate_quality": "A",
    }

    selected = select_primary_address_for_company_table([lower_accuracy, known_accuracy])

    assert selected is known_accuracy
    assert select_primary_address_for_company_table([lower_accuracy]) is lower_accuracy
