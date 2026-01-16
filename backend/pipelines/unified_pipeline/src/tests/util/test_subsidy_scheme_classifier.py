"""
Tests for subsidy scheme classification utility.
"""

from unified_pipeline.util.subsidy_scheme_classifier import (
    DERIVED_PAYMENT_RATES,
    classify_deminimis,
    classify_eu_scheme,
    classify_fvm_tilsagn,
    get_expected_rate,
    get_pillar,
    is_summary_row,
)


class TestClassifyEUScheme:
    """Tests for EU scheme classification."""

    def test_grundbetaling(self):
        """Test basic payment scheme classification."""
        code, danish = classify_eu_scheme("Basic payment scheme")
        assert code == "GRUNDBETALING"
        assert danish == "Grundbetaling"

    def test_groen_stoette(self):
        """Test greening payment classification."""
        code, danish = classify_eu_scheme(
            "Payment for agricultural practices beneficial for the climate and the environment"
        )
        assert code == "GROEN_STOETTE"
        assert danish == "Grøn støtte"

    def test_organic_farming(self):
        """Test organic farming scheme classification."""
        code, danish = classify_eu_scheme("Organic farming")
        assert code == "OEKOLOGISK"
        assert danish == "Økologisk arealtilskud"

    def test_summary_row_detection(self):
        """Test that summary rows are classified correctly."""
        code, danish = classify_eu_scheme("Total for beneficiary")
        assert code == "TOTAL_SUMMARY"

    def test_unknown_scheme(self):
        """Test handling of unknown scheme names."""
        code, danish = classify_eu_scheme("Unknown scheme XYZ")
        assert code is None
        assert danish is None

    def test_none_input(self):
        """Test handling of None input."""
        code, danish = classify_eu_scheme(None)
        assert code is None
        assert danish is None


class TestClassifyFVMTilsagn:
    """Tests for FVM commitment type classification."""

    def test_organic_36(self):
        """Test organic 36 (omlægning) classification."""
        code, danish = classify_fvm_tilsagn("36")
        assert code == "OEKOLOGISK_36"
        assert "Omlægning" in danish

    def test_organic_37(self):
        """Test organic 37 (opretholdelse) classification."""
        code, danish = classify_fvm_tilsagn("37")
        assert code == "OEKOLOGISK_37"
        assert "Opretholdelse" in danish

    def test_grassland_66(self):
        """Test grassland 66 classification."""
        code, danish = classify_fvm_tilsagn("66")
        assert code == "GRAESPLEJE_66"

    def test_grassland_67(self):
        """Test grassland 67 classification."""
        code, danish = classify_fvm_tilsagn("67")
        assert code == "GRAESPLEJE_67"
        assert "afgræsning" in danish.lower()

    def test_unknown_code(self):
        """Test handling of unknown codes."""
        code, danish = classify_fvm_tilsagn("999")
        assert code is None


class TestClassifyDeminimis:
    """Tests for de minimis scheme classification."""

    def test_mrdm(self):
        """Test MRDM classification."""
        code, danish = classify_deminimis("MRDM")
        assert code == "DEMINIMIS_MRDM"
        assert "De minimis" in danish

    def test_case_insensitive(self):
        """Test case-insensitive matching."""
        code1, _ = classify_deminimis("mrdm")
        code2, _ = classify_deminimis("MRDM")
        assert code1 == code2


class TestIsSummaryRow:
    """Tests for summary row detection."""

    def test_total_for_beneficiary(self):
        """Test detection of 'Total for beneficiary' rows."""
        assert is_summary_row("Total for beneficiary") is True

    def test_partial_match(self):
        """Test detection with partial matches."""
        assert is_summary_row("Total") is True
        assert is_summary_row("TOTAL") is True

    def test_not_summary(self):
        """Test that regular scheme names are not flagged."""
        assert is_summary_row("Basic payment scheme") is False
        assert is_summary_row("Organic farming") is False

    def test_none_input(self):
        """Test handling of None input."""
        assert is_summary_row(None) is False
        assert is_summary_row("") is False


class TestGetPillar:
    """Tests for pillar determination."""

    def test_pillar1_schemes(self):
        """Test that Pillar 1 schemes are correctly identified."""
        assert get_pillar("GRUNDBETALING") == 1
        assert get_pillar("GROEN_STOETTE") == 1
        assert get_pillar("KOBLET_STOETTE") == 1

    def test_pillar2_schemes(self):
        """Test that Pillar 2 schemes are correctly identified."""
        assert get_pillar("OEKOLOGISK") == 2
        assert get_pillar("GRAESPLEJE") == 2
        assert get_pillar("NATURA2000") == 2

    def test_pillar2_with_suffix(self):
        """Test that Pillar 2 schemes with suffixes are correctly identified."""
        assert get_pillar("OEKOLOGISK_36") == 2
        assert get_pillar("GRAESPLEJE_67") == 2

    def test_unknown_scheme(self):
        """Test handling of unknown schemes."""
        assert get_pillar("UNKNOWN_SCHEME") is None


class TestGetExpectedRate:
    """Tests for payment rate lookup."""

    def test_derived_rate(self):
        """Test lookup of data-derived rates."""
        rate = get_expected_rate("OEKOLOGISK_36", use_derived=True)
        assert rate == 868  # Data-derived rate

    def test_official_rate(self):
        """Test lookup of official rates."""
        rate = get_expected_rate("OEKOLOGISK_36", use_derived=False)
        assert rate == 955  # Official rate

    def test_unknown_scheme(self):
        """Test handling of unknown schemes."""
        rate = get_expected_rate("UNKNOWN_SCHEME")
        assert rate is None


class TestDerivedRates:
    """Tests for derived payment rates."""

    def test_derived_rates_exist(self):
        """Test that key derived rates are defined."""
        assert "OEKOLOGISK_36" in DERIVED_PAYMENT_RATES
        assert "OEKOLOGISK_37" in DERIVED_PAYMENT_RATES
        assert "GRAESPLEJE_66" in DERIVED_PAYMENT_RATES
        assert "GRAESPLEJE_67" in DERIVED_PAYMENT_RATES
        assert "GRUNDBETALING" in DERIVED_PAYMENT_RATES

    def test_derived_rates_reasonable(self):
        """Test that derived rates are within reasonable ranges."""
        for scheme, rate in DERIVED_PAYMENT_RATES.items():
            assert rate > 0, f"Rate for {scheme} should be positive"
            assert rate < 10000, f"Rate for {scheme} seems too high: {rate}"
