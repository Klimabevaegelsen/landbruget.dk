"""Tests for FVM WFS year discovery utilities."""

from unittest.mock import patch

import pytest

from unified_pipeline.util.fvm_wfs_year_discovery import (
    FVMWFSYearDiscoveryError,
    discover_marker_related_years,
    extract_layer_years,
    fetch_wfs_capabilities,
)


def test_extract_layer_years_parses_and_sorts_known_layers() -> None:
    """extract_layer_years should parse known year-suffixed FVM layers."""
    capabilities_xml = """
    <Name>Marker:Marker_2026</Name>
    <Name>Marker:Marker_2024</Name>
    <Name>Marker:Marker_2026</Name>
    <Name>Marker:Smaabiotoper_2026</Name>
    <Name>Marker:Smaabiotoper_2025</Name>
    <Name>Markblokke:Markblokke_2026</Name>
    <Name>Miljoe_og_oekologitilsagn:Oekologiske_arealer_2025</Name>
    <Name>Miljoe_og_oekologitilsagn:Tilsagn_til_oekologiske_arealtilskud_2015-2020_2024</Name>
    <Name>Miljoe_og_oekologitilsagn:Tilsagn_til_pleje_af_graes_2015-2020_2023</Name>
    <Name>Miljoe_og_oekologitilsagn:Miljoetilsagn_oevrige_typer_2023</Name>
    """
    layer_years = extract_layer_years(capabilities_xml)

    assert layer_years["marker"] == [2024, 2026]
    assert layer_years["smaabiotoper"] == [2025, 2026]
    assert layer_years["markblokke"] == [2026]
    assert layer_years["organic_areas"] == [2025]
    assert layer_years["organic_subsidies"] == [2024]
    assert layer_years["grassland_subsidies"] == [2023]
    assert layer_years["environmental_subsidies"] == [2023]


def test_discover_marker_related_years_returns_marker_subset() -> None:
    """discover_marker_related_years should only expose marker-related keys."""
    capabilities_xml = """
    <Name>Marker:Marker_2026</Name>
    <Name>Marker:Smaabiotoper_2026</Name>
    <Name>Markblokke:Markblokke_2026</Name>
    """
    with patch(
        "unified_pipeline.util.fvm_wfs_year_discovery.fetch_wfs_capabilities",
        return_value=capabilities_xml,
    ):
        years = discover_marker_related_years("https://example.test/geoserver/wfs")

    assert years == {
        "marker": [2026],
        "smaabiotoper": [2026],
    }


def test_discover_marker_related_years_raises_on_missing_marker_layers() -> None:
    """discover_marker_related_years should fail when no marker-related layers are found."""
    capabilities_xml = "<Name>Markblokke:Markblokke_2026</Name>"
    with (
        patch(
            "unified_pipeline.util.fvm_wfs_year_discovery.fetch_wfs_capabilities",
            return_value=capabilities_xml,
        ),
        pytest.raises(FVMWFSYearDiscoveryError, match="Marker:Marker_\\*"),
    ):
        discover_marker_related_years("https://example.test/geoserver/wfs")


def test_fetch_wfs_capabilities_raises_with_endpoint_context() -> None:
    """fetch_wfs_capabilities should wrap network errors with endpoint context."""
    with (
        patch(
            "unified_pipeline.util.fvm_wfs_year_discovery.urlopen",
            side_effect=OSError("network down"),
        ),
        pytest.raises(FVMWFSYearDiscoveryError, match="Failed to fetch WFS capabilities from"),
    ):
        fetch_wfs_capabilities("https://example.test/geoserver/wfs")
