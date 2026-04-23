"""
Utilities for discovering available FVM WFS layer years from GetCapabilities.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import urlopen

FVM_LAYER_PATTERNS: dict[str, str] = {
    "markblokke": r"\bMarkblokke:Markblokke_(\d{4})\b",
    "marker": r"\bMarker:Marker_(\d{4})\b",
    "smaabiotoper": r"\bMarker:Smaabiotoper_(\d{4})\b",
    "organic_areas": r"\bMiljoe_og_oekologitilsagn:Oekologiske_arealer_(\d{4})\b",
    "organic_subsidies": (
        r"\bMiljoe_og_oekologitilsagn:Tilsagn_til_oekologiske_arealtilskud_2015-2020_(\d{4})\b"
    ),
    "grassland_subsidies": (
        r"\bMiljoe_og_oekologitilsagn:Tilsagn_til_pleje_af_graes_2015-2020_(\d{4})\b"
    ),
    "environmental_subsidies": (
        r"\bMiljoe_og_oekologitilsagn:Miljoetilsagn_oevrige_typer_(\d{4})\b"
    ),
}


class FVMWFSYearDiscoveryError(RuntimeError):
    """Raised when FVM WFS year discovery fails."""


def fetch_wfs_capabilities(wfs_url: str, timeout_s: int = 30) -> str:
    """
    Fetch WFS GetCapabilities XML payload from the configured endpoint.

    Args:
        wfs_url: Base WFS endpoint URL.
        timeout_s: Request timeout in seconds.

    Returns:
        Raw GetCapabilities XML as text.

    Raises:
        FVMWFSYearDiscoveryError: If request fails or returns an empty payload.
    """
    parts = urlsplit(wfs_url)
    query_params = dict(parse_qsl(parts.query, keep_blank_values=True))
    query_params.update(
        {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetCapabilities",
        }
    )
    capabilities_url = urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query_params),
            parts.fragment,
        )
    )

    try:
        with urlopen(capabilities_url, timeout=timeout_s) as response:
            xml = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        raise FVMWFSYearDiscoveryError(
            f"Failed to fetch WFS capabilities from {wfs_url}: {exc}"
        ) from exc

    if not xml.strip():
        raise FVMWFSYearDiscoveryError(f"Empty WFS capabilities response from {wfs_url}")

    return xml


def extract_layer_years(capabilities_xml: str) -> dict[str, list[int]]:
    """
    Extract known FVM layer years from WFS GetCapabilities XML.

    Args:
        capabilities_xml: Raw GetCapabilities XML payload.

    Returns:
        Dictionary with sorted unique years for known year-suffixed FVM layers.
    """
    return {
        layer_name: sorted({int(year) for year in re.findall(pattern, capabilities_xml)})
        for layer_name, pattern in FVM_LAYER_PATTERNS.items()
    }


def discover_fvm_layer_years(wfs_url: str, timeout_s: int = 30) -> dict[str, list[int]]:
    """
    Discover all known FVM WFS layer years from live WFS capabilities.

    Args:
        wfs_url: Base WFS endpoint URL.
        timeout_s: Request timeout in seconds.

    Returns:
        Dictionary keyed by FVM layer name with sorted year lists.

    Raises:
        FVMWFSYearDiscoveryError: If fetching/parsing fails or no known layers are found.
    """
    try:
        capabilities_xml = fetch_wfs_capabilities(wfs_url=wfs_url, timeout_s=timeout_s)
        layer_years = extract_layer_years(capabilities_xml)
    except FVMWFSYearDiscoveryError:
        raise
    except Exception as exc:
        raise FVMWFSYearDiscoveryError(
            f"Failed to discover FVM layer years from {wfs_url}: {exc}"
        ) from exc

    if not any(layer_years.values()):
        raise FVMWFSYearDiscoveryError(
            f"No known FVM year-suffixed layers found in capabilities from {wfs_url}"
        )

    return layer_years


def discover_marker_related_years(wfs_url: str, timeout_s: int = 30) -> dict[str, list[int]]:
    """
    Discover marker-related years from live WFS capabilities.

    Args:
        wfs_url: Base WFS endpoint URL.
        timeout_s: Request timeout in seconds.

    Returns:
        Dictionary with keys `marker` and `smaabiotoper`.

    Raises:
        FVMWFSYearDiscoveryError: If fetching/parsing fails or no marker-related
            layers are found at all.
    """
    layer_years = discover_fvm_layer_years(wfs_url=wfs_url, timeout_s=timeout_s)
    marker_related = {
        "marker": layer_years["marker"],
        "smaabiotoper": layer_years["smaabiotoper"],
    }

    if not marker_related["marker"] and not marker_related["smaabiotoper"]:
        raise FVMWFSYearDiscoveryError(
            f"No Marker:Marker_* or Marker:Smaabiotoper_* layers found in capabilities from {wfs_url}"
        )

    return marker_related
