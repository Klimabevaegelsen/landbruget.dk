"""GML parsing helpers for Jordbrugsanalyser marker features."""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid

NAMESPACES: dict[str, str] = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "Jordbrugsanalyser": "Jordbrugsanalyser",
}

FIELD_MAPPING: dict[str, tuple[str, Any]] = {
    "AfgKat": ("crop_category", str),
    "AfgNavn": ("crop_name", str),
    "AfgNr": ("crop_code", lambda x: int(x) if x and x.isdigit() else None),
    "EjerNr": ("owner_number", lambda x: int(x) if x and x.isdigit() else None),
    "Ha": ("area_ha", lambda x: float(x) if x else None),
    "HaIalt": ("total_area_ha", lambda x: float(x) if x else None),
    "MarkBlok": ("field_block", str),
    "MarkNr": ("field_number", str),
    "X": ("centroid_x", lambda x: float(x) if x else None),
    "Y": ("centroid_y", lambda x: float(x) if x else None),
}


def _log(logger: Any, level: str, message: str) -> None:
    if logger is not None:
        getattr(logger, level)(message)


def clean_text_value(value: str | None) -> str | None:
    """Clean and normalize text values from the WFS XML."""
    if not value or not isinstance(value, str):
        return None

    cleaned = value.strip()
    if not cleaned:
        return None

    replacements = {
        "gr�s": "græs",
        "\ufffd": "ø",
    }
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)

    return cleaned


def parse_geometry(geom_elem: ET.Element, logger: Any | None = None) -> str | None:
    """Parse a GML geometry element to WKT."""
    try:
        multi_surface = geom_elem.find(".//gml:MultiSurface", NAMESPACES)
        if multi_surface is None:
            polygon_elem = geom_elem.find(".//gml:Polygon", NAMESPACES)
            if polygon_elem is None:
                _log(logger, "warning", "No MultiSurface or Polygon found in geometry")
                return None
            polygons = [polygon_elem]
        else:
            polygons = multi_surface.findall(".//gml:Polygon", NAMESPACES)

        if not polygons:
            _log(logger, "warning", "No Polygon elements found in geometry")
            return None

        parsed_polygons = []

        for polygon in polygons:
            exterior_elem = polygon.find(".//gml:exterior/gml:LinearRing/gml:posList", NAMESPACES)
            if exterior_elem is None or not exterior_elem.text:
                _log(logger, "warning", "No exterior ring found in polygon")
                continue

            coords = [float(x) for x in exterior_elem.text.strip().split()]
            exterior_coords = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]
            if exterior_coords[0] != exterior_coords[-1]:
                exterior_coords.append(exterior_coords[0])

            interior_rings = []
            interior_elems = polygon.findall(
                ".//gml:interior/gml:LinearRing/gml:posList", NAMESPACES
            )
            for interior_elem in interior_elems:
                if interior_elem.text:
                    interior_coords_raw = [float(x) for x in interior_elem.text.strip().split()]
                    interior_coords = [
                        (interior_coords_raw[i], interior_coords_raw[i + 1])
                        for i in range(0, len(interior_coords_raw), 2)
                    ]
                    if interior_coords[0] != interior_coords[-1]:
                        interior_coords.append(interior_coords[0])
                    interior_rings.append(interior_coords)

            try:
                polygon_geom = (
                    Polygon(exterior_coords, interior_rings)
                    if interior_rings
                    else Polygon(exterior_coords)
                )

                if polygon_geom.is_valid:
                    parsed_polygons.append(polygon_geom)
                else:
                    _log(logger, "warning", "Invalid polygon geometry, attempting to fix")
                    fixed_geom = make_valid(polygon_geom)
                    if getattr(fixed_geom, "geom_type", None) in ["Polygon", "MultiPolygon"]:
                        parsed_polygons.append(fixed_geom)
            except Exception as e:
                _log(logger, "warning", f"Error creating polygon: {e}")
                continue

        if not parsed_polygons:
            return None

        final_geom = (
            parsed_polygons[0] if len(parsed_polygons) == 1 else MultiPolygon(parsed_polygons)
        )
        return wkt.dumps(final_geom)

    except Exception as e:
        _log(logger, "error", f"Error parsing geometry: {e}")
        return None


def parse_feature(
    feature_elem: ET.Element, year: int, logger: Any | None = None
) -> dict[str, Any] | None:
    """Parse a single Marker feature from XML."""
    try:
        feature_data = {"year": year}

        geom_elem = feature_elem.find(".//Jordbrugsanalyser:the_geom", NAMESPACES)
        if geom_elem is not None:
            geometry_wkt = parse_geometry(geom_elem, logger)
            if geometry_wkt:
                feature_data["geometry_wkt"] = geometry_wkt
            else:
                _log(logger, "warning", "Failed to parse geometry for feature")
                return None
        else:
            _log(logger, "warning", "No geometry element found for feature")
            return None

        for xml_field, (target_field, converter) in FIELD_MAPPING.items():
            elem = feature_elem.find(f".//Jordbrugsanalyser:{xml_field}", NAMESPACES)
            if elem is not None and elem.text:
                try:
                    raw_value = elem.text.strip()
                    if raw_value:
                        feature_data[target_field] = (
                            clean_text_value(raw_value)
                            if converter is str
                            else converter(raw_value)
                        )
                    else:
                        feature_data[target_field] = None
                except (ValueError, TypeError) as e:
                    _log(logger, "warning", f"Error converting field {xml_field}: {e}")
                    feature_data[target_field] = None
            else:
                feature_data[target_field] = None

        feature_data["processed_at"] = datetime.now()
        return feature_data

    except Exception as e:
        _log(logger, "error", f"Error parsing feature: {e}")
        return None


def parse_wfs_response(
    xml_content: str, year: int, logger: Any | None = None
) -> list[dict[str, Any]]:
    """Parse a WFS FeatureCollection XML response into structured feature dictionaries."""
    try:
        root = ET.fromstring(xml_content)
        layer_name = f"Marker{str(year)[-2:]}"
        features = root.findall(f".//Jordbrugsanalyser:{layer_name}", NAMESPACES)

        parsed_features = []
        for feature_elem in features:
            feature_data = parse_feature(feature_elem, year, logger)
            if feature_data:
                parsed_features.append(feature_data)

        _log(
            logger,
            "info",
            f"Parsed {len(parsed_features)} features from {len(features)} XML elements for year {year}",
        )
        return parsed_features

    except ET.ParseError as e:
        _log(logger, "error", f"XML parsing error for year {year}: {e}")
        return []
    except Exception as e:
        _log(logger, "error", f"Error parsing WFS response for year {year}: {e}")
        return []
