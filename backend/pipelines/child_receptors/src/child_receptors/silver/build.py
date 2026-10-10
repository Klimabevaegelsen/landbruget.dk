"""Build and validate the national child receptor layer from local bronze files."""

from __future__ import annotations

import csv
import json
import logging
import math
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from child_receptors.config import (
    ALLOWED_RECEPTOR_TYPES,
    DAGTILBUD_MAX_STALE_DAYS,
    DAYCARE_TYPES,
    KOMMUNE_CODES_BY_NAME,
    OSM_REGIONS,
    PLAYGROUND_DISTANCE_METERS,
    PLAYGROUND_PRIORITY,
    PRODUCTION_FLOORS,
    SOURCE_CRS,
    SOURCE_FILENAMES,
    STIL_TYPES,
)

_SPATIAL_CONNECTION: duckdb.DuckDBPyConnection | None = None
_DIGITS = re.compile(r"^\d+$")
_CVR_PATTERN = re.compile(r"^\d{8}$")
_P_NUMBER_PATTERN = re.compile(r"^\d{10}$")
DAGTILBUD_SEED_COMMAND = "cd backend/pipelines/child_receptors && python main.py --layer bronze --sources dagtilbud"


class DagtilbudBronzeError(ValueError):
    """Raised when no recent complete Dagtilbudsregisteret bronze is available."""


def _spatial_connection() -> duckdb.DuckDBPyConnection:
    global _SPATIAL_CONNECTION
    if _SPATIAL_CONNECTION is None:
        _SPATIAL_CONNECTION = duckdb.connect()
        _SPATIAL_CONNECTION.execute("LOAD spatial")
    return _SPATIAL_CONNECTION


def _field(row: dict[str, Any], *names: str) -> Any:
    """Find a source field ignoring underscore and case differences."""
    normalized = {re.sub(r"[^a-z0-9]", "", str(key).casefold()): value for key, value in row.items()}
    for name in names:
        value = normalized.get(re.sub(r"[^a-z0-9]", "", name.casefold()))
        if value is not None and str(value).strip() != "":
            return value
    return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(value: Any, field_name: str) -> str:
    text = _text(value)
    if text is None:
        raise ValueError(f"Missing required source identifier: {field_name}")
    return text


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _identifier(value: Any, digits: int, warnings: Counter | None = None, label: str = "") -> str | None:
    text = _text(value)
    if text is None:
        return None
    if not _DIGITS.fullmatch(text) or len(text) > digits:
        if warnings is not None:
            warnings[f"invalid_{label or digits}_format"] += 1
        return None
    return text.zfill(digits)


def _kommune(value: Any) -> str | None:
    text = _text(value)
    if text is None:
        return None
    digits = re.sub(r"\D", "", text)
    return digits.zfill(4) if digits else None


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source, delimiter=";"))


def _read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _point_rows(rows: list[dict[str, Any]], source_crs: str) -> list[dict[str, Any]]:
    """Add EPSG:25832 and WGS84 coordinates plus WKB using DuckDB spatial."""
    if not rows:
        return rows
    connection = _spatial_connection()
    coordinate_input = [
        {"row_index": index, "x": row.pop("_raw_x"), "y": row.pop("_raw_y")} for index, row in enumerate(rows)
    ]
    connection.execute("CREATE OR REPLACE TEMP TABLE _child_receptor_coords (row_index INTEGER, x DOUBLE, y DOUBLE)")
    connection.executemany(
        "INSERT INTO _child_receptor_coords VALUES (?, ?, ?)",
        [(item["row_index"], item["x"], item["y"]) for item in coordinate_input],
    )
    try:
        if source_crs == "EPSG:4326":
            geometry_sql = "ST_Transform(ST_Point(x, y), 'EPSG:4326', 'EPSG:25832', true)"
        elif source_crs == "EPSG:25832":
            geometry_sql = "ST_Point(x, y)"
        else:
            raise ValueError(f"Unsupported source CRS: {source_crs}")
        transformed = connection.execute(
            f"""
            WITH projected AS (
                SELECT row_index, {geometry_sql} AS geom
                FROM _child_receptor_coords
            ), geographic AS (
                SELECT row_index, geom,
                    ST_Transform(geom, 'EPSG:25832', 'EPSG:4326', true) AS wgs84
                FROM projected
            )
            SELECT row_index, ST_X(geom), ST_Y(geom), ST_X(wgs84), ST_Y(wgs84), ST_AsWKB(geom)
            FROM geographic ORDER BY row_index
            """
        ).fetchall()
    finally:
        connection.unregister("_child_receptor_coords")
    for index, utm_e, utm_n, lon, lat, geometry_wkb in transformed:
        rows[index].update(
            {
                "utm_e": float(utm_e),
                "utm_n": float(utm_n),
                "lon": float(lon),
                "lat": float(lat),
                "geometry": bytes(geometry_wkb),
            }
        )
    return rows


def _base_row(
    *,
    receptor_id: str,
    receptor_type: str,
    subtype: str,
    name: Any,
    ownership: Any,
    kommune_kode: Any,
    address: Any,
    dawa_id: Any,
    cvr: Any,
    p_number: Any,
    source: str,
    source_updated_at: Any,
    source_crs: str,
    fetch_timestamp: str | None,
    x: Any,
    y: Any,
    warnings: Counter | None = None,
) -> dict[str, Any] | None:
    raw_x = _number(x)
    raw_y = _number(y)
    if raw_x is None or raw_y is None:
        return None
    return {
        "receptor_id": receptor_id,
        "receptor_type": receptor_type,
        "subtype": subtype,
        "name": _text(name),
        "ownership": _text(ownership),
        "kommune_kode": _kommune(kommune_kode),
        "address": _text(address),
        "dawa_id": _text(dawa_id),
        "cvr": _identifier(cvr, 8, warnings, "cvr"),
        "p_number": _identifier(p_number, 10, warnings, "p_number"),
        "is_active": True,
        "source": source,
        "matched_sources": [source],
        "source_updated_at": _text(source_updated_at),
        "_fetch_timestamp": fetch_timestamp,
        "_source_crs": source_crs,
        "_raw_x": raw_x,
        "_raw_y": raw_y,
    }


def _join_address(row: dict[str, Any]) -> str | None:
    road = _field(row, "vejNavn", "vejnavn", "adresseVejnavn")
    number = _field(row, "husNummer", "husnr", "adresseHusnummer")
    postal = _field(row, "postNummer", "postnummer", "postnr")
    city = _field(row, "byNavn", "bynavn", "postdistrikt")
    road_part = " ".join(part for part in (_text(road), _text(number)) if part)
    postal_part = " ".join(part for part in (_text(postal), _text(city)) if part)
    return ", ".join(part for part in (road_part, postal_part) if part) or None


def parse_dagtilbud(
    anvisningsenhed_path: str | Path,
    daginstitution_path: str | Path,
    alternativ_adresse_path: str | Path | None = None,
    *,
    fetch_timestamp: str | None = None,
    dropped: Counter | None = None,
    warnings: Counter | None = None,
) -> list[dict[str, Any]]:
    """Parse active Dagtilbud sites and qualifying alternative addresses."""
    dropped = dropped if dropped is not None else Counter()
    warnings = warnings if warnings is not None else Counter()
    institutions = _read_csv(daginstitution_path)
    institution_by_id = {
        str(_field(row, "daginstitutionsNummer")): row
        for row in institutions
        if _field(row, "daginstitutionsNummer") is not None
    }
    active_rows: list[dict[str, Any]] = []
    for site in _read_csv(anvisningsenhed_path):
        if _text(_field(site, "aktivitetsstatus")) != "Aktiv":
            dropped["dagtilbud_inactive"] += 1
            continue
        raw_type = _text(_field(site, "instType3"))
        if raw_type == "6013":
            dropped["dagtilbud_dagpleje_excluded"] += 1
            continue
        if raw_type not in DAYCARE_TYPES:
            raise ValueError(f"Unknown active Dagtilbud instType3 code: {raw_type!r}")
        parent_id = _field(site, "daginstitutionsNummer", "daginstitutionNummer")
        parent = institution_by_id.get(str(parent_id)) if parent_id is not None else None
        ownership = _field(parent or {}, "ejerformKode_Tekst")
        if ownership is None:
            ownership = {"1": "Kommunal", "2": "Selvejende", "3": "Private"}.get(
                _text(_field(parent or {}, "ejerformKode")) or ""
            )
        receptor_type, subtype = DAYCARE_TYPES[raw_type]
        anvisnings_id = _required_text(_field(site, "anvisningsenhedsNummer"), "anvisningsenhedsNummer")
        row = _base_row(
            receptor_id=f"dtr:{anvisnings_id}",
            receptor_type=receptor_type,
            subtype=subtype,
            name=_field(site, "navn", "anvisningsenhedsNavn", "dagtilbudsNavn"),
            ownership=ownership,
            kommune_kode=_field(site, "kommuneKode", "kommunekode"),
            address=_join_address(site),
            dawa_id=_field(site, "dawaId", "dawa_id", "dawaUUID"),
            cvr=_field(site, "cvr", "cvrNummer", "CVRnummer"),
            p_number=_field(site, "pNumber", "pnummer", "pNummer"),
            source="dagtilbudsregister",
            source_updated_at=_field(site, "senestOpdateret"),
            source_crs=SOURCE_CRS["dagtilbud"],
            fetch_timestamp=fetch_timestamp,
            x=_field(site, "geoLaengde"),
            y=_field(site, "geoBredde"),
            warnings=warnings,
        )
        if row is None:
            dropped["dagtilbud_missing_coordinates"] += 1
            continue
        row["_parent_anvisningsenhedsnummer"] = anvisnings_id
        active_rows.append(row)

    rows = list(active_rows)
    if alternativ_adresse_path is not None and Path(alternativ_adresse_path).exists():
        by_parent = {row["_parent_anvisningsenhedsnummer"]: row for row in active_rows}
        for alternative in _read_csv(alternativ_adresse_path):
            parent_id = _text(_field(alternative, "anvisningsenhedsNummer"))
            parent = by_parent.get(parent_id)
            if parent is None:
                dropped["alternative_address_inactive_or_excluded_parent"] += 1
                continue
            alternative_id = _required_text(_field(alternative, "alternativAdresseNummer"), "alternativAdresseNummer")
            point_row = _base_row(
                receptor_id=f"dtr-alt:{alternative_id}",
                receptor_type=parent["receptor_type"],
                subtype=f"{parent['subtype']} – alternativ adresse",
                name=_field(alternative, "navn", "alternativAdresseNavn") or parent["name"],
                ownership=parent["ownership"],
                kommune_kode=_field(alternative, "kommuneKode", "kommunekode") or parent["kommune_kode"],
                address=_join_address(alternative),
                dawa_id=_field(alternative, "dawaId", "dawa_id", "dawaUUID"),
                cvr=parent["cvr"],
                p_number=parent["p_number"],
                source="dagtilbudsregister",
                source_updated_at=_field(alternative, "senestOpdateret"),
                source_crs=SOURCE_CRS["dagtilbud"],
                fetch_timestamp=fetch_timestamp,
                x=_field(alternative, "geoLaengde"),
                y=_field(alternative, "geoBredde"),
                warnings=warnings,
            )
            if point_row is None:
                dropped["dagtilbud_missing_coordinates"] += 1
                continue
            rows.append(point_row)
    for row in rows:
        row.pop("_parent_anvisningsenhedsnummer", None)
    return _point_rows(rows, SOURCE_CRS["dagtilbud"])


def parse_stil(
    path: str | Path,
    *,
    fetch_timestamp: str | None = None,
    dropped: Counter | None = None,
    warnings: Counter | None = None,
) -> list[dict[str, Any]]:
    """Parse active STIL school and boarding-school institutions."""
    dropped = dropped if dropped is not None else Counter()
    warnings = warnings if warnings is not None else Counter()
    institutions = _read_json(path)
    if not isinstance(institutions, list):
        raise ValueError("STIL institutions JSON must be a list")
    rows: list[dict[str, Any]] = []
    for institution in institutions:
        active_name = _text(_field(institution, "activeCodeName")) or ""
        if not active_name.startswith("Aktiv"):
            dropped["stil_inactive"] += 1
            continue
        try:
            type_id = int(_field(institution, "institutionTypeId"))
        except (TypeError, ValueError):
            type_id = -1
        receptor_type = STIL_TYPES.get(type_id)
        if receptor_type is None:
            dropped["stil_excluded_type"] += 1
            continue
        longitude = _number(_field(institution, "longitude"))
        latitude = _number(_field(institution, "latitude"))
        if longitude is None or latitude is None:
            dropped["stil_missing_coordinates"] += 1
            continue
        municipality = _text(_field(institution, "locationMunicipality"))
        if municipality and municipality.endswith(" Kommune"):
            municipality = municipality.removesuffix(" Kommune")
        kommune_kode = KOMMUNE_CODES_BY_NAME.get(municipality or "")
        if municipality and kommune_kode is None:
            warnings["unknown_stil_municipality"] += 1
        address = (
            ", ".join(
                part
                for part in (
                    _text(_field(institution, "address")),
                    " ".join(
                        part
                        for part in (
                            _text(_field(institution, "postalCode")),
                            _text(_field(institution, "postalDistrict")),
                        )
                        if part
                    ),
                )
                if part
            )
            or None
        )
        row = _base_row(
            receptor_id=f"stil:{_required_text(_field(institution, 'institutionNumber'), 'institutionNumber')}",
            receptor_type=receptor_type,
            subtype=_field(institution, "institutionType"),
            name=_field(institution, "institutionName"),
            ownership=_field(institution, "ownerCodeName"),
            kommune_kode=kommune_kode,
            address=address,
            dawa_id=None,
            cvr=_field(institution, "cvrNumber"),
            p_number=_field(institution, "pNumber"),
            source="stil_institutionsregister",
            source_updated_at=_field(institution, "updateDate"),
            source_crs=SOURCE_CRS["stil"],
            fetch_timestamp=fetch_timestamp,
            x=longitude,
            y=latitude,
            warnings=warnings,
        )
        if row is not None:
            rows.append(row)
    return _point_rows(rows, SOURCE_CRS["stil"])


def _read_bbr_records(path: str | Path) -> list[dict[str, Any]]:
    text = Path(path).read_text(encoding="utf-8")
    if not text.strip():
        return []
    if text.lstrip().startswith("["):
        values = json.loads(text)
        if not isinstance(values, list):
            raise ValueError("BBR data must be JSON Lines or a JSON list")
        return values
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def parse_bbr(
    path: str | Path,
    *,
    fetch_timestamp: str | None = None,
    dropped: Counter | None = None,
) -> list[dict[str, Any]]:
    """Keep the newest bitemporal BBR row per local id, then filter status."""
    dropped = dropped if dropped is not None else Counter()
    latest: dict[str, dict[str, Any]] = {}
    for item in _read_bbr_records(path):
        local_id = _text(_field(item, "id_lokalId"))
        if not local_id:
            dropped["bbr_missing_id"] += 1
            continue
        current = latest.get(local_id)
        key = (_text(_field(item, "registreringFra")) or "", _text(_field(item, "virkningFra")) or "")
        current_key = (
            (_text(_field(current, "registreringFra")) or "", _text(_field(current, "virkningFra")) or "")
            if current
            else None
        )
        if current is None or key > current_key:
            latest[local_id] = item

    rows: list[dict[str, Any]] = []
    for item in latest.values():
        status = _text(_field(item, "status"))
        if status not in {"6", "7"}:
            dropped["bbr_status_excluded"] += 1
            continue
        coordinate = _field(item, "tek109Koordinat") or {}
        wkt = _text(_field(coordinate, "wkt"))
        match = re.fullmatch(r"\s*POINT\s*\(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)\s*", wkt or "", re.IGNORECASE)
        if not match:
            dropped["bbr_missing_or_invalid_geometry"] += 1
            continue
        receptor_id = _required_text(_field(item, "id_lokalId"), "id_lokalId")
        row = _base_row(
            receptor_id=f"bbr:{receptor_id}",
            receptor_type="playground",
            subtype="Legeplads",
            name=None,
            ownership=None,
            kommune_kode=_field(item, "kommunekode"),
            address=None,
            dawa_id=None,
            cvr=None,
            p_number=None,
            source="bbr",
            source_updated_at=_field(item, "virkningFra"),
            source_crs=SOURCE_CRS["bbr"],
            fetch_timestamp=fetch_timestamp,
            x=match.group(1),
            y=match.group(2),
        )
        if row:
            rows.append(row)
    return _point_rows(rows, SOURCE_CRS["bbr"])


def parse_geofa(
    path: str | Path,
    *,
    fetch_timestamp: str | None = None,
    dropped: Counter | None = None,
) -> list[dict[str, Any]]:
    """Filter GeoFA playground features and use the first MultiPoint member."""
    dropped = dropped if dropped is not None else Counter()
    payload = _read_json(path)
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        raise ValueError("GeoFA input must be a GeoJSON FeatureCollection")
    rows: list[dict[str, Any]] = []
    for feature in features:
        props = feature.get("properties") or {}
        try:
            facility_type = int(_field(props, "facil_ty_k"))
            status_code = int(_field(props, "statuskode"))
        except (TypeError, ValueError):
            facility_type, status_code = -1, -1
        if facility_type not in {1031, 1041, 1211} or status_code != 3:
            dropped["geofa_type_or_status_excluded"] += 1
            continue
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        if geometry.get("type") == "Point":
            point = coordinates
        elif geometry.get("type") == "MultiPoint" and coordinates:
            point = coordinates[0]
        else:
            point = []
        if len(point) < 2:
            dropped["geofa_missing_or_invalid_geometry"] += 1
            continue
        object_id = _required_text(_field(props, "objekt_id"), "objekt_id")
        row = _base_row(
            receptor_id=f"geofa:{object_id}",
            receptor_type="playground",
            subtype=_field(props, "facil_ty") or "Legeplads",
            name=_field(props, "navn"),
            ownership=None,
            kommune_kode=_field(props, "kommunekode"),
            address=None,
            dawa_id=None,
            cvr=None,
            p_number=None,
            source="geofa",
            source_updated_at=_field(props, "senestOpdateret", "updateDate"),
            source_crs=SOURCE_CRS["geofa"],
            fetch_timestamp=fetch_timestamp,
            x=point[0],
            y=point[1],
        )
        if row:
            rows.append(row)
    return _point_rows(rows, SOURCE_CRS["geofa"])


def parse_osm(
    paths: list[str | Path] | tuple[str | Path, ...],
    *,
    fetch_timestamp: str | None = None,
    dropped: Counter | None = None,
) -> list[dict[str, Any]]:
    """Parse Overpass elements, filter private access, and remove regional repeats."""
    dropped = dropped if dropped is not None else Counter()
    elements_by_id: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted((Path(item) for item in paths), key=lambda value: value.name):
        payload = _read_json(path)
        elements = payload.get("elements") if isinstance(payload, dict) else None
        if not isinstance(elements, list):
            raise ValueError(f"OSM input {path} must contain an elements list")
        for item in elements:
            tags = item.get("tags") or {}
            if tags.get("leisure") != "playground":
                dropped["osm_not_playground"] += 1
                continue
            if tags.get("access") in {"private", "no"}:
                dropped["osm_private_access"] += 1
                continue
            identity = (str(item.get("type") or ""), str(item.get("id") or ""))
            if not identity[0] or not identity[1]:
                dropped["osm_missing_id"] += 1
                continue
            if identity in elements_by_id:
                dropped["osm_duplicate_region_element"] += 1
                continue
            elements_by_id[identity] = item

    rows: list[dict[str, Any]] = []
    for (element_type, element_id), item in sorted(elements_by_id.items()):
        center = item.get("center") or {}
        longitude = item.get("lon", center.get("lon"))
        latitude = item.get("lat", center.get("lat"))
        if _number(longitude) is None or _number(latitude) is None:
            dropped["osm_missing_coordinates"] += 1
            continue
        tags = item.get("tags") or {}
        row = _base_row(
            receptor_id=f"osm:{element_type}/{element_id}",
            receptor_type="playground",
            subtype="Legeplads (OSM)",
            name=tags.get("name"),
            ownership=None,
            kommune_kode=None,
            address=None,
            dawa_id=None,
            cvr=None,
            p_number=None,
            source="osm",
            source_updated_at=item.get("timestamp"),
            source_crs=SOURCE_CRS["osm"],
            fetch_timestamp=fetch_timestamp,
            x=longitude,
            y=latitude,
        )
        if row:
            rows.append(row)
    return _point_rows(rows, SOURCE_CRS["osm"])


def dedupe_playgrounds(
    rows: list[dict[str, Any]],
    *,
    distance_meters: float = PLAYGROUND_DISTANCE_METERS,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Greedy source-priority dedupe; every candidate joins its nearest kept point."""
    non_playgrounds = [row for row in rows if row["receptor_type"] != "playground"]
    candidates = [row for row in rows if row["receptor_type"] == "playground"]
    candidates.sort(key=lambda row: (PLAYGROUND_PRIORITY[row["source"]], row["receptor_id"]))
    kept: list[dict[str, Any]] = []
    grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    pair_counts: Counter = Counter()
    grid_size = distance_meters

    for candidate in candidates:
        east = float(candidate["utm_e"])
        north = float(candidate["utm_n"])
        cell = (math.floor(east / grid_size), math.floor(north / grid_size))
        nearest_index: int | None = None
        nearest_distance = distance_meters
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for kept_index in grid.get((cell[0] + dx, cell[1] + dy), []):
                    existing = kept[kept_index]
                    separation = math.hypot(east - existing["utm_e"], north - existing["utm_n"])
                    if separation <= nearest_distance and (separation < nearest_distance or nearest_index is None):
                        nearest_index = kept_index
                        nearest_distance = separation
        if nearest_index is None:
            kept_index = len(kept)
            kept.append(candidate)
            grid[cell].append(kept_index)
            continue

        target = kept[nearest_index]
        for existing_source in target["matched_sources"]:
            if existing_source != candidate["source"]:
                left, right = sorted((existing_source, candidate["source"]))
                pair_counts[f"{left}+{right}"] += 1
        target["matched_sources"] = sorted(set(target["matched_sources"]) | {candidate["source"]})
        if target["name"] is None and candidate["name"] is not None:
            target["name"] = candidate["name"]
    return non_playgrounds + kept, dict(sorted(pair_counts.items()))


def validate_rows(
    rows: list[dict[str, Any]],
    *,
    minimums: dict[str, int] | None = None,
    qa: dict[str, Any] | None = None,
    enforce_floors: bool = True,
) -> list[dict[str, Any]]:
    """Drop out-of-Denmark points, then enforce schema and national row floors."""
    qa = qa if qa is not None else {"dropped_rows": {}, "warnings": {}}
    drops: Counter = Counter(qa.setdefault("dropped_rows", {}))
    source_totals = Counter(row.get("source") for row in rows)
    outside_by_source: Counter = Counter()
    kept_rows: list[dict[str, Any]] = []
    for row in rows:
        x, y = row.get("utm_e"), row.get("utm_n")
        if x is None or y is None or not 400000 <= float(x) <= 900000 or not 6000000 <= float(y) <= 6420000:
            drops[f"outside_denmark_bbox:{row.get('source')}"] += 1
            outside_by_source[row.get("source")] += 1
            continue
        kept_rows.append(row)
    for source, count in outside_by_source.items():
        total = source_totals[source]
        if total and count / total > 0.005:
            raise ValueError(f"{source} has {count}/{total} rows outside the Denmark bbox (>0.5%)")

    missing_ids = [row for row in kept_rows if not row.get("receptor_id")]
    if missing_ids:
        raise ValueError("Rows are missing receptor_id")
    ids = [row.get("receptor_id") for row in kept_rows]
    duplicates = sorted(value for value, count in Counter(ids).items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate receptor_id values: {duplicates[:5]}")
    for row in kept_rows:
        if row.get("receptor_type") not in ALLOWED_RECEPTOR_TYPES:
            raise ValueError(f"Invalid receptor_type for {row.get('receptor_id')}: {row.get('receptor_type')!r}")
        if row.get("geometry") is None:
            raise ValueError(f"Missing geometry for {row.get('receptor_id')}")
        cvr = row.get("cvr")
        if cvr is not None and not _CVR_PATTERN.fullmatch(str(cvr)):
            raise ValueError(f"Invalid CVR format for {row.get('receptor_id')}: {cvr!r}")
        p_number = row.get("p_number")
        if p_number is not None and not _P_NUMBER_PATTERN.fullmatch(str(p_number)):
            raise ValueError(f"Invalid p_number format for {row.get('receptor_id')}: {p_number!r}")

    for reason, count in drops.items():
        qa["dropped_rows"][reason] = count
    if enforce_floors:
        floors = minimums or PRODUCTION_FLOORS
        counts = Counter(row["receptor_type"] for row in kept_rows)
        checks = {
            "daycare": counts["daycare"] >= floors.get("daycare", 0),
            "school": counts["school"] + counts["special_school"] + counts["boarding_school"]
            >= floors.get("school", 0),
            "playground": counts["playground"] >= floors.get("playground", 0),
        }
        if not all(checks.values()):
            failed = [key for key, passed in checks.items() if not passed]
            raise ValueError(f"Silver receptor row floors failed: {failed}; counts={dict(counts)}")
        daycares = {
            row["kommune_kode"] for row in kept_rows if row["receptor_type"] == "daycare" and row["kommune_kode"]
        }
        expected_kommunes = floors.get("daycare_kommune_count", 98)
        if len(daycares) < expected_kommunes:
            raise ValueError(f"Daycare kommune floor failed: {len(daycares)} < {expected_kommunes}")
    return kept_rows


def _source_run(source_dir: Path, timestamp: str | None) -> Path:
    if timestamp:
        run_dir = source_dir / timestamp
        if not run_dir.is_dir():
            raise FileNotFoundError(f"No bronze run at {run_dir}")
        return run_dir
    runs = sorted(path for path in source_dir.iterdir() if path.is_dir()) if source_dir.exists() else []
    if not runs:
        raise FileNotFoundError(f"No bronze timestamp directories in {source_dir}")
    return runs[-1]


def _complete_source_run(run_dir: Path, source: str, *, require_manifest: bool = False) -> bool:
    filenames = SOURCE_FILENAMES[source]
    if require_manifest:
        filenames = (*filenames, "manifest.json")
    return run_dir.is_dir() and all((run_dir / filename).is_file() for filename in filenames)


def _source_run_with_fallback(
    source_dir: Path,
    source: str,
    timestamp: str | None,
    *,
    require_manifest: bool = False,
) -> tuple[Path | None, str]:
    """Choose the requested/latest complete source run, falling back to an earlier complete run."""
    runs = sorted(path for path in source_dir.iterdir() if path.is_dir()) if source_dir.exists() else []
    target_timestamp = timestamp or (runs[-1].name if runs else None)
    if target_timestamp is None:
        return None, "missing"

    requested_run = source_dir / target_timestamp
    if _complete_source_run(requested_run, source, require_manifest=require_manifest):
        return requested_run, "fresh"

    earlier_runs = [
        run
        for run in runs
        if run.name < target_timestamp and _complete_source_run(run, source, require_manifest=require_manifest)
    ]
    if earlier_runs:
        return earlier_runs[-1], "stale"
    return None, "missing"


def _dagtilbud_age(manifest: dict[str, Any], now: datetime | None) -> tuple[int, bool]:
    fetch_timestamp = manifest.get("_fetch_timestamp")
    if not isinstance(fetch_timestamp, str) or not fetch_timestamp:
        raise DagtilbudBronzeError(
            "Dagtilbudsregisteret bronze manifest has no valid _fetch_timestamp. "
            f"Seed locally with: {DAGTILBUD_SEED_COMMAND}"
        )
    try:
        fetched_at = datetime.fromisoformat(fetch_timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DagtilbudBronzeError(
            "Dagtilbudsregisteret bronze manifest has an invalid _fetch_timestamp. "
            f"Seed locally with: {DAGTILBUD_SEED_COMMAND}"
        ) from exc
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    age = current_time.astimezone(UTC) - fetched_at.astimezone(UTC)
    return max(0, age.days), age > timedelta(days=DAGTILBUD_MAX_STALE_DAYS)


def _floor_requirements(minimums: dict[str, int] | None, osm_status: str) -> dict[str, int]:
    if minimums is not None:
        return minimums
    floors = dict(PRODUCTION_FLOORS)
    if osm_status == "missing":
        floors["playground"] = 9000
    return floors


def build_silver(
    bronze_root: str | Path,
    *,
    sources: tuple[str, ...] | list[str] = ("dagtilbud", "stil", "bbr", "geofa", "osm"),
    bronze_timestamp: str | None = None,
    now: datetime | None = None,
    minimums: dict[str, int] | None = None,
    enforce_floors: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read each source's latest (or requested) bronze run and return rows plus QA."""
    bronze_root = Path(bronze_root)
    records: list[dict[str, Any]] = []
    dropped: Counter = Counter()
    warnings: Counter = Counter()
    run_dirs: dict[str, Path] = {}
    osm_run_status = "missing"
    osm_run_timestamp: str | None = None
    osm_regions: list[str] = []
    dagtilbud_run_status = "missing"
    dagtilbud_run_timestamp: str | None = None
    dagtilbud_age_days: int | None = None
    for source in sources:
        if source == "osm":
            run_dir, osm_run_status = _source_run_with_fallback(bronze_root / source, source, bronze_timestamp)
            if run_dir is None:
                continue
            osm_run_timestamp = run_dir.name
            osm_regions = list(OSM_REGIONS)
        elif source == "dagtilbud":
            run_dir, dagtilbud_run_status = _source_run_with_fallback(
                bronze_root / source,
                source,
                bronze_timestamp,
                require_manifest=True,
            )
            if run_dir is None:
                raise DagtilbudBronzeError(
                    "No complete Dagtilbudsregisteret bronze run is available. "
                    f"Seed locally with: {DAGTILBUD_SEED_COMMAND}"
                )
            dagtilbud_run_timestamp = run_dir.name
        else:
            run_dir = _source_run(bronze_root / source, bronze_timestamp)
        run_dirs[source] = run_dir
        manifest_path = run_dir / "manifest.json"
        manifest: dict[str, Any] = {}
        if manifest_path.is_file() or source != "osm":
            try:
                loaded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                if source == "dagtilbud":
                    raise DagtilbudBronzeError(
                        "Dagtilbudsregisteret bronze manifest cannot be read. "
                        f"Seed locally with: {DAGTILBUD_SEED_COMMAND}"
                    ) from exc
                raise
            if not isinstance(loaded_manifest, dict):
                if source == "dagtilbud":
                    raise DagtilbudBronzeError(
                        f"Dagtilbudsregisteret bronze manifest is invalid. Seed locally with: {DAGTILBUD_SEED_COMMAND}"
                    )
            else:
                manifest = loaded_manifest
        fetch_timestamp = manifest.get("_fetch_timestamp")
        if source == "dagtilbud":
            dagtilbud_age_days, dagtilbud_is_over_limit = _dagtilbud_age(manifest, now)
            if dagtilbud_is_over_limit:
                raise DagtilbudBronzeError(
                    f"Dagtilbudsregisteret bronze is {dagtilbud_age_days} days old; "
                    f"the maximum allowed age is {DAGTILBUD_MAX_STALE_DAYS} days. "
                    f"Seed locally with: {DAGTILBUD_SEED_COMMAND}"
                )
            records.extend(
                parse_dagtilbud(
                    run_dir / "dagtilbudsregister_anvisningsenhed.csv",
                    run_dir / "dagtilbudsregister_daginstitution.csv",
                    run_dir / "dagtilbudsregister_alternativ_adresse.csv",
                    fetch_timestamp=fetch_timestamp,
                    dropped=dropped,
                    warnings=warnings,
                )
            )
        elif source == "stil":
            records.extend(
                parse_stil(
                    run_dir / "institutions.json", fetch_timestamp=fetch_timestamp, dropped=dropped, warnings=warnings
                )
            )
        elif source == "bbr":
            records.extend(
                parse_bbr(run_dir / "bbr_tekniskanlaeg_1905.jsonl", fetch_timestamp=fetch_timestamp, dropped=dropped)
            )
        elif source == "geofa":
            records.extend(
                parse_geofa(run_dir / "geofa_t5800_fac_pkt.json", fetch_timestamp=fetch_timestamp, dropped=dropped)
            )
        elif source == "osm":
            files = sorted(run_dir.glob("osm_playgrounds_*.json"))
            records.extend(parse_osm(files, fetch_timestamp=fetch_timestamp, dropped=dropped))
        else:
            raise ValueError(f"Unknown child receptor source: {source}")
        for row in records:
            if row["source"] in {"dagtilbudsregister", "stil_institutionsregister"} or (
                row["source"] in {"bbr", "geofa", "osm"} and row.get("_fetch_timestamp") == fetch_timestamp
            ):
                row["_source_crs"] = manifest.get("_source_crs", SOURCE_CRS[source])

    source_totals = Counter(row["source"] for row in records)
    qa: dict[str, Any] = {
        "dropped_rows": dict(dropped),
        "warnings": dict(warnings),
        "source_runs": {k: v.name for k, v in run_dirs.items()},
        "osm": {
            "status": osm_run_status,
            "bronze_timestamp": osm_run_timestamp,
            "regions": osm_regions,
        },
    }
    if "dagtilbud" in sources:
        qa["dagtilbud"] = {
            "status": dagtilbud_run_status,
            "bronze_timestamp": dagtilbud_run_timestamp,
            "age_days": dagtilbud_age_days,
        }
    checked = validate_rows(records, minimums=minimums, qa=qa, enforce_floors=False)
    deduped, dedupe_stats = dedupe_playgrounds(checked)
    floor_requirements = _floor_requirements(minimums, osm_run_status)
    final = (
        validate_rows(deduped, minimums=floor_requirements, qa=qa, enforce_floors=True) if enforce_floors else deduped
    )
    qa["dedupe_stats"] = {
        "merged_pairs_by_source": dedupe_stats,
        "playground_rows_before": sum(row["receptor_type"] == "playground" for row in checked),
        "playground_rows_after": sum(row["receptor_type"] == "playground" for row in final),
    }
    counts: dict[str, dict[str, dict[str, int]]] = {}
    for row in final:
        source_bucket = counts.setdefault(row["source"], {})
        type_bucket = source_bucket.setdefault(row["receptor_type"], {})
        kommune = row["kommune_kode"] or "NULL"
        type_bucket[kommune] = type_bucket.get(kommune, 0) + 1
    qa["counts_by_source_receptor_type_kommune"] = counts
    qa["row_count"] = len(final)
    qa["source_row_counts_before_dedupe"] = dict(source_totals)
    qa["dropped_rows"] = dict(sorted(qa["dropped_rows"].items()))
    qa["warnings"] = dict(sorted(qa["warnings"].items()))
    return final, qa


def write_parquet(rows: list[dict[str, Any]], path: str | Path) -> Path:
    """Write GeoParquet with a GEOMETRY column and EPSG:25832 metadata."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    connection = _spatial_connection()
    values = [
        {key: value for key, value in row.items() if key != "geometry"} | {"_geometry_wkb": row["geometry"]}
        for row in rows
    ]
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE _child_receptor_output (
            receptor_id VARCHAR, receptor_type VARCHAR, subtype VARCHAR, name VARCHAR,
            ownership VARCHAR, kommune_kode VARCHAR, address VARCHAR, dawa_id VARCHAR,
            cvr VARCHAR, p_number VARCHAR, is_active BOOLEAN, source VARCHAR,
            matched_sources VARCHAR[], source_updated_at VARCHAR, utm_e DOUBLE, utm_n DOUBLE,
            lon DOUBLE, lat DOUBLE, _fetch_timestamp VARCHAR, _source_crs VARCHAR,
            _geometry_wkb BLOB
        )
        """
    )
    output_fields = (
        "receptor_id",
        "receptor_type",
        "subtype",
        "name",
        "ownership",
        "kommune_kode",
        "address",
        "dawa_id",
        "cvr",
        "p_number",
        "is_active",
        "source",
        "matched_sources",
        "source_updated_at",
        "utm_e",
        "utm_n",
        "lon",
        "lat",
        "_fetch_timestamp",
        "_source_crs",
        "_geometry_wkb",
    )
    placeholders = ", ".join("?" for _ in output_fields)
    connection.executemany(
        f"INSERT INTO _child_receptor_output VALUES ({placeholders})",
        [tuple(row.get(field) for field in output_fields) for row in values],
    )
    try:
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE _typed_child_receptors AS
            SELECT
                CAST(receptor_id AS VARCHAR) AS receptor_id,
                CAST(receptor_type AS VARCHAR) AS receptor_type,
                CAST(subtype AS VARCHAR) AS subtype,
                CAST(name AS VARCHAR) AS name,
                CAST(ownership AS VARCHAR) AS ownership,
                CAST(kommune_kode AS VARCHAR) AS kommune_kode,
                CAST(address AS VARCHAR) AS address,
                CAST(dawa_id AS VARCHAR) AS dawa_id,
                CAST(cvr AS VARCHAR) AS cvr,
                CAST(p_number AS VARCHAR) AS p_number,
                CAST(is_active AS BOOLEAN) AS is_active,
                CAST(source AS VARCHAR) AS source,
                CAST(matched_sources AS VARCHAR[]) AS matched_sources,
                CAST(source_updated_at AS VARCHAR) AS source_updated_at,
                CAST(utm_e AS DOUBLE) AS utm_e,
                CAST(utm_n AS DOUBLE) AS utm_n,
                CAST(lon AS DOUBLE) AS lon,
                CAST(lat AS DOUBLE) AS lat,
                ST_GeomFromWKB(_geometry_wkb) AS geometry,
                CAST(_fetch_timestamp AS VARCHAR) AS _fetch_timestamp,
                CAST(_source_crs AS VARCHAR) AS _source_crs
            FROM _child_receptor_output
            """
        )
        escaped_path = str(destination).replace("'", "''")
        connection.execute(f"COPY _typed_child_receptors TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        connection.execute("DROP TABLE IF EXISTS _child_receptor_output")
    try:
        from common.storage.core import _patch_geoparquet_crs

        _patch_geoparquet_crs(str(destination), "EPSG:25832", logging.getLogger("child_receptors"))
    except ImportError:
        # DuckDB writes GeoParquet metadata; the common helper only injects CRS PROJJSON.
        pass
    return destination


__all__ = [
    "build_silver",
    "dedupe_playgrounds",
    "parse_bbr",
    "parse_dagtilbud",
    "parse_geofa",
    "parse_osm",
    "parse_stil",
    "validate_rows",
    "write_parquet",
]
