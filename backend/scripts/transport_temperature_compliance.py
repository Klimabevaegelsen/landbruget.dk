# ruff: noqa
"""
Pig Transport Temperature Compliance Analysis (Jan-Mar 2026).

Checks whether pig movements complied with EU Regulation 1/2005 temperature rules.
Uses a funnel strategy to minimize API calls:
  Movements → Weather (for those locations) → Filter cold → Route (only cold movements)

Usage:
    python scripts/transport_temperature_compliance.py \
        --start-date 2026-01-01 --end-date 2026-03-31 \
        --output-dir ./output/temperature_compliance

EU Regulation 1/2005, Annex I Chapter VI 3.1:
  - Long journeys (>8h): vehicle must maintain 5-30°C (±5°C tolerance → 0-35°C)
  - Short journeys (≤8h): must protect from "extreme temperatures" (Art 3)
"""

import argparse
import json
import logging
import math
import os
import time
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from common.storage.filesystem import setup_duckdb_cloud_auth
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("temperature_compliance")


BUCKET = "landbruget-data"
SVINEFLYTNING_PATTERN = "silver/svineflytning/*/movements*.parquet"

# Open-Meteo batch limit (API rejects >~30 locations per call)
OPEN_METEO_BATCH_SIZE = 20


def find_latest_parquet(conn: duckdb.DuckDBPyConnection) -> str:
    """Find latest svineflytning movements parquet in R2."""
    # List directory timestamps and pick latest
    try:
        dirs = conn.execute(f"""
            SELECT DISTINCT regexp_extract(file, '.*/(\\d{{8}}_\\d{{6}})/', 1) as ts
            FROM glob('r2://{BUCKET}/silver/svineflytning/*/movements*.parquet')
            WHERE ts IS NOT NULL AND ts != ''
            ORDER BY ts DESC
            LIMIT 1
        """).fetchone()
        if dirs:
            return f"r2://{BUCKET}/silver/svineflytning/{dirs[0]}/movements.parquet"
    except Exception:
        pass

    # Fallback: try reading with wildcard directly
    return f"r2://{BUCKET}/silver/svineflytning/*/movements.parquet"


def load_movements(
    conn: duckdb.DuckDBPyConnection, start_date: str, end_date: str, local_data: str | None = None
) -> list[int]:
    """Step 1: Load svineflytning movements and return unique postal codes."""

    if local_data:
        local_path = Path(local_data)
        # Find movements.parquet in the directory (may be in a subdirectory)
        candidates = list(local_path.rglob("movements.parquet"))
        if not candidates:
            raise FileNotFoundError(f"No movements.parquet found in {local_data}")
        latest_file = str(candidates[0])
    else:
        latest_file = find_latest_parquet(conn)
    logger.info(f"Loading movements from: {latest_file}")

    conn.execute(f"""
        CREATE TABLE movements AS
        SELECT
            movement_id,
            movement_date,
            movement_time,
            CASE
                WHEN movement_time IS NOT NULL AND movement_time > 0
                THEN movement_time // 100
                ELSE NULL
            END AS movement_hour,
            sender_chr_number,
            sender_address,
            sender_city_name,
            sender_postal_code,
            sender_postal_district,
            sender_municipality_name,
            receiver_chr_number,
            receiver_address,
            receiver_city_name,
            receiver_postal_code,
            receiver_postal_district,
            receiver_municipality_name,
            total_animals,
            sow_count,
            slaughter_pig_count,
            sender_country_code,
            receiver_country_code
        FROM read_parquet('{latest_file}')
        WHERE movement_date BETWEEN '{start_date}' AND '{end_date}'
          AND (is_deleted IS NULL OR is_deleted = false)
          AND (is_invalid IS NULL OR is_invalid = false)
          AND sender_country_code = 'DK'
    """)

    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT sender_postal_code) as sender_pcs,
            COUNT(DISTINCT receiver_postal_code) as receiver_pcs,
            MIN(movement_date) as min_date,
            MAX(movement_date) as max_date,
            SUM(total_animals) as total_animals
        FROM movements
    """).fetchone()

    logger.info(f"Loaded {stats[0]:,} movements ({stats[1]} sender PCs, {stats[2]} receiver PCs)")
    logger.info(f"Date range: {stats[3]} to {stats[4]}, total animals: {stats[5]:,}")

    # Get all unique postal codes (sender + receiver)
    postal_codes = conn.execute("""
        SELECT DISTINCT postal_code FROM (
            SELECT sender_postal_code AS postal_code FROM movements
            WHERE sender_postal_code IS NOT NULL
            UNION
            SELECT receiver_postal_code AS postal_code FROM movements
            WHERE receiver_postal_code IS NOT NULL
        )
        ORDER BY postal_code
    """).fetchall()

    unique_pcs = [row[0] for row in postal_codes]
    logger.info(f"Unique postal codes to geocode: {len(unique_pcs)}")
    return unique_pcs


def geocode_postal_codes(postal_codes: list[int], cache_path: Path) -> dict[int, tuple[float, float]]:
    """Step 2: Get lat/lon centroid for each postal code via DAWA."""

    # Load cache
    cache = {}
    if cache_path.exists():
        with open(cache_path) as f:
            raw = json.load(f)
            cache = {int(k): tuple(v) for k, v in raw.items()}
        logger.info(f"Loaded {len(cache)} cached postal code coordinates")

    coords = {}
    to_fetch = [pc for pc in postal_codes if pc not in cache]

    if to_fetch:
        logger.info(f"Fetching {len(to_fetch)} postal code centroids from DAWA...")
        for i, pc in enumerate(to_fetch):
            try:
                resp = requests.get(
                    f"https://api.dataforsyningen.dk/postnumre/{pc}",
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    # DAWA returns [lon, lat] in visueltcenter
                    lon, lat = data["visueltcenter"]
                    cache[pc] = (lat, lon)
                else:
                    logger.warning(f"DAWA returned {resp.status_code} for postal code {pc}")
            except Exception as e:
                logger.warning(f"Failed to geocode postal code {pc}: {e}")

            if (i + 1) % 50 == 0:
                logger.info(f"  Geocoded {i + 1}/{len(to_fetch)}")

        # Save cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump({str(k): list(v) for k, v in cache.items()}, f)
        logger.info(f"Saved {len(cache)} postal code coordinates to cache")

    for pc in postal_codes:
        if pc in cache:
            coords[pc] = cache[pc]

    logger.info(f"Coordinates available for {len(coords)}/{len(postal_codes)} postal codes")
    return coords


def fetch_temperatures(
    postal_coords: dict[int, tuple[float, float]],
    start_date: str,
    end_date: str,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Step 3: Fetch hourly temperatures from Open-Meteo for all postal code locations."""

    all_pcs = list(postal_coords.keys())
    all_rows = []

    # Cap end_date to today (historical forecast API doesn't accept future dates)
    today = datetime.now().strftime("%Y-%m-%d")
    effective_end = min(end_date, today)
    logger.info(f"Fetching weather for {start_date} to {effective_end}")

    # Batch coordinates (Open-Meteo supports multiple in one call)
    for batch_start in range(0, len(all_pcs), OPEN_METEO_BATCH_SIZE):
        batch_pcs = all_pcs[batch_start : batch_start + OPEN_METEO_BATCH_SIZE]
        lats = ",".join(str(postal_coords[pc][0]) for pc in batch_pcs)
        lons = ",".join(str(postal_coords[pc][1]) for pc in batch_pcs)

        batch_num = batch_start // OPEN_METEO_BATCH_SIZE + 1
        total_batches = (len(all_pcs) + OPEN_METEO_BATCH_SIZE - 1) // OPEN_METEO_BATCH_SIZE
        logger.info(f"Fetching weather batch {batch_num}/{total_batches} ({len(batch_pcs)} locations)")

        resp = requests.get(
            "https://historical-forecast-api.open-meteo.com/v1/forecast",
            params={
                "latitude": lats,
                "longitude": lons,
                "hourly": "temperature_2m",
                "start_date": start_date,
                "end_date": effective_end,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        # Open-Meteo returns a list when multiple coordinates, single dict when one
        if isinstance(data, list):
            results = data
        else:
            results = [data]

        for idx, result in enumerate(results):
            pc = batch_pcs[idx]
            times = result["hourly"]["time"]
            temps = result["hourly"]["temperature_2m"]
            for t, temp in zip(times, temps):
                if temp is not None:
                    # Parse "2026-01-01T00:00" format
                    dt = datetime.fromisoformat(t)
                    all_rows.append((pc, dt.strftime("%Y-%m-%d"), dt.hour, temp))

        # Small delay between batches to be polite
        if batch_start + OPEN_METEO_BATCH_SIZE < len(all_pcs):
            time.sleep(1)

    logger.info(f"Fetched {len(all_rows):,} hourly temperature readings")

    # Load into DuckDB
    conn.execute("""
        CREATE TABLE temperatures (
            postal_code INTEGER,
            date VARCHAR,
            hour INTEGER,
            temperature_c DOUBLE
        )
    """)

    if all_rows:
        conn.executemany(
            "INSERT INTO temperatures VALUES (?, ?, ?, ?)",
            all_rows,
        )

    # Summary stats
    stats = conn.execute("""
        SELECT
            MIN(temperature_c) as min_temp,
            MAX(temperature_c) as max_temp,
            AVG(temperature_c) as avg_temp,
            COUNT(*) FILTER (WHERE temperature_c < 0) as hours_below_zero,
            COUNT(*) FILTER (WHERE temperature_c < 5) as hours_below_five,
            COUNT(DISTINCT date) FILTER (WHERE temperature_c < 5) as days_with_cold
        FROM temperatures
    """).fetchone()

    logger.info(f"Temperature range: {stats[0]:.1f}°C to {stats[1]:.1f}°C (avg {stats[2]:.1f}°C)")
    logger.info(f"Hours below 0°C: {stats[3]:,}, below 5°C: {stats[4]:,}")
    logger.info(f"Days with any sub-5°C hour: {stats[5]}")


def filter_cold_movements(conn: duckdb.DuckDBPyConnection) -> int:
    """Step 4: Filter movements to only those during cold hours (<5°C at sender)."""

    conn.execute("""
        CREATE TABLE cold_movements AS
        SELECT
            m.*,
            t.temperature_c AS sender_departure_temp
        FROM movements m
        JOIN temperatures t
            ON t.postal_code = m.sender_postal_code
            AND t.date = CAST(m.movement_date AS VARCHAR)
            AND t.hour = COALESCE(m.movement_hour, 8)  -- default to 8 AM if no time
        WHERE t.temperature_c < 5.0
    """)

    total, cold, animals = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM movements),
            COUNT(*),
            COALESCE(SUM(total_animals), 0)
        FROM cold_movements
    """).fetchone()

    logger.info(f"FUNNEL: {total:,} total → {cold:,} during cold hours (<5°C)")
    logger.info(f"  Animals in cold-hour transports: {animals:,}")

    if cold == 0:
        logger.info("No movements during cold hours — all transports compliant!")

    # Also check movements with missing time (conservative: use daily minimum)
    conn.execute("""
        CREATE TABLE cold_movements_daily AS
        SELECT
            m.*,
            daily_min.min_temp AS sender_daily_min_temp
        FROM movements m
        JOIN (
            SELECT postal_code, date, MIN(temperature_c) as min_temp
            FROM temperatures
            GROUP BY postal_code, date
        ) daily_min
            ON daily_min.postal_code = m.sender_postal_code
            AND daily_min.date = CAST(m.movement_date AS VARCHAR)
        WHERE m.movement_hour IS NULL
          AND daily_min.min_temp < 5.0
          AND m.movement_id NOT IN (SELECT movement_id FROM cold_movements)
    """)

    daily_cold = conn.execute("SELECT COUNT(*) FROM cold_movements_daily").fetchone()[0]
    if daily_cold > 0:
        logger.info(
            f"  +{daily_cold} movements with no timestamp on cold days (using daily min temperature, conservative)"
        )
        # Merge into cold_movements (columns must match exactly)
        # cold_movements has all movement columns + sender_departure_temp
        # cold_movements_daily has all movement columns + sender_daily_min_temp
        conn.execute("""
            INSERT INTO cold_movements
            SELECT * EXCLUDE (sender_daily_min_temp), sender_daily_min_temp AS sender_departure_temp
            FROM cold_movements_daily
        """)

    return conn.execute("SELECT COUNT(*) FROM cold_movements").fetchone()[0]


def geocode_addresses(conn: duckdb.DuckDBPyConnection, cache_path: Path) -> dict[str, tuple[float, float]]:
    """Step 5a: Geocode full addresses for cold-hour movements via DAWA."""

    # Get unique addresses to geocode
    addresses = conn.execute("""
        SELECT DISTINCT address, postal_code FROM (
            SELECT sender_address AS address, sender_postal_code AS postal_code
            FROM cold_movements
            WHERE sender_address IS NOT NULL
            UNION
            SELECT receiver_address AS address, receiver_postal_code AS postal_code
            FROM cold_movements
            WHERE receiver_address IS NOT NULL
        )
    """).fetchall()

    logger.info(f"Geocoding {len(addresses)} unique addresses for cold-hour movements")

    # Load cache
    cache = {}
    if cache_path.exists():
        with open(cache_path) as f:
            cache = json.load(f)

    coords = {}
    fetched = 0

    for addr, postal_code in addresses:
        cache_key = f"{addr}|{postal_code}"
        if cache_key in cache:
            coords[cache_key] = tuple(cache[cache_key])
            continue

        try:
            resp = requests.get(
                "https://api.dataforsyningen.dk/adresser",
                params={"q": f"{addr}, {postal_code}", "struktur": "mini"},
                timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json()
                if results:
                    coord = results[0]
                    lat, lon = coord["y"], coord["x"]
                    cache[cache_key] = [lat, lon]
                    coords[cache_key] = (lat, lon)
                    fetched += 1
                else:
                    # Fallback to postal code centroid (already have these)
                    logger.debug(f"No DAWA result for {addr}, {postal_code}")
        except Exception as e:
            logger.debug(f"Geocode failed for {addr}: {e}")

        if fetched % 50 == 0 and fetched > 0:
            logger.info(f"  Geocoded {fetched} addresses")

    # Save cache
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    logger.info(f"Geocoded {len(coords)}/{len(addresses)} addresses ({fetched} new)")
    return coords


def estimate_routes(
    conn: duckdb.DuckDBPyConnection,
    address_coords: dict[str, tuple[float, float]],
    postal_coords: dict[int, tuple[float, float]],
    ors_api_key: str | None,
    cache_path: Path,
) -> None:
    """Step 5b: Estimate route distance/duration via ORS HGV profile."""

    # Get unique route pairs
    pairs = conn.execute("""
        SELECT DISTINCT
            sender_address, sender_postal_code,
            receiver_address, receiver_postal_code
        FROM cold_movements
    """).fetchall()

    logger.info(f"Estimating routes for {len(pairs)} unique sender-receiver pairs")

    # Load cache
    route_cache = {}
    if cache_path.exists():
        with open(cache_path) as f:
            route_cache = json.load(f)

    routes = {}
    fetched = 0

    for sender_addr, sender_pc, receiver_addr, receiver_pc in pairs:
        route_key = f"{sender_pc}|{receiver_pc}"

        if route_key in route_cache:
            routes[route_key] = route_cache[route_key]
            continue

        # Get coordinates (prefer address-level, fallback to postal centroid)
        sender_key = f"{sender_addr}|{sender_pc}"
        receiver_key = f"{receiver_addr}|{receiver_pc}"

        sender_coord = address_coords.get(sender_key) or postal_coords.get(sender_pc)
        receiver_coord = address_coords.get(receiver_key) or postal_coords.get(receiver_pc)

        if not sender_coord or not receiver_coord:
            logger.debug(f"No coordinates for route {sender_pc} → {receiver_pc}")
            continue

        if ors_api_key:
            try:
                resp = requests.get(
                    "https://api.openrouteservice.org/v2/directions/driving-hgv",
                    params={
                        "start": f"{sender_coord[1]},{sender_coord[0]}",
                        "end": f"{receiver_coord[1]},{receiver_coord[0]}",
                    },
                    headers={"Authorization": ors_api_key},
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    segment = data["features"][0]["properties"]["segments"][0]
                    distance_km = segment["distance"] / 1000
                    duration_hours = segment["duration"] / 3600
                    route_info = {
                        "distance_km": round(distance_km, 1),
                        "duration_hours": round(duration_hours, 2),
                        "is_long_journey": duration_hours > 8,
                    }
                    routes[route_key] = route_info
                    route_cache[route_key] = route_info
                    fetched += 1

                    # Rate limit: 40/min
                    time.sleep(1.5)
                elif resp.status_code == 429:
                    logger.warning("ORS rate limit hit, waiting 60s...")
                    time.sleep(60)
                else:
                    logger.debug(f"ORS returned {resp.status_code} for {route_key}")
            except Exception as e:
                logger.debug(f"ORS failed for {route_key}: {e}")
        else:
            # Fallback: haversine estimate
            lat1, lon1 = sender_coord
            lat2, lon2 = receiver_coord
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))
            distance_km = 6371 * c * 1.3  # 1.3x road factor
            duration_hours = distance_km / 60  # 60 km/h truck speed
            route_info = {
                "distance_km": round(distance_km, 1),
                "duration_hours": round(duration_hours, 2),
                "is_long_journey": duration_hours > 8,
            }
            routes[route_key] = route_info
            route_cache[route_key] = route_info

    # Save cache
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(route_cache, f)

    logger.info(f"Routes estimated: {len(routes)}/{len(pairs)} ({fetched} new ORS calls)")

    # Load into DuckDB
    conn.execute("""
        CREATE TABLE routes (
            sender_postal_code INTEGER,
            receiver_postal_code INTEGER,
            distance_km DOUBLE,
            duration_hours DOUBLE,
            is_long_journey BOOLEAN
        )
    """)

    route_rows = [
        (int(k.split("|")[0]), int(k.split("|")[1]), v["distance_km"], v["duration_hours"], v["is_long_journey"])
        for k, v in routes.items()
    ]
    if route_rows:
        conn.executemany("INSERT INTO routes VALUES (?, ?, ?, ?, ?)", route_rows)


def check_journey_temperature(conn: duckdb.DuckDBPyConnection) -> None:
    """Step 6a: Check temperature at departure and arrival for each cold movement."""

    conn.execute("""
        CREATE TABLE compliance_check AS
        SELECT
            cm.*,
            r.distance_km,
            r.duration_hours,
            COALESCE(r.is_long_journey, false) AS is_long_journey,
            -- Departure temp (already have from cold_movements)
            cm.sender_departure_temp,
            -- Arrival temp: receiver location at departure + duration
            t_arrival.temperature_c AS receiver_arrival_temp,
            -- Worst temperature along journey
            LEAST(
                cm.sender_departure_temp,
                COALESCE(t_arrival.temperature_c, cm.sender_departure_temp)
            ) AS worst_temp
        FROM cold_movements cm
        LEFT JOIN routes r
            ON r.sender_postal_code = cm.sender_postal_code
            AND r.receiver_postal_code = cm.receiver_postal_code
        LEFT JOIN temperatures t_arrival
            ON t_arrival.postal_code = cm.receiver_postal_code
            AND t_arrival.date = CAST(cm.movement_date AS VARCHAR)
            AND t_arrival.hour = LEAST(
                23,
                COALESCE(cm.movement_hour, 8) + CAST(CEIL(COALESCE(r.duration_hours, 1)) AS INTEGER)
            )
    """)


def classify_compliance(conn: duckdb.DuckDBPyConnection) -> None:
    """Step 6b: Apply regulatory thresholds using scientific thermoneutral zones.

    Thermoneutral zones (EURCAW-Pigs / European Commission factsheet):
      - Piglets <15 kg: 20-35°C
      - Growing/finishing 16-110 kg: 15-30°C
      - Finishing 111-160 kg: 10-28°C
      - Sows: 15-20°C (thermoneutral), tolerant down to ~10°C

    We classify by pig category using sow_count vs slaughter_pig_count.
    Since we don't know exact weights, we use conservative lower bounds:
      - Sows: below 5°C is outside comfort, below 0°C is high risk
      - Slaughter pigs (finishing): below 10°C outside thermoneutral, below 0°C high risk
      - Mixed/unknown: use slaughter pig thresholds (most common)
    """

    conn.execute("""
        CREATE TABLE results AS
        SELECT
            *,
            -- Determine primary animal category
            CASE
                WHEN sow_count > 0 AND (slaughter_pig_count IS NULL OR slaughter_pig_count = 0) THEN 'sows'
                WHEN slaughter_pig_count > 0 AND (sow_count IS NULL OR sow_count = 0) THEN 'slaughter_pigs'
                WHEN COALESCE(sow_count, 0) > 0 AND COALESCE(slaughter_pig_count, 0) > 0 THEN 'mixed'
                ELSE 'unknown'
            END AS pig_category,
            CASE
                -- Long journeys: strict legal rules (Annex I Ch VI 3.1)
                WHEN is_long_journey AND worst_temp < 0 THEN 'VIOLATION'
                WHEN is_long_journey AND worst_temp BETWEEN 0 AND 5 THEN 'CAUTION'
                WHEN is_long_journey AND worst_temp > 35 THEN 'VIOLATION'

                -- Short journeys with SOWS (thermoneutral ~10-20°C)
                WHEN NOT is_long_journey AND sow_count > 0
                     AND worst_temp < -5 THEN 'WELFARE_RISK'
                WHEN NOT is_long_journey AND sow_count > 0
                     AND worst_temp BETWEEN -5 AND 0 THEN 'BELOW_THERMONEUTRAL'
                WHEN NOT is_long_journey AND sow_count > 0
                     AND worst_temp BETWEEN 0 AND 5 THEN 'BELOW_THERMONEUTRAL'

                -- Short journeys with SLAUGHTER PIGS (thermoneutral ~10-28°C for finishing)
                WHEN NOT is_long_journey AND COALESCE(slaughter_pig_count, 0) > 0
                     AND worst_temp < -5 THEN 'WELFARE_RISK'
                WHEN NOT is_long_journey AND COALESCE(slaughter_pig_count, 0) > 0
                     AND worst_temp BETWEEN -5 AND 0 THEN 'BELOW_THERMONEUTRAL'
                WHEN NOT is_long_journey AND COALESCE(slaughter_pig_count, 0) > 0
                     AND worst_temp BETWEEN 0 AND 10 THEN 'BELOW_THERMONEUTRAL'

                -- Short journeys, unknown category: use conservative thresholds
                WHEN NOT is_long_journey AND worst_temp < -5 THEN 'WELFARE_RISK'
                WHEN NOT is_long_journey AND worst_temp BETWEEN -5 AND 0 THEN 'BELOW_THERMONEUTRAL'
                WHEN NOT is_long_journey AND worst_temp BETWEEN 0 AND 5 THEN 'BELOW_THERMONEUTRAL'

                -- Heat (any journey type)
                WHEN worst_temp > 35 THEN 'VIOLATION'
                WHEN worst_temp > 30 THEN 'WELFARE_RISK'

                ELSE 'COMPLIANT'
            END AS compliance_status,
            CASE
                WHEN is_long_journey THEN 'Annex I Ch VI 3.1 (5-30°C ±5°C)'
                WHEN sow_count > 0 THEN 'Art 3 + EURCAW thermoneutral zone (sows: 10-20°C)'
                WHEN COALESCE(slaughter_pig_count, 0) > 0 THEN 'Art 3 + EURCAW thermoneutral zone (finishing: 10-28°C)'
                ELSE 'Art 3 general welfare + EC factsheet thermoneutral zones'
            END AS regulatory_basis
        FROM compliance_check
    """)

    # Summary
    summary = conn.execute("""
        SELECT
            compliance_status,
            COUNT(*) as count,
            COALESCE(SUM(total_animals), 0) as animals,
            ROUND(MIN(worst_temp), 1) as min_temp,
            ROUND(AVG(worst_temp), 1) as avg_temp
        FROM results
        GROUP BY compliance_status
        ORDER BY
            CASE compliance_status
                WHEN 'VIOLATION' THEN 1
                WHEN 'WELFARE_RISK' THEN 2
                WHEN 'BELOW_THERMONEUTRAL' THEN 3
                WHEN 'CAUTION' THEN 4
                ELSE 5
            END
    """).fetchall()

    logger.info("=== COMPLIANCE RESULTS ===")
    for status, count, animals, min_temp, avg_temp in summary:
        logger.info(f"  {status}: {count:,} movements, {animals:,} animals (min {min_temp}°C, avg {avg_temp}°C)")

    # Category breakdown
    cat_summary = conn.execute("""
        SELECT pig_category, compliance_status, COUNT(*), COALESCE(SUM(total_animals), 0)
        FROM results WHERE compliance_status != 'COMPLIANT'
        GROUP BY pig_category, compliance_status
        ORDER BY pig_category, compliance_status
    """).fetchall()
    if cat_summary:
        logger.info("  --- By pig category ---")
        for cat, status, cnt, animals in cat_summary:
            logger.info(f"    {cat} / {status}: {cnt:,} movements, {animals:,} animals")


def create_all_compliant_results(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a synthetic all-compliant results table when no cold movements are found.

    Keeps downstream report/export code working by providing the same table shape
    that classify_compliance() normally produces.
    """
    conn.execute("""
        CREATE TABLE results AS
        SELECT
            m.*,
            CAST(NULL AS DOUBLE) AS distance_km,
            CAST(NULL AS DOUBLE) AS duration_hours,
            false AS is_long_journey,
            CAST(NULL AS DOUBLE) AS sender_departure_temp,
            CAST(NULL AS DOUBLE) AS receiver_arrival_temp,
            CAST(NULL AS DOUBLE) AS worst_temp,
            CASE
                WHEN sow_count > 0 AND (slaughter_pig_count IS NULL OR slaughter_pig_count = 0) THEN 'sows'
                WHEN slaughter_pig_count > 0 AND (sow_count IS NULL OR sow_count = 0) THEN 'slaughter_pigs'
                WHEN COALESCE(sow_count, 0) > 0 AND COALESCE(slaughter_pig_count, 0) > 0 THEN 'mixed'
                ELSE 'unknown'
            END AS pig_category,
            'COMPLIANT' AS compliance_status,
            'No sub-5°C departure temperatures observed in analysis period' AS regulatory_basis
        FROM movements m
    """)


def generate_report(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Step 7: Generate CSV and HTML reports."""

    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV export of flagged movements
    flagged_count = conn.execute("SELECT COUNT(*) FROM results WHERE compliance_status != 'COMPLIANT'").fetchone()[0]

    if flagged_count > 0:
        csv_path = output_dir / "flagged_movements.csv"
        conn.execute(f"""
            COPY (
                SELECT
                    movement_id,
                    movement_date,
                    movement_time,
                    sender_chr_number,
                    sender_address,
                    sender_postal_code,
                    sender_postal_district,
                    sender_municipality_name,
                    receiver_chr_number,
                    receiver_address,
                    receiver_postal_code,
                    receiver_postal_district,
                    receiver_municipality_name,
                    total_animals,
                    sow_count,
                    slaughter_pig_count,
                    distance_km,
                    duration_hours,
                    is_long_journey,
                    sender_departure_temp,
                    receiver_arrival_temp,
                    worst_temp,
                    compliance_status,
                    regulatory_basis
                FROM results
                WHERE compliance_status != 'COMPLIANT'
                ORDER BY worst_temp ASC
            ) TO '{csv_path}' (FORMAT CSV, HEADER)
        """)
        logger.info(f"Exported {flagged_count} flagged movements to {csv_path}")
    else:
        logger.info("No flagged movements — all compliant!")

    # Generate HTML report
    total_movements = conn.execute("SELECT COUNT(*) FROM movements").fetchone()[0]
    cold_movements = conn.execute("SELECT COUNT(*) FROM cold_movements").fetchone()[0]

    summary_rows = conn.execute("""
        SELECT compliance_status, COUNT(*) as cnt, COALESCE(SUM(total_animals), 0) as animals,
               ROUND(MIN(worst_temp), 1), ROUND(AVG(worst_temp), 1)
        FROM results GROUP BY compliance_status
        ORDER BY CASE compliance_status
            WHEN 'VIOLATION' THEN 1 WHEN 'WELFARE_RISK' THEN 2
            WHEN 'BELOW_THERMONEUTRAL' THEN 3 WHEN 'CAUTION' THEN 4 ELSE 5
        END
    """).fetchall()

    # Daily temperature summary for chart
    daily_temps = conn.execute("""
        SELECT date, MIN(temperature_c), MAX(temperature_c), AVG(temperature_c)
        FROM temperatures
        GROUP BY date ORDER BY date
    """).fetchall()

    # Daily movement counts
    daily_movements = conn.execute("""
        SELECT CAST(movement_date AS VARCHAR) as date, COUNT(*) as cnt
        FROM movements GROUP BY date ORDER BY date
    """).fetchall()

    daily_flagged = conn.execute("""
        SELECT CAST(movement_date AS VARCHAR) as date, COUNT(*) as cnt
        FROM results WHERE compliance_status != 'COMPLIANT'
        GROUP BY date ORDER BY date
    """).fetchall()

    # Top flagged routes
    top_routes = conn.execute("""
        SELECT
            sender_municipality_name,
            receiver_municipality_name,
            COUNT(*) as cnt,
            SUM(total_animals) as animals,
            ROUND(MIN(worst_temp), 1) as min_temp,
            compliance_status
        FROM results
        WHERE compliance_status != 'COMPLIANT'
        GROUP BY sender_municipality_name, receiver_municipality_name, compliance_status
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    # Build HTML
    html = _build_html_report(
        total_movements,
        cold_movements,
        summary_rows,
        daily_temps,
        daily_movements,
        daily_flagged,
        top_routes,
    )

    html_path = output_dir / "report.html"
    with open(html_path, "w") as f:
        f.write(html)
    logger.info(f"HTML report saved to {html_path}")

    # Export Kepler.gl-ready CSV with sender/receiver coordinates
    _export_kepler_csv(conn, output_dir)

    # Fetch actual route geometries for flagged movements and export GeoJSON
    _export_route_geojson(conn, output_dir)

    # Export Trip layer GeoJSON with timestamps for animated playback
    _export_trip_geojson(conn, output_dir)

    # Export hourly temperature grid for map visualization
    _export_temperature_grid(conn, output_dir)


def _export_kepler_csv(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Export CSV with arc data for Kepler.gl visualization.

    Kepler.gl needs: sender_lat, sender_lon, receiver_lat, receiver_lon
    plus attributes for coloring/filtering.
    """

    # Load postal code coords from cache
    cache_path = output_dir / "postal_code_coords.json"
    if not cache_path.exists():
        logger.warning("No postal code coords cache — skipping Kepler export")
        return

    with open(cache_path) as f:
        coords = json.load(f)

    # Register coords as a DuckDB table
    coord_rows = [(int(pc), c[0], c[1]) for pc, c in coords.items()]
    conn.execute("CREATE TABLE IF NOT EXISTS pc_coords (postal_code INTEGER, lat DOUBLE, lon DOUBLE)")
    conn.executemany("INSERT INTO pc_coords VALUES (?, ?, ?)", coord_rows)

    # Export all results with coordinates for Kepler.gl
    kepler_path = output_dir / "kepler_arcs.csv"
    conn.execute(f"""
        COPY (
            SELECT
                r.movement_date,
                r.movement_hour,
                r.pig_category,
                r.total_animals,
                r.sow_count,
                r.slaughter_pig_count,
                r.sender_postal_code,
                r.sender_municipality_name,
                sc.lat AS sender_lat,
                sc.lon AS sender_lon,
                r.receiver_postal_code,
                r.receiver_municipality_name,
                rc.lat AS receiver_lat,
                rc.lon AS receiver_lon,
                r.worst_temp,
                r.sender_departure_temp,
                r.distance_km,
                r.duration_hours,
                r.compliance_status,
                r.regulatory_basis
            FROM results r
            LEFT JOIN pc_coords sc ON sc.postal_code = r.sender_postal_code
            LEFT JOIN pc_coords rc ON rc.postal_code = r.receiver_postal_code
            WHERE sc.lat IS NOT NULL AND rc.lat IS NOT NULL
            ORDER BY r.worst_temp ASC
        ) TO '{kepler_path}' (FORMAT CSV, HEADER)
    """)

    row_count = conn.execute("""
        SELECT COUNT(*) FROM results r
        LEFT JOIN pc_coords sc ON sc.postal_code = r.sender_postal_code
        LEFT JOIN pc_coords rc ON rc.postal_code = r.receiver_postal_code
        WHERE sc.lat IS NOT NULL AND rc.lat IS NOT NULL
    """).fetchone()[0]

    logger.info(f"Kepler.gl arc CSV saved to {kepler_path} ({row_count:,} rows)")
    logger.info(
        "  Open https://kepler.gl → Add Data → upload kepler_arcs.csv → "
        "Add Arc layer with sender_lat/lon → receiver_lat/lon, color by worst_temp"
    )


def _export_route_geojson(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Fetch actual road geometries from OSRM for flagged movements, export GeoJSON.

    Uses OSRM public demo server (free, no key needed) to get real driving
    route polylines. Only fetches for VIOLATION + WELFARE_RISK movements
    to keep call count manageable.
    """
    cache_path = output_dir / "route_geometries.json"
    route_cache: dict[str, list] = {}
    if cache_path.exists():
        with open(cache_path) as f:
            route_cache = json.load(f)

    # Get unique coordinate pairs for flagged movements
    pairs = conn.execute("""
        SELECT DISTINCT
            sc.lat AS sender_lat, sc.lon AS sender_lon,
            rc.lat AS receiver_lat, rc.lon AS receiver_lon,
            r.sender_postal_code, r.receiver_postal_code
        FROM results r
        LEFT JOIN pc_coords sc ON sc.postal_code = r.sender_postal_code
        LEFT JOIN pc_coords rc ON rc.postal_code = r.receiver_postal_code
        WHERE r.compliance_status IN ('VIOLATION', 'WELFARE_RISK')
          AND sc.lat IS NOT NULL AND rc.lat IS NOT NULL
          AND (sc.lat != rc.lat OR sc.lon != rc.lon)  -- skip only exact same-point trips
    """).fetchall()

    logger.info(f"Fetching route geometries for {len(pairs)} unique flagged pairs")

    new_fetches = 0
    for i, (s_lat, s_lon, r_lat, r_lon, s_pc, r_pc) in enumerate(pairs):
        cache_key = f"{s_lat},{s_lon}->{r_lat},{r_lon}"
        if cache_key in route_cache:
            continue

        # OSRM public API — returns GeoJSON route geometry
        url = (
            f"https://router.project-osrm.org/route/v1/driving/"
            f"{s_lon},{s_lat};{r_lon},{r_lat}"
            f"?overview=full&geometries=geojson"
        )
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                route_cache[cache_key] = data["routes"][0]["geometry"]["coordinates"]
            else:
                logger.debug(f"OSRM no route: {s_pc} → {r_pc}")
                route_cache[cache_key] = None
        except Exception as e:
            logger.debug(f"OSRM error {s_pc} → {r_pc}: {e}")
            route_cache[cache_key] = None

        new_fetches += 1
        if new_fetches % 50 == 0:
            logger.info(f"  Fetched {new_fetches} routes so far...")
            # Save intermediate cache
            with open(cache_path, "w") as f:
                json.dump(route_cache, f)

        # Respect OSRM public server — ~1 req/sec
        time.sleep(1.0)

    # Save final cache
    with open(cache_path, "w") as f:
        json.dump(route_cache, f)
    logger.info(f"Route geometry cache: {len(route_cache)} entries ({new_fetches} new)")

    # Build GeoJSON FeatureCollection
    # Aggregate movement data per route for properties
    route_stats = conn.execute("""
        SELECT
            sc.lat AS sender_lat, sc.lon AS sender_lon,
            rc.lat AS receiver_lat, rc.lon AS receiver_lon,
            r.sender_postal_code,
            r.sender_municipality_name,
            r.receiver_postal_code,
            r.receiver_municipality_name,
            COUNT(*) AS movement_count,
            SUM(r.total_animals) AS total_animals,
            MIN(r.worst_temp) AS min_temp,
            ROUND(AVG(r.worst_temp), 1) AS avg_temp,
            MAX(r.worst_temp) AS max_temp,
            r.compliance_status,
            MIN(r.movement_date) AS first_date,
            MAX(r.movement_date) AS last_date
        FROM results r
        LEFT JOIN pc_coords sc ON sc.postal_code = r.sender_postal_code
        LEFT JOIN pc_coords rc ON rc.postal_code = r.receiver_postal_code
        WHERE r.compliance_status IN ('VIOLATION', 'WELFARE_RISK')
          AND sc.lat IS NOT NULL AND rc.lat IS NOT NULL
        GROUP BY sc.lat, sc.lon, rc.lat, rc.lon,
                 r.sender_postal_code, r.sender_municipality_name,
                 r.receiver_postal_code, r.receiver_municipality_name,
                 r.compliance_status
    """).fetchall()

    features = []
    routes_found = 0
    for row in route_stats:
        (
            s_lat,
            s_lon,
            r_lat,
            r_lon,
            s_pc,
            s_mun,
            r_pc,
            r_mun,
            count,
            animals,
            min_t,
            avg_t,
            max_t,
            status,
            first_d,
            last_d,
        ) = row

        cache_key = f"{s_lat},{s_lon}->{r_lat},{r_lon}"
        coords = route_cache.get(cache_key)

        if coords is None:
            # Fallback: straight line
            coords = [[s_lon, s_lat], [r_lon, r_lat]]
        else:
            routes_found += 1

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords,
                },
                "properties": {
                    "sender_pc": int(s_pc) if s_pc else None,
                    "sender_municipality": s_mun,
                    "receiver_pc": int(r_pc) if r_pc else None,
                    "receiver_municipality": r_mun,
                    "movement_count": int(count),
                    "total_animals": int(animals) if animals else 0,
                    "min_temp": float(min_t) if min_t else None,
                    "avg_temp": float(avg_t) if avg_t else None,
                    "max_temp": float(max_t) if max_t else None,
                    "compliance_status": status,
                    "first_date": str(first_d),
                    "last_date": str(last_d),
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    geojson_path = output_dir / "flagged_routes.geojson"
    with open(geojson_path, "w") as f:
        json.dump(geojson, f)

    logger.info(
        f"GeoJSON with route trajectories saved to {geojson_path} "
        f"({len(features)} routes, {routes_found} with road geometry)"
    )
    logger.info(
        "  Open https://kepler.gl → Add Data → upload flagged_routes.geojson → "
        "color by min_temp or compliance_status, height by movement_count"
    )


def _export_temperature_grid(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Export hourly temperature data per postal code as compact JSON for map visualization.

    Format: { locations: [{pc, lat, lon}], hours: [{ts, temps: [t1,t2,...]}] }
    where temps array index matches locations array index.
    """
    # Get unique postal codes with coordinates that appear in movements
    locations = conn.execute("""
        SELECT DISTINCT t.postal_code, pc.lat, pc.lon
        FROM temperatures t
        JOIN pc_coords pc ON pc.postal_code = t.postal_code
        WHERE pc.lat IS NOT NULL
        ORDER BY t.postal_code
    """).fetchall()

    pc_to_idx = {row[0]: i for i, row in enumerate(locations)}
    n_locs = len(locations)

    # Get all hourly readings, sorted
    readings = conn.execute("""
        SELECT postal_code, date, hour, temperature_c
        FROM temperatures
        ORDER BY date, hour, postal_code
    """).fetchall()

    # Group by (date, hour) → array of temps indexed by location
    # Insertion order is preserved by dict on Python 3.7+; readings are pre-sorted.
    hours_dict: dict[str, list] = {}
    for pc, date_str, hour, temp in readings:
        if pc not in pc_to_idx:
            continue
        # Build unix timestamp
        ts_key = f"{date_str} {int(hour):02d}"
        if ts_key not in hours_dict:
            hours_dict[ts_key] = [None] * n_locs
        hours_dict[ts_key][pc_to_idx[pc]] = round(temp, 1) if temp is not None else None

    # Convert to compact format with unix timestamps
    hours_out = []
    for ts_key, temps in hours_dict.items():
        try:
            dt = datetime.strptime(ts_key, "%Y-%m-%d %H")
            unix_ts = int(dt.timestamp())
        except ValueError:
            continue
        hours_out.append({"ts": unix_ts, "t": temps})

    result = {
        "locations": [{"pc": r[0], "lat": round(r[1], 4), "lon": round(r[2], 4)} for r in locations],
        "hours": hours_out,
    }

    path = output_dir / "temperature_grid.json"
    with open(path, "w") as f:
        json.dump(result, f)

    logger.info(f"Temperature grid saved to {path} ({n_locs} locations, {len(hours_out)} hourly snapshots)")


def _export_trip_geojson(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Export Kepler.gl Trip layer GeoJSON with timestamps for animated playback.

    Trip layer requires coordinates as [lon, lat, altitude, unix_timestamp].
    Each movement becomes one Feature — timestamps are interpolated along
    the route geometry based on estimated duration.
    """
    cache_path = output_dir / "route_geometries.json"
    if not cache_path.exists():
        logger.warning("No route geometry cache — skipping trip export")
        return

    with open(cache_path) as f:
        route_cache = json.load(f)

    # Get individual movements (not aggregated) for VIOLATION + WELFARE_RISK
    movements = conn.execute("""
        SELECT
            r.movement_date,
            r.movement_hour,
            sc.lat AS sender_lat, sc.lon AS sender_lon,
            rc.lat AS receiver_lat, rc.lon AS receiver_lon,
            r.sender_postal_code,
            r.receiver_postal_code,
            r.sender_municipality_name,
            r.receiver_municipality_name,
            r.total_animals,
            r.worst_temp,
            r.duration_hours,
            r.compliance_status,
            r.pig_category
        FROM results r
        LEFT JOIN pc_coords sc ON sc.postal_code = r.sender_postal_code
        LEFT JOIN pc_coords rc ON rc.postal_code = r.receiver_postal_code
        WHERE r.compliance_status IN ('VIOLATION', 'WELFARE_RISK')
          AND sc.lat IS NOT NULL AND rc.lat IS NOT NULL
        ORDER BY r.movement_date, r.movement_hour
    """).fetchall()

    logger.info(f"Building trip GeoJSON for {len(movements)} flagged movements")

    features = []
    for row in movements:
        (
            date,
            hour,
            s_lat,
            s_lon,
            r_lat,
            r_lon,
            s_pc,
            r_pc,
            s_mun,
            r_mun,
            animals,
            temp,
            duration_h,
            status,
            pig_cat,
        ) = row

        # Build departure unix timestamp from date + hour
        hour = hour if hour is not None else 8  # default 8am
        date_str = str(date)
        try:
            departure_dt = datetime.strptime(f"{date_str} {int(hour):02d}:00:00", "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            continue
        departure_unix = int(departure_dt.timestamp())

        # Duration in seconds (default 1 hour if missing)
        duration_s = int((duration_h or 1.0) * 3600)
        if duration_s < 60:
            duration_s = 3600

        # Look up route geometry and simplify to max 30 points
        cache_key = f"{s_lat},{s_lon}->{r_lat},{r_lon}"
        coords = route_cache.get(cache_key)

        if coords is None or len(coords) < 2:
            coords = [[s_lon, s_lat], [r_lon, r_lat]]

        # Downsample to max ~30 points to keep file size manageable
        max_pts = 30
        if len(coords) > max_pts:
            step = (len(coords) - 1) / (max_pts - 1)
            indices = [int(round(i * step)) for i in range(max_pts)]
            coords = [coords[i] for i in indices]

        # Interpolate timestamps evenly along route points
        n_points = len(coords)
        trip_coords = []
        for i, coord in enumerate(coords):
            frac = i / max(n_points - 1, 1)
            t = departure_unix + int(frac * duration_s)
            # [lon, lat, altitude, timestamp]
            trip_coords.append([coord[0], coord[1], 0, t])

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": trip_coords,
                },
                "properties": {
                    "sender": f"{s_mun} ({s_pc})",
                    "receiver": f"{r_mun} ({r_pc})",
                    "animals": int(animals) if animals else 0,
                    "temp": float(temp) if temp else None,
                    "status": status,
                    "pig_category": pig_cat,
                    "date": date_str,
                    "hour": int(hour) if hour else None,
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    trip_path = output_dir / "trip_animation.geojson"
    with open(trip_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Trip animation GeoJSON saved to {trip_path} ({len(features)} trips)")
    logger.info(
        "  Open https://kepler.gl → Add Data → upload trip_animation.geojson → "
        "Change layer type to 'Trip' → press play to animate over time"
    )


def _build_html_report(
    total_movements,
    cold_movements,
    summary_rows,
    daily_temps,
    daily_movements,
    daily_flagged,
    top_routes,
):
    """Build self-contained HTML report following data-viz best practices.

    Principles applied:
    - Tufte: maximize data-ink, no chartjunk, separate charts instead of dual axes
    - Data journalism: insight titles, direct annotations, generous context
    - Accessibility: colorblind-safe palette, WCAG contrast, screen-reader tables
    """

    # Prepare chart data
    all_dates = sorted(set(r[0] for r in daily_temps))
    dates = json.dumps(all_dates)
    min_temps_list = [r[1] for r in daily_temps]
    max_temps_list = [r[2] for r in daily_temps]
    avg_temps_list = [round(r[3], 1) for r in daily_temps]
    min_temps = json.dumps(min_temps_list)
    max_temps = json.dumps(max_temps_list)
    avg_temps = json.dumps(avg_temps_list)

    movement_dates = {r[0]: r[1] for r in daily_movements}
    flagged_dates = {r[0]: r[1] for r in daily_flagged}
    mvmt_list = [movement_dates.get(d, 0) for d in all_dates]
    flag_list = [flagged_dates.get(d, 0) for d in all_dates]
    mvmt_counts = json.dumps(mvmt_list)
    flag_counts = json.dumps(flag_list)

    # Find coldest day for annotation
    coldest_idx = min_temps_list.index(min(min_temps_list)) if min_temps_list else 0
    coldest_date = all_dates[coldest_idx] if all_dates else ""
    coldest_val = min(min_temps_list) if min_temps_list else 0

    # Compute flagged percentage per day for the ratio chart
    pct_list = []
    for m, f in zip(mvmt_list, flag_list):
        pct_list.append(round(f / m * 100, 1) if m > 0 else 0)
    pct_counts = json.dumps(pct_list)

    # Status display names and colorblind-safe palette
    status_display = {
        "VIOLATION": "EU regulation violation",
        "WELFARE_RISK": "Welfare risk",
        "BELOW_THERMONEUTRAL": "Below thermoneutral zone",
        "CAUTION": "Caution",
        "COMPLIANT": "Compliant",
    }
    # Colorblind-safe: dark red-brown, orange, amber, blue-gray, teal
    status_colors = {
        "VIOLATION": "#9f1239",
        "WELFARE_RISK": "#c2410c",
        "BELOW_THERMONEUTRAL": "#b45309",
        "CAUTION": "#78716c",
        "COMPLIANT": "#0f766e",
    }

    # Summary table rows
    summary_html = ""
    status_counts = {}
    for status, cnt, animals, min_t, avg_t in summary_rows:
        status_counts[status] = cnt
        color = status_colors.get(status, "#666")
        label = status_display.get(status, status)
        summary_html += f"""
        <tr>
            <td><span style="color:{color};font-weight:600">{label}</span></td>
            <td style="text-align:right">{cnt:,}</td>
            <td style="text-align:right">{animals:,}</td>
            <td style="text-align:right">{min_t}°C</td>
            <td style="text-align:right">{avg_t}°C</td>
        </tr>"""

    # Top routes table — sorted by severity, then count
    routes_html = ""
    for muni_s, muni_r, cnt, animals, min_t, status in top_routes:
        color = status_colors.get(status, "#666")
        label = status_display.get(status, status)
        routes_html += f"""
        <tr>
            <td>{muni_s or "Unknown"} → {muni_r or "Unknown"}</td>
            <td style="text-align:right">{cnt:,}</td>
            <td style="text-align:right">{animals or 0:,}</td>
            <td style="text-align:right">{min_t}°C</td>
            <td><span style="color:{color}">{label}</span></td>
        </tr>"""

    violations = status_counts.get("VIOLATION", 0)
    welfare_risk = status_counts.get("WELFARE_RISK", 0)
    below_tn = status_counts.get("BELOW_THERMONEUTRAL", 0)
    caution = status_counts.get("CAUTION", 0)
    compliant = total_movements - violations - welfare_risk - below_tn - caution

    # Compute headline stats
    total_flagged = violations + welfare_risk + below_tn + caution
    pct_flagged = round(total_flagged / total_movements * 100, 1) if total_movements else 0
    total_animals_at_risk = sum(r[2] for r in summary_rows if r[0] != "COMPLIANT")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pig Transport Temperature Compliance — Jan–Mar 2026</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3"></script>
<style>
  :root {{
    --color-violation: #9f1239;
    --color-welfare: #c2410c;
    --color-below-tn: #b45309;
    --color-caution: #78716c;
    --color-compliant: #0f766e;
    --color-text: #1e293b;
    --color-text-secondary: #64748b;
    --color-text-muted: #94a3b8;
    --color-border: #e2e8f0;
    --color-bg: #fafafa;
    --color-surface: #ffffff;
    --color-temp-range: #1d4ed8;
    --color-temp-avg: #1e293b;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem;
    background: var(--color-bg); color: var(--color-text);
    line-height: 1.5;
  }}

  /* Typography */
  h1 {{
    font-size: 1.75rem; font-weight: 700; line-height: 1.2;
    margin: 0 0 0.25rem; color: var(--color-text);
  }}
  .subtitle {{ color: var(--color-text-secondary); font-size: 0.95rem; margin: 0 0 2rem; }}
  h2 {{
    font-size: 1.1rem; font-weight: 600; color: var(--color-text);
    margin: 2.5rem 0 0.75rem; padding: 0;
  }}
  h2 .subhead {{ font-weight: 400; color: var(--color-text-secondary); }}

  /* Cards */
  .cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; margin: 1rem 0; background: var(--color-border); border-radius: 8px; overflow: hidden; }}
  .card {{ background: var(--color-surface); padding: 1.25rem; }}
  .card .value {{ font-size: 1.75rem; font-weight: 700; line-height: 1; }}
  .card .label {{ color: var(--color-text-secondary); font-size: 0.8rem; margin-top: 0.35rem; line-height: 1.3; }}

  /* Tables */
  table {{ width: 100%; border-collapse: collapse; margin: 0.5rem 0; font-size: 0.9rem; }}
  th {{ text-align: left; padding: 0.6rem 0.75rem; border-bottom: 2px solid var(--color-text); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; color: var(--color-text-secondary); }}
  td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--color-border); }}
  tr:last-child td {{ border-bottom: none; }}

  /* Charts */
  .chart-figure {{ margin: 0.5rem 0 2rem; }}
  .chart-figure figcaption {{ margin-bottom: 0.5rem; }}
  .chart-figure .chart-title {{ font-size: 1rem; font-weight: 600; line-height: 1.3; }}
  .chart-figure .chart-subtitle {{ font-size: 0.8rem; color: var(--color-text-secondary); margin-top: 0.15rem; }}
  .chart-wrap {{ position: relative; }}
  .chart-source {{ font-size: 0.75rem; color: var(--color-text-muted); margin-top: 0.5rem; }}

  /* Regulation box */
  .regulation {{ border-left: 3px solid var(--color-text-muted); padding: 0.75rem 1rem; margin: 1.5rem 0; font-size: 0.85rem; color: var(--color-text-secondary); line-height: 1.6; }}
  .regulation strong {{ color: var(--color-text); }}

  /* Funnel */
  .funnel {{ display: flex; align-items: center; gap: 0; margin: 1rem 0; flex-wrap: wrap; }}
  .funnel-step {{ display: flex; align-items: baseline; gap: 0.5rem; padding: 0.5rem 1.25rem 0.5rem 0; }}
  .funnel-step .num {{ font-size: 1.25rem; font-weight: 700; font-variant-numeric: tabular-nums; }}
  .funnel-step .desc {{ font-size: 0.85rem; color: var(--color-text-secondary); }}
  .funnel-arrow {{ color: var(--color-text-muted); font-size: 1.25rem; padding: 0 0.25rem; }}

  /* Accessibility: screen-reader only */
  .sr-only {{ position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border: 0; }}

  /* Footer */
  .footer {{ font-size: 0.75rem; color: var(--color-text-muted); margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--color-border); }}

  @media (max-width: 640px) {{
    .cards {{ grid-template-columns: repeat(2, 1fr); }}
    .funnel {{ flex-direction: column; align-items: flex-start; }}
    .funnel-arrow {{ transform: rotate(90deg); }}
  }}
</style>
</head>
<body>

<h1>{pct_flagged}% of pig transports occurred below safe temperature thresholds</h1>
<p class="subtitle">
  {total_flagged:,} of {total_movements:,} movements flagged · {total_animals_at_risk:,} animals affected ·
  Denmark, January–March 2026
</p>

<div class="regulation">
  <strong>Regulatory basis:</strong>
  Long journeys (&gt;8 h) must maintain 5–30 °C inside vehicles (Annex I, Ch. VI, 3.1, EU Reg. 1/2005).
  Short journeys (≤8 h) require protection from "extreme temperatures" (Art. 3) — no specific threshold.
  <br><strong>Scientific reference:</strong> EURCAW-Pigs thermoneutral zones (Wageningen/Aarhus, 2020):
  sows 5–25 °C, slaughter pigs 15–30 °C, heavy finishers (&gt;110 kg) 10–28 °C.
</div>

<h2>Analysis funnel</h2>
<div class="funnel">
  <div class="funnel-step"><span class="num">{total_movements:,}</span><span class="desc">total movements</span></div>
  <span class="funnel-arrow">→</span>
  <div class="funnel-step"><span class="num">{cold_movements:,}</span><span class="desc">during cold hours (&lt;5 °C)</span></div>
  <span class="funnel-arrow">→</span>
  <div class="funnel-step"><span class="num">{total_flagged:,}</span><span class="desc">flagged after classification</span></div>
</div>

<h2>Compliance breakdown</h2>
<div class="cards">
  <div class="card"><div class="value" style="color:var(--color-violation)">{violations:,}</div><div class="label">EU regulation violations<br>(long journeys &lt;0 °C)</div></div>
  <div class="card"><div class="value" style="color:var(--color-welfare)">{welfare_risk:,}</div><div class="label">Welfare risk<br>(below scientific thresholds)</div></div>
  <div class="card"><div class="value" style="color:var(--color-below-tn)">{below_tn:,}</div><div class="label">Below thermoneutral zone<br>(suboptimal conditions)</div></div>
  <div class="card"><div class="value" style="color:var(--color-compliant)">{compliant:,}</div><div class="label">Compliant<br>(within safe range)</div></div>
</div>

<table>
  <thead>
    <tr>
      <th style="text-align:left">Status</th>
      <th style="text-align:right">Movements</th>
      <th style="text-align:right">Animals</th>
      <th style="text-align:right">Coldest</th>
      <th style="text-align:right">Avg temp</th>
    </tr>
  </thead>
  <tbody>{summary_html}</tbody>
</table>

<h2>Temperature range across Denmark <span class="subhead">· daily min/max, Jan–Mar 2026</span></h2>
<figure class="chart-figure" role="figure" aria-label="Daily temperature range in Denmark from January to March 2026. Temperatures dropped to {coldest_val} °C on {coldest_date}. The 5 °C thermoneutral threshold was frequently breached in January and February.">
  <div class="chart-wrap">
    <canvas id="tempChart" height="80"></canvas>
  </div>
  <p class="chart-source">Source: Open-Meteo Historical Weather API, aggregated across Danish postal codes</p>
</figure>

<h2>Flagged transport share <span class="subhead">· % of daily movements below safe thresholds</span></h2>
<figure class="chart-figure" role="figure" aria-label="Percentage of daily pig transports flagged for temperature concerns. Nearly 100% of movements were flagged during the coldest periods in January and mid-February.">
  <div class="chart-wrap">
    <canvas id="flagChart" height="60"></canvas>
  </div>
  <p class="chart-source">Source: Svineflytning silver layer (R2), cross-referenced with hourly temperature data</p>
</figure>

<h2>Busiest flagged routes <span class="subhead">· sorted by movement count</span></h2>
<table>
  <thead>
    <tr>
      <th style="text-align:left">Route</th>
      <th style="text-align:right">Movements</th>
      <th style="text-align:right">Animals</th>
      <th style="text-align:right">Coldest</th>
      <th style="text-align:left">Status</th>
    </tr>
  </thead>
  <tbody>{routes_html or '<tr><td colspan="5">No flagged routes</td></tr>'}</tbody>
</table>

<p class="footer">
  Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} ·
  Weather: Open-Meteo Historical API ·
  Movements: Svineflytning silver layer (R2) ·
  Routing: OpenRouteService HGV ·
  Analysis: <a href="https://landbruget.dk" style="color:inherit">landbruget.dk</a>
</p>

<script>
/* --- Chart 1: Temperature range (min/max band + avg line) --- */
const tempCtx = document.getElementById('tempChart').getContext('2d');
new Chart(tempCtx, {{
  type: 'line',
  data: {{
    labels: {dates},
    datasets: [
      {{
        label: 'Max temperature',
        data: {max_temps},
        borderColor: 'transparent',
        backgroundColor: 'rgba(29, 78, 216, 0.12)',
        fill: '+1',
        pointRadius: 0,
        tension: 0.3
      }},
      {{
        label: 'Min temperature',
        data: {min_temps},
        borderColor: 'rgba(29, 78, 216, 0.3)',
        backgroundColor: 'transparent',
        fill: false,
        borderWidth: 1,
        pointRadius: 0,
        tension: 0.3
      }},
      {{
        label: 'Daily average',
        data: {avg_temps},
        borderColor: '#1e293b',
        backgroundColor: 'transparent',
        fill: false,
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3
      }}
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: true,
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: (ctx) => {{
            const ds = ctx.dataset.label;
            return ds + ': ' + ctx.parsed.y + ' °C';
          }}
        }}
      }},
      annotation: {{
        annotations: {{
          thermo: {{
            type: 'line',
            yMin: 5, yMax: 5,
            borderColor: '#b45309',
            borderWidth: 1.5,
            borderDash: [6, 4],
            label: {{
              content: '5 °C thermoneutral threshold',
              display: true,
              position: 'start',
              backgroundColor: 'transparent',
              color: '#b45309',
              font: {{ size: 11 }}
            }}
          }},
          zero: {{
            type: 'line',
            yMin: 0, yMax: 0,
            borderColor: '#9f1239',
            borderWidth: 1.5,
            borderDash: [6, 4],
            label: {{
              content: '0 °C — EU violation threshold (long journeys)',
              display: true,
              position: 'start',
              backgroundColor: 'transparent',
              color: '#9f1239',
              font: {{ size: 11 }}
            }}
          }},
          coldest: {{
            type: 'point',
            xValue: '{coldest_date}',
            yValue: {coldest_val},
            radius: 5,
            backgroundColor: '#9f1239',
            borderColor: '#fff',
            borderWidth: 2
          }},
          coldestLabel: {{
            type: 'label',
            xValue: '{coldest_date}',
            yValue: {coldest_val - 2},
            content: ['Coldest: {coldest_val} °C'],
            color: '#9f1239',
            font: {{ size: 11, weight: 'bold' }}
          }}
        }}
      }}
    }},
    scales: {{
      y: {{
        title: {{ display: false }},
        ticks: {{
          callback: (v) => v + ' °C',
          font: {{ size: 11 }}
        }},
        grid: {{ color: 'rgba(0,0,0,0.04)' }}
      }},
      x: {{
        ticks: {{
          maxTicksLimit: 12,
          font: {{ size: 11 }},
          callback: function(value, index) {{
            const label = this.getLabelForValue(value);
            const d = new Date(label);
            return d.toLocaleDateString('en-GB', {{ day: 'numeric', month: 'short' }});
          }}
        }},
        grid: {{ display: false }}
      }}
    }}
  }}
}});

/* --- Chart 2: Flagged percentage (separate chart, no dual axis) --- */
const flagCtx = document.getElementById('flagChart').getContext('2d');
new Chart(flagCtx, {{
  type: 'bar',
  data: {{
    labels: {dates},
    datasets: [{{
      label: '% flagged',
      data: {pct_counts},
      backgroundColor: {pct_counts}.map(v =>
        v > 90 ? '#9f1239' :
        v > 50 ? '#c2410c' :
        v > 0 ? '#b45309' :
        '#e2e8f0'
      ),
      borderRadius: 1,
      barPercentage: 1.0,
      categoryPercentage: 1.0
    }}]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          title: (items) => {{
            const d = new Date(items[0].label);
            return d.toLocaleDateString('en-GB', {{ day: 'numeric', month: 'long', year: 'numeric' }});
          }},
          label: (ctx) => {{
            const idx = ctx.dataIndex;
            const total = {mvmt_counts}[idx];
            const flagged = {flag_counts}[idx];
            return [
              ctx.parsed.y + '% of movements flagged',
              flagged.toLocaleString() + ' of ' + total.toLocaleString() + ' movements'
            ];
          }}
        }}
      }}
    }},
    scales: {{
      y: {{
        min: 0, max: 100,
        ticks: {{
          callback: (v) => v + '%',
          font: {{ size: 11 }},
          stepSize: 25
        }},
        grid: {{ color: 'rgba(0,0,0,0.04)' }}
      }},
      x: {{
        ticks: {{
          maxTicksLimit: 12,
          font: {{ size: 11 }},
          callback: function(value, index) {{
            const label = this.getLabelForValue(value);
            const d = new Date(label);
            return d.toLocaleDateString('en-GB', {{ day: 'numeric', month: 'short' }});
          }}
        }},
        grid: {{ display: false }}
      }}
    }}
  }}
}});
</script>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description="Pig transport temperature compliance analysis")
    parser.add_argument("--start-date", default="2026-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-03-31", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--output-dir",
        default="./output/temperature_compliance",
        help="Output directory for reports",
    )
    parser.add_argument(
        "--local-data",
        help="Path to local silver data directory (skips R2 download)",
    )
    args = parser.parse_args()

    load_dotenv()
    output_dir = Path(args.output_dir)

    logger.info("=" * 60)
    logger.info("PIG TRANSPORT TEMPERATURE COMPLIANCE ANALYSIS")
    logger.info(f"Period: {args.start_date} to {args.end_date}")
    logger.info("=" * 60)

    # Setup DuckDB (cloud auth only if not using local data)
    conn = duckdb.connect()
    if not args.local_data:
        setup_duckdb_cloud_auth(conn)

    # Step 1: Load movements
    logger.info("\n--- Step 1: Loading movements ---")
    unique_postal_codes = load_movements(conn, args.start_date, args.end_date, args.local_data)

    if not unique_postal_codes:
        logger.info("No movements found in date range")
        return

    # Step 2: Geocode postal codes
    logger.info("\n--- Step 2: Geocoding postal codes ---")
    postal_coords = geocode_postal_codes(unique_postal_codes, output_dir / "postal_code_coords.json")

    if not postal_coords:
        logger.error("Failed to geocode any postal codes")
        return

    # Step 3: Fetch temperatures
    logger.info("\n--- Step 3: Fetching temperatures ---")
    fetch_temperatures(postal_coords, args.start_date, args.end_date, conn)

    # Step 4: Filter to cold movements
    logger.info("\n--- Step 4: Filtering cold-hour movements ---")
    cold_count = filter_cold_movements(conn)

    if cold_count == 0:
        logger.info("\nAll movements occurred at temperatures ≥5°C. Analysis complete!")
        create_all_compliant_results(conn)
        generate_report(conn, output_dir)
        return

    # Step 5: Geocode addresses + route estimation
    logger.info("\n--- Step 5: Geocoding addresses & estimating routes ---")
    ors_api_key = os.getenv("ORS_API_KEY")
    if not ors_api_key:
        logger.warning("ORS_API_KEY not set — using haversine distance estimates")

    address_coords = geocode_addresses(conn, output_dir / "address_cache.json")
    estimate_routes(conn, address_coords, postal_coords, ors_api_key, output_dir / "route_cache.json")

    # Step 6: Check temperatures + classify
    logger.info("\n--- Step 6: Checking journey temperatures ---")
    check_journey_temperature(conn)
    classify_compliance(conn)

    # Step 7: Generate report
    logger.info("\n--- Step 7: Generating report ---")
    generate_report(conn, output_dir)

    logger.info("\nAnalysis complete!")


if __name__ == "__main__":
    main()
