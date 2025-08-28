#!/usr/bin/env python3
"""
Seed Geocoding Cache Script
===========================

This script seeds the geocoding cache with existing geocoded addresses from
the latest CVR enrichment data. This provides a massive performance boost
by pre-populating the cache with thousands of known address->coordinate mappings.

The script:
1. Finds the latest CVR enrichment addresses data
2. Extracts all geocoded addresses (both DAWA and Datavask)
3. Populates the geocoding cache with these mappings
4. Saves the cache to GCS for immediate use

This means the very first pipeline run after cache implementation will
already have thousands of cached addresses ready to use!
"""

import asyncio
import json
from typing import Dict, List, Optional

from backend.pipelines.unified_pipeline.src.unified_pipeline.util.gcs_access import GCSDataAccess
from backend.pipelines.unified_pipeline.src.unified_pipeline.util.geocoding_cache import GeocodingCache
from backend.pipelines.unified_pipeline.src.unified_pipeline.util.log_util import Logger


def find_latest_cvr_address_data(gcs_access: GCSDataAccess, bucket: str = "landbrugsdata-raw-data") -> Optional[str]:
    """
    Find the latest CVR enrichment address geocoding data file.

    Args:
        gcs_access: GCS access instance
        bucket: GCS bucket name

    Returns:
        Path to latest address geocoding file or None if not found
    """
    log = Logger.get_logger()

    try:
        # Look for CVR enrichment directories with address data
        prefix = "gold/cvr_enrichment/"

        # List all directories (timestamps)
        import subprocess

        result = subprocess.run(["gsutil", "ls", f"gs://{bucket}/{prefix}"], capture_output=True, text=True, check=True)

        directories = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
        if not directories:
            log.warning(f"No CVR enrichment directories found in gs://{bucket}/{prefix}")
            return None

        # Check directories in reverse chronological order for address_geocoding.parquet
        directories.sort(reverse=True)
        for directory in directories:
            potential_file = f"{directory}address_geocoding.parquet"

            # Check if this file exists
            check_result = subprocess.run(["gsutil", "ls", potential_file], capture_output=True, text=True)

            if check_result.returncode == 0:
                log.info(f"Found latest CVR address geocoding file: {potential_file}")
                return potential_file

        log.warning("No address_geocoding.parquet files found in CVR enrichment directories")
        return None

    except Exception as e:
        log.error(f"Error finding latest CVR address data: {e}")
        return None


def extract_geocoded_addresses_from_address_data(gcs_access: GCSDataAccess, file_path: str) -> Dict[str, List]:
    """
    Extract all geocoded addresses from CVR enrichment address data.

    Args:
        gcs_access: GCS access instance
        file_path: Path to address geocoding parquet file

    Returns:
        Dictionary with DAWA and address text mappings
    """
    log = Logger.get_logger()

    log.info(f"Extracting geocoded addresses from {file_path}")

    try:
        # Use DuckDB to efficiently process the address data file
        conn = gcs_access.duckdb_conn

        # Create temporary table from the address data file
        conn.execute(f"""
            CREATE OR REPLACE TABLE temp_addresses AS
            SELECT * FROM read_parquet('{file_path}')
        """)

        # Get count of total addresses
        total_count = conn.execute("SELECT COUNT(*) FROM temp_addresses").fetchone()[0]
        log.info(f"Found {total_count:,} total addresses in dataset")

        # Check what columns are available
        columns_result = conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'temp_addresses'
        """).fetchall()

        available_columns = [col[0] for col in columns_result]
        log.info(f"Available columns: {', '.join(available_columns)}")

        # Extract DAWA geocoded addresses (with adresse_id and coordinates)
        dawa_addresses = conn.execute("""
            SELECT
                adresse_id,
                latitude,
                longitude,
                coordinate_system,
                srid,
                geometry_wkt,
                geometry_geojson,
                coordinate_quality,
                coordinate_source,
                geocoding_timestamp
            FROM temp_addresses
            WHERE adresse_id IS NOT NULL
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND dawa_enriched = true
        """).fetchall()

        log.info(f"Found {len(dawa_addresses):,} DAWA-geocoded addresses with adresse_id")

        # Extract Datavask geocoded addresses (with full address text and coordinates)
        datavask_addresses = conn.execute("""
            SELECT
                full_address,
                latitude,
                longitude,
                coordinate_system,
                srid,
                geometry_wkt,
                geometry_geojson,
                coordinate_quality,
                coordinate_source,
                geocoding_timestamp
            FROM temp_addresses
            WHERE full_address IS NOT NULL
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND datavask_enriched = true
                AND (adresse_id IS NULL OR dawa_enriched = false)  -- Only Datavask-only addresses
        """).fetchall()

        log.info(f"Found {len(datavask_addresses):,} Datavask-only geocoded addresses")

        # Clean up temporary table
        conn.execute("DROP TABLE temp_addresses")

        return {"dawa_addresses": dawa_addresses, "datavask_addresses": datavask_addresses}

    except Exception as e:
        log.error(f"Error extracting addresses from {file_path}: {e}")
        return {"dawa_addresses": [], "datavask_addresses": []}


def seed_cache_with_addresses(cache: GeocodingCache, addresses_data: Dict[str, List]) -> Dict[str, int]:
    """
    Seed the geocoding cache with extracted address data.

    Args:
        cache: Geocoding cache instance
        addresses_data: Extracted address data

    Returns:
        Dictionary with seeding statistics
    """
    log = Logger.get_logger()

    stats = {
        "dawa_addresses_seeded": 0,
        "datavask_addresses_seeded": 0,
        "dawa_addresses_skipped": 0,
        "datavask_addresses_skipped": 0,
    }

    # Seed DAWA addresses
    log.info("Seeding DAWA address cache...")
    for addr_data in addresses_data["dawa_addresses"]:
        try:
            adresse_id = addr_data[0]
            if not adresse_id:
                stats["dawa_addresses_skipped"] += 1
                continue

            geocoding_result = {
                "latitude": addr_data[1],
                "longitude": addr_data[2],
                "coordinate_system": addr_data[3] or "WGS84",
                "srid": addr_data[4] or 4326,
                "geometry_wkt": addr_data[5],
                "geometry_geojson": json.loads(addr_data[6]) if addr_data[6] else None,
                "coordinate_quality": addr_data[7],
                "coordinate_source": addr_data[8],
                "dawa_fetch_timestamp": addr_data[9],
            }

            cache.store_dawa_result(adresse_id, geocoding_result)
            stats["dawa_addresses_seeded"] += 1

            if stats["dawa_addresses_seeded"] % 1000 == 0:
                log.info(f"   Seeded {stats['dawa_addresses_seeded']:,} DAWA addresses...")

        except Exception as e:
            log.warning(f"Error seeding DAWA address {addr_data[0]}: {e}")
            stats["dawa_addresses_skipped"] += 1

    log.info(
        f"Completed DAWA seeding: {stats['dawa_addresses_seeded']:,} seeded, "
        f"{stats['dawa_addresses_skipped']:,} skipped"
    )

    # Seed Datavask addresses
    log.info("Seeding Datavask address cache...")
    for addr_data in addresses_data["datavask_addresses"]:
        try:
            full_address = addr_data[0]
            if not full_address:
                stats["datavask_addresses_skipped"] += 1
                continue

            geocoding_result = {
                "latitude": addr_data[1],
                "longitude": addr_data[2],
                "coordinate_system": addr_data[3] or "WGS84",
                "srid": addr_data[4] or 4326,
                "geometry_wkt": addr_data[5],
                "geometry_geojson": json.loads(addr_data[6]) if addr_data[6] else None,
                "coordinate_quality": addr_data[7],
                "coordinate_source": addr_data[8],
                "datavask_fetch_timestamp": addr_data[9],
            }

            cache.store_address_text_result(full_address, geocoding_result, "datavask")
            stats["datavask_addresses_seeded"] += 1

            if stats["datavask_addresses_seeded"] % 1000 == 0:
                log.info(f"   Seeded {stats['datavask_addresses_seeded']:,} Datavask addresses...")

        except Exception as e:
            log.warning(f"Error seeding Datavask address {addr_data[0]}: {e}")
            stats["datavask_addresses_skipped"] += 1

    log.info(
        f"Completed Datavask seeding: {stats['datavask_addresses_seeded']:,} seeded, "
        f"{stats['datavask_addresses_skipped']:,} skipped"
    )

    return stats


async def main():
    """Main seeding process."""
    log = Logger.get_logger()

    log.info("🌱 Starting geocoding cache seeding process...")

    try:
        # Initialize GCS access
        gcs_access = GCSDataAccess()

        # Find latest CVR address data
        log.info("📂 Finding latest CVR enrichment address data...")
        latest_file = find_latest_cvr_address_data(gcs_access)

        if not latest_file:
            log.error("❌ Could not find CVR enrichment address data to seed from")
            return

        # Extract geocoded addresses from the data
        log.info("📊 Extracting geocoded addresses from latest address data...")
        addresses_data = extract_geocoded_addresses_from_address_data(gcs_access, latest_file)

        total_addresses = len(addresses_data["dawa_addresses"]) + len(addresses_data["datavask_addresses"])
        if total_addresses == 0:
            log.warning("⚠️  No geocoded addresses found in the data - nothing to seed")
            return

        log.info(f"📍 Found {total_addresses:,} geocoded addresses to seed cache with")

        # Initialize geocoding cache
        log.info("🗄️  Initializing geocoding cache...")
        with GeocodingCache(cache_version=1) as cache:  # Use production cache version
            # Check if cache already has data
            initial_stats = cache.get_cache_stats()
            log.info(f"📊 Initial cache stats: {initial_stats}")

            if initial_stats["total_cache_entries"] > 0:
                log.info("ℹ️  Cache already contains data - seeding will add/update entries")

            # Seed the cache
            log.info("🌱 Seeding cache with geocoded addresses...")
            seeding_stats = seed_cache_with_addresses(cache, addresses_data)

            # Get final cache stats
            final_stats = cache.get_cache_stats()

            # Log comprehensive results
            log.info("🎉 Cache seeding completed!")
            log.info("📊 Seeding Summary:")
            log.info(f"   • DAWA addresses seeded: {seeding_stats['dawa_addresses_seeded']:,}")
            log.info(f"   • DAWA addresses skipped: {seeding_stats['dawa_addresses_skipped']:,}")
            log.info(f"   • Datavask addresses seeded: {seeding_stats['datavask_addresses_seeded']:,}")
            log.info(f"   • Datavask addresses skipped: {seeding_stats['datavask_addresses_skipped']:,}")
            total_seeded_count = seeding_stats["dawa_addresses_seeded"] + seeding_stats["datavask_addresses_seeded"]
            log.info(f"   • Total addresses seeded: {total_seeded_count:,}")

            log.info("📊 Final Cache Stats:")
            log.info(f"   • DAWA ID cache entries: {final_stats['dawa_id_cache_entries']:,}")
            log.info(f"   • Address text cache entries: {final_stats['address_text_cache_entries']:,}")
            log.info(f"   • Total cache entries: {final_stats['total_cache_entries']:,}")

            # Calculate potential performance impact
            total_seeded = seeding_stats["dawa_addresses_seeded"] + seeding_stats["datavask_addresses_seeded"]
            if total_seeded > 0:
                log.info("🚀 Performance Impact:")
                log.info(f"   • {total_seeded:,} addresses will now be cached for instant lookup")
                min_savings = total_seeded * 0.5
                max_savings = total_seeded * 2
                log.info(f"   • Estimated time savings: {min_savings:.0f}-{max_savings:.0f} seconds per pipeline run")
                log.info(f"   • API calls saved per run: up to {total_seeded:,}")

        log.info("✅ Geocoding cache seeding completed successfully!")
        log.info("💡 The cache is now pre-populated and ready for immediate use in CVR enrichment!")

    except Exception as e:
        log.error(f"❌ Cache seeding failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
