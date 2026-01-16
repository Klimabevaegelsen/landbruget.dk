"""
Cattle feed emissions (purchased feed).

Based on:
- KB_21_5397_AP2: Notat beregningsgrundlag (SEGES 2021)
- Table 3: Feed emission factors for livestock production (pages 24-25)
- Lifecycle emissions from feed production and transport
- Cattle feed is measured in kg TS (Tørstof/dry matter) per day

Source Attribution:
- Feed emission factors: KB_21_5397_AP2 pages 24-25, SEGES 2021
- Standard feed intake: standardfaktorer_dyr.json (Tables 2a-2f when available)
"""

from typing import Dict, Optional
from pathlib import Path
import json


def load_reference_values() -> Dict[str, Dict[str, float]]:
    """
    Load feed emission factors from Table 3.

    Returns emission factors in g CO2e per kg TS (dry matter).

    Source: KB_21_5397_AP2 pages 24-25, SEGES 2021
    """
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(base_path / "tabel_3_indkøbte_fodermidler_har_følgende_klimaværdi_udtryk_i_g_co2_ækv_per_kg_tørstof_side_24-25.json") as f:
        table_3 = json.load(f)

    # Parse Table 3 - Feed emission factors
    feed_factors = {}
    for row in table_3["data"]:
        feed_name = row["fodermiddel"]
        # Create normalized key for lookup
        key = feed_name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        feed_factors[key] = {
            "g_co2e_per_kg_ts": row["g_CO2_ækv_per_kg_TS"],
            "name": feed_name,
            "reference": row.get("Reference", "KB_21_5397_AP2 pages 24-25"),
        }

    return feed_factors


# Load constants once at module import
FEED_FACTORS = load_reference_values()


def calculate_feed_emissions_kvaeg(
    cattle_type: str,
    antal_dyr: float,
    ts_per_day: float,
    days_per_year: float = 365,
    feed_composition: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """
    Calculate emissions from purchased cattle feed.

    Formula:
        CO2e (kg) = antal_dyr × TS_per_day × days × Σ(share × EF_per_kg_TS) / 1000

    Where:
        - TS_per_day: Dry matter intake per animal per day (kg TS/day)
        - days: Number of days per year (typically 365)
        - share: Fraction of feed type in diet (0-1)
        - EF_per_kg_TS: Emission factor per kg dry matter (g CO2e/kg TS)
        - Division by 1000: Convert g to kg

    Args:
        cattle_type: Type of cattle ('malkekøer', 'kvier', 'tyre_stude')
        antal_dyr: Number of animals
        ts_per_day: Dry matter intake per animal (kg TS/day)
        days_per_year: Number of days per year (default: 365)
        feed_composition: Optional custom feed composition as dict mapping
            feed type to share (0-1). If None, uses default composition.

    Returns:
        Dict with:
            - co2e_kg: Total feed emissions (kg CO2e/year)
            - co2e_per_animal: Emissions per animal (kg CO2e/year)
            - total_ts: Total dry matter (kg TS/year)
            - feed_breakdown: Emissions by feed type

    Source: KB_21_5397_AP2 pages 24-25, SEGES 2021

    Example:
        >>> # 100 dairy cows with 23.5 kg TS/day intake
        >>> result = calculate_feed_emissions_kvaeg('malkekøer', 100, 23.5)
        >>> print(f"Feed emissions: {result['co2e_kg']:.2f} kg CO2e")
    """
    if antal_dyr <= 0 or ts_per_day <= 0:
        return {
            "co2e_kg": 0.0,
            "co2e_per_animal": 0.0,
            "total_ts": 0.0,
            "feed_breakdown": {},
        }

    # Normalize cattle type
    cattle_type_clean = cattle_type.lower().strip()

    # Determine default feed composition based on cattle type
    # Source: Typical Danish dairy/beef feed mixes from KB_21_5397_AP2
    if cattle_type_clean in ["malkekøer", "malkeko", "dairy_cow", "dairy_cows"]:
        # Dairy cows: High-energy TMR (Total Mixed Ration)
        default_feeds = {
            "majsensilage": 0.30,  # 30% maize silage
            "kløvergræs_/græsensilge_(_>20_%_kløver)": 0.25,  # 25% grass/clover silage
            "kraftfoder_<_25_%_protein": 0.30,  # 30% concentrate
            "sojaprodukter": 0.10,  # 10% protein supplement
            "andet_grovfoder_(roer,_helsæd_mv)": 0.05,  # 5% other roughage
        }
    elif cattle_type_clean in ["kvier", "kvie", "heifer", "heifers"]:
        # Heifers: Lower energy, more roughage
        default_feeds = {
            "kløvergræs_/græsensilge_(_>20_%_kløver)": 0.50,  # 50% grass/clover silage
            "majsensilage": 0.20,  # 20% maize silage
            "halm": 0.10,  # 10% straw
            "kraftfoder_<_25_%_protein": 0.15,  # 15% concentrate
            "andet_grovfoder_(roer,_helsæd_mv)": 0.05,  # 5% other roughage
        }
    elif cattle_type_clean in ["tyre_stude", "tyr", "stud", "bull", "bulls", "steer", "steers"]:
        # Bulls/steers: High-energy fattening diet
        default_feeds = {
            "majsensilage": 0.35,  # 35% maize silage
            "kløvergræs_/græsensilge_(_>20_%_kløver)": 0.20,  # 20% grass/clover silage
            "kraftfoder_<_25_%_protein": 0.35,  # 35% concentrate
            "sojaprodukter": 0.05,  # 5% protein supplement
            "andet_grovfoder_(roer,_helsæd_mv)": 0.05,  # 5% other roughage
        }
    else:
        raise ValueError(f"Unknown cattle type: {cattle_type}. Must be one of: malkekøer, kvier, tyre_stude")

    # Use custom composition if provided, otherwise use defaults
    feed_mix = feed_composition or default_feeds

    # Validate feed mix sums to ~1.0
    total_share = sum(feed_mix.values())
    if not (0.95 <= total_share <= 1.05):
        raise ValueError(f"Feed composition shares must sum to ~1.0, got {total_share}")

    # Calculate total dry matter per year
    total_ts = antal_dyr * ts_per_day * days_per_year

    # Calculate emissions by feed type
    feed_breakdown = {}
    total_co2e = 0.0

    for feed_type, share in feed_mix.items():
        feed_key = feed_type.lower().replace(" ", "_").replace("-", "_").replace("/", "_")

        if feed_key not in FEED_FACTORS:
            raise ValueError(f"Unknown feed type: {feed_type}. Available: {list(FEED_FACTORS.keys())}")

        ef_g_per_kg_ts = FEED_FACTORS[feed_key]["g_co2e_per_kg_ts"]

        # Calculate emissions for this feed type
        ts_this_type = total_ts * share  # kg TS
        co2e_g_this_type = ts_this_type * ef_g_per_kg_ts  # g CO2e
        co2e_kg_this_type = co2e_g_this_type / 1000  # kg CO2e

        feed_breakdown[feed_type] = {
            "ts_kg": ts_this_type,
            "co2e_kg": co2e_kg_this_type,
            "share": share,
            "ef_g_per_kg_ts": ef_g_per_kg_ts,
        }

        total_co2e += co2e_kg_this_type

    return {
        "co2e_kg": total_co2e,
        "co2e_per_animal": total_co2e / antal_dyr if antal_dyr > 0 else 0.0,
        "total_ts": total_ts,
        "feed_breakdown": feed_breakdown,
    }


def calculate_all_cattle_feed(
    livestock_data: Dict[str, Dict[str, float]],
) -> Dict[str, Dict]:
    """
    Calculate feed emissions for all cattle types in a farm.

    Args:
        livestock_data: Dict mapping cattle type to count and TS intake, e.g.:
            {
                "malkekøer": {"count": 100, "ts_per_day": 23.5},
                "kvier": {"count": 50, "ts_per_day": 7.5},
                "tyre_stude": {"count": 30, "ts_per_day": 8.0}
            }

    Returns:
        Dict with results per animal type and totals:
            {
                "malkekøer": {...},
                "kvier": {...},
                "tyre_stude": {...},
                "total": {"co2e_kg": X}
            }

    Source: KB_21_5397_AP2 pages 24-25, SEGES 2021
    """
    results = {}
    total_co2e = 0.0

    for cattle_type, data in livestock_data.items():
        count = data["count"]
        ts_per_day = data["ts_per_day"]
        days_per_year = data.get("days_per_year", 365)

        if count > 0:
            result = calculate_feed_emissions_kvaeg(
                cattle_type, count, ts_per_day, days_per_year
            )
            results[cattle_type] = result
            total_co2e += result["co2e_kg"]

    results["total"] = {
        "co2e_kg": total_co2e,
    }

    return results


if __name__ == "__main__":
    # Test with example farm
    print("=== Cattle Feed Emissions Test ===\n")
    print("Source: KB_21_5397_AP2 pages 24-25, SEGES 2021")
    print("Feed factors from Table 3\n")

    # Test 1: Dairy cows (23.5 kg TS/day)
    result = calculate_feed_emissions_kvaeg("malkekøer", 100, 23.5)
    print(f"100 dairy cows (23.5 kg TS/day each):")
    print(f"  Total dry matter: {result['total_ts']:.0f} kg TS/year")
    print(f"  Feed emissions: {result['co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per cow: {result['co2e_per_animal']:.2f} kg CO2e/year")
    print(f"  Breakdown:")
    for feed_type, data in result['feed_breakdown'].items():
        print(f"    - {feed_type}: {data['co2e_kg']:.2f} kg CO2e ({data['share']*100:.0f}%)")
    print()

    # Test 2: Heifers (7.5 kg TS/day)
    result = calculate_feed_emissions_kvaeg("kvier", 50, 7.5)
    print(f"50 heifers (7.5 kg TS/day each):")
    print(f"  Total dry matter: {result['total_ts']:.0f} kg TS/year")
    print(f"  Feed emissions: {result['co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per heifer: {result['co2e_per_animal']:.2f} kg CO2e/year")
    print()

    # Test 3: Full farm
    farm_data = {
        "malkekøer": {"count": 100, "ts_per_day": 23.5},
        "kvier": {"count": 50, "ts_per_day": 7.5},
        "tyre_stude": {"count": 30, "ts_per_day": 8.0},
    }
    results = calculate_all_cattle_feed(farm_data)
    print("Full farm (100 dairy cows, 50 heifers, 30 bulls):")
    print(f"  Total feed emissions: {results['total']['co2e_kg']:.2f} kg CO2e/year")
    total_animals = 100 + 50 + 30
    print(f"  Per animal: {results['total']['co2e_kg']/total_animals:.2f} kg CO2e/year")
    print()

    # Show impact on total cattle emissions
    print("Impact Assessment:")
    print("  Feed typically represents 40-50% of total cattle emissions")
    print("  This module adds this critical missing component")
