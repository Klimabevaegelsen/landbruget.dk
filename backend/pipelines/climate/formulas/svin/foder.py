"""
Pig feed emissions (purchased feed).

Based on:
- Table 6: Feed emission factors for pig production
- Lifecycle emissions from feed production and transport
"""

import json
from pathlib import Path


def load_reference_values() -> dict[str, dict[str, float]]:
    """Load feed emission factors from Table 6."""
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(
        base_path / "tabel_6_databehov_ved_beregning_af_indkøbt_foder_svineproduktion_side_30.json"
    ) as f:
        table_6 = json.load(f)

    # Parse Table 6 - Feed emission factors
    feed_factors = {}
    for row in table_6["data"]:
        feed_type = row["Foder_type"]
        feed_factors[feed_type.lower()] = {
            "kg_co2e_per_fe": row["Kg_CO2e_Foderenhed"],
            "default_share_pct": row["Andel_pct"],
        }

    return feed_factors


# Load constants once at module import
FEED_FACTORS = load_reference_values()


def calculate_feed_emissions_svin(
    dyretype: str,
    antal_dyr: float,
    fe_per_animal: float,
    feed_composition: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Calculate emissions from purchased pig feed.

    Formula:
        CO2e (kg) = antal_dyr × FE_per_animal × Σ(share × EF_per_FE)

    Where:
        - FE_per_animal: Feed energy per animal (FE/year)
        - share: Fraction of feed type in diet (0-1)
        - EF_per_FE: Emission factor per feed unit (kg CO2e/FE)

    Args:
        dyretype: Type of pig ('søer', 'smågrise', 'slagtesvin')
        antal_dyr: Number of animals
        fe_per_animal: Feed energy per animal (FE/year)
        feed_composition: Optional custom feed composition as dict mapping
            feed type to share (0-1). If None, uses default composition from Table 6.

    Returns:
        Dict with:
            - co2e_kg: Total feed emissions (kg CO2e/year)
            - co2e_per_animal: Emissions per animal (kg CO2e/year)
            - total_fe: Total feed energy (FE/year)
            - feed_breakdown: Emissions by feed type

    Example:
        >>> # 1000 finishers with 227 FE/year each
        >>> result = calculate_feed_emissions_svin('slagtesvin', 1000, 227)
        >>> print(f"Feed emissions: {result['co2e_kg']:.2f} kg CO2e")
    """
    if antal_dyr <= 0 or fe_per_animal <= 0:
        return {
            "co2e_kg": 0.0,
            "co2e_per_animal": 0.0,
            "total_fe": 0.0,
            "feed_breakdown": {},
        }

    # Normalize animal type
    dyretype_clean = dyretype.lower().strip()

    # Determine default feed types based on animal type
    if dyretype_clean in ["søer", "so", "sow", "sows"]:
        default_feeds = {
            "sofoder": 0.5,  # 50% complete sow feed
            "sotilskudsfoder": 0.2,  # 20% sow supplement
            "korn": 0.3,  # 30% grain
        }
    elif dyretype_clean in ["smågrise", "smågris", "weaner", "weaners"]:
        default_feeds = {
            "smågrisefoder": 0.5,  # 50% complete weaner feed
            "smågrisetilskudsfoder": 0.3,  # 30% weaner supplement
            "korn": 0.2,  # 20% grain
        }
    elif dyretype_clean in ["slagtesvin", "slagtegris", "finisher", "finishers", "frats"]:
        default_feeds = {
            "slagtesvinefoder": 0.5,  # 50% complete finisher feed
            "slagtesvinetilskudsfoder": 0.25,  # 25% finisher supplement
            "korn": 0.25,  # 25% grain
        }
    else:
        raise ValueError(
            f"Unknown pig type: {dyretype}. Must be one of: søer, smågrise, slagtesvin"
        )

    # Use custom composition if provided, otherwise use defaults
    feed_mix = feed_composition or default_feeds

    # Validate feed mix sums to ~1.0
    total_share = sum(feed_mix.values())
    if not (0.95 <= total_share <= 1.05):
        raise ValueError(f"Feed composition shares must sum to ~1.0, got {total_share}")

    # Calculate total feed energy
    total_fe = antal_dyr * fe_per_animal

    # Calculate emissions by feed type
    feed_breakdown = {}
    total_co2e = 0.0

    for feed_type, share in feed_mix.items():
        feed_key = feed_type.lower()

        if feed_key not in FEED_FACTORS:
            raise ValueError(
                f"Unknown feed type: {feed_type}. Available: {list(FEED_FACTORS.keys())}"
            )

        ef_per_fe = FEED_FACTORS[feed_key]["kg_co2e_per_fe"]

        # Calculate emissions for this feed type
        fe_this_type = total_fe * share
        co2e_this_type = fe_this_type * ef_per_fe

        feed_breakdown[feed_type] = {
            "fe": fe_this_type,
            "co2e_kg": co2e_this_type,
            "share": share,
            "ef_per_fe": ef_per_fe,
        }

        total_co2e += co2e_this_type

    return {
        "co2e_kg": total_co2e,
        "co2e_per_animal": total_co2e / antal_dyr if antal_dyr > 0 else 0.0,
        "total_fe": total_fe,
        "feed_breakdown": feed_breakdown,
    }


def calculate_all_pig_feed(
    livestock_data: dict[str, dict[str, float]],
) -> dict[str, dict]:
    """
    Calculate feed emissions for all pig types in a farm.

    Args:
        livestock_data: Dict mapping pig type to count and FE, e.g.:
            {
                "søer": {"count": 200, "fe_per_animal": 1492},
                "smågrise": {"count": 500, "fe_per_animal": 45.4},
                "slagtesvin": {"count": 1500, "fe_per_animal": 227}
            }

    Returns:
        Dict with results per animal type and totals:
            {
                "søer": {...},
                "smågrise": {...},
                "slagtesvin": {...},
                "total": {"co2e_kg": X}
            }
    """
    results = {}
    total_co2e = 0.0

    for pig_type, data in livestock_data.items():
        count = data["count"]
        fe_per_animal = data["fe_per_animal"]

        if count > 0:
            result = calculate_feed_emissions_svin(pig_type, count, fe_per_animal)
            results[pig_type] = result
            total_co2e += result["co2e_kg"]

    results["total"] = {
        "co2e_kg": total_co2e,
    }

    return results


if __name__ == "__main__":
    # Test with example farm
    print("=== Pig Feed Emissions Test ===\n")

    # Test 1: Conventional sows (1492 FE/year)
    result = calculate_feed_emissions_svin("søer", 200, 1492)
    print("200 conventional sows (1492 FE/year each):")
    print(f"  Total feed: {result['total_fe']:.0f} FE")
    print(f"  Feed emissions: {result['co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per sow: {result['co2e_per_animal']:.2f} kg CO2e/year")
    print("  Breakdown:")
    for feed_type, data in result["feed_breakdown"].items():
        print(f"    - {feed_type}: {data['co2e_kg']:.2f} kg CO2e ({data['share'] * 100:.0f}%)")
    print()

    # Test 2: Conventional finishers (82 kg gain × 2.77 FE/kg = 227 FE)
    result = calculate_feed_emissions_svin("slagtesvin", 1000, 227)
    print("1000 conventional finishers (227 FE/year each):")
    print(f"  Total feed: {result['total_fe']:.0f} FE")
    print(f"  Feed emissions: {result['co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per pig: {result['co2e_per_animal']:.2f} kg CO2e/year")
    print()

    # Test 3: Full farm
    farm_data = {
        "søer": {"count": 200, "fe_per_animal": 1492},
        "smågrise": {"count": 500, "fe_per_animal": 45.4},  # 24.3 kg × 1.87 FE/kg
        "slagtesvin": {"count": 1500, "fe_per_animal": 227},  # 82 kg × 2.77 FE/kg
    }
    results = calculate_all_pig_feed(farm_data)
    print("Full farm (200 sows, 500 weaners, 1500 finishers):")
    print(f"  Total feed emissions: {results['total']['co2e_kg']:.2f} kg CO2e/year")
    total_animals = 200 + 500 + 1500
    print(f"  Per animal: {results['total']['co2e_kg'] / total_animals:.2f} kg CO2e/year")
