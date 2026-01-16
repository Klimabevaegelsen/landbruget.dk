"""
Standardfaktorer (Standard Factors) Lookup Module

This module provides access to Danish livestock standard factors for feed intake
and production parameters when farm-specific data is not available.

Source: KB_21_5397_AP2 "Notat beregningsgrundlag for Landbrugets klimaværktøj"
Organization: SEGES Innovation
Date: 2021-10-15

These standardfaktorer represent typical Danish production conditions and are
used as defaults when actual farm data is unavailable.
"""

import json
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class StandardFaktor:
    """
    Represents a standard factor for a specific animal type and production system.

    Attributes:
        fe_value: Feed energy value (FE per year or per kg gain)
        system_type: Production system (e.g., "conventional_indoor", "organic_outdoor_access")
        description: Human-readable description
        source: Source reference with page number
        data_quality: Quality indicator ("standard" for standardfaktorer)
    """
    fe_value: float
    system_type: str
    description: str
    source: str
    data_quality: str = "standard"


class StandardfaktorerLookup:
    """
    Lookup class for accessing Danish livestock standard factors.

    Loads standardfaktorer from JSON reference file and provides
    lookup methods for different animal types and production systems.
    """

    def __init__(self, reference_file: Optional[str] = None):
        """
        Initialize the standardfaktorer lookup.

        Args:
            reference_file: Path to standardfaktorer JSON file.
                          Defaults to reference_values/standardfaktorer_dyr.json
        """
        if reference_file is None:
            module_dir = Path(__file__).parent
            reference_file = module_dir / "reference_values" / "standardfaktorer_dyr.json"

        with open(reference_file, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        self.source_info = {
            "name": self.data.get("table_name"),
            "source": self.data.get("source"),
            "page": self.data.get("source_page"),
            "date": self.data.get("source_date"),
            "organization": self.data.get("source_organization")
        }

    def get_pig_standardfaktor(
        self,
        pig_type: str,
        production_system: str = "conventional"
    ) -> Tuple[Optional[StandardFaktor], Dict[str, any]]:
        """
        Get standardfaktor for a specific pig type and production system.

        Args:
            pig_type: One of "årssøer", "smågrise", "slagtesvin"
            production_system: One of "conventional", "organic", "frats" (finishers only)

        Returns:
            Tuple of (StandardFaktor or None, metadata dict with source info)

        Examples:
            >>> lookup = StandardfaktorerLookup()
            >>> faktor, meta = lookup.get_pig_standardfaktor("årssøer", "conventional")
            >>> print(f"Sows FE: {faktor.fe_value} FE/year")
            Sows FE: 1492 FE/year
            >>> print(f"Source: {meta['source']}")
            Source: KB_21_5397_AP2 page 28, SEGES 2021
        """
        try:
            pig_data = self.data["data"]["grise"][pig_type][production_system]

            # Determine FE value based on data structure
            if "fe_per_year" in pig_data:
                fe_value = pig_data["fe_per_year"]
            elif "fe_per_animal" in pig_data:
                fe_value = pig_data["fe_per_animal"]
            elif "fe_per_kg_gain" in pig_data:
                # For growth animals, return FE per animal if available
                fe_value = pig_data.get("fe_per_animal", pig_data["fe_per_kg_gain"])
            else:
                return None, {"error": "No FE value found in standardfaktor"}

            faktor = StandardFaktor(
                fe_value=fe_value,
                system_type=pig_data["system_type"],
                description=pig_data["description"],
                source=pig_data["source"],
                data_quality="standard"
            )

            # Build metadata
            metadata = {
                "source": pig_data["source"],
                "source_document": self.source_info["source"],
                "source_page": self.source_info["page"],
                "system_type": pig_data["system_type"],
                "data_quality": "standard",
                "note": "Using Danish standardfaktor (national average)"
            }

            # Add additional parameters if present
            if "fe_per_kg_gain" in pig_data:
                metadata["fe_per_kg_gain"] = pig_data["fe_per_kg_gain"]
            if "avg_weight_gain_kg" in pig_data:
                metadata["avg_weight_gain_kg"] = pig_data["avg_weight_gain_kg"]

            return faktor, metadata

        except KeyError as e:
            return None, {
                "error": f"Standardfaktor not found for {pig_type} / {production_system}",
                "exception": str(e)
            }

    def get_cattle_standardfaktor(
        self,
        cattle_type: str,
        breed: str = "heavy_breed",
        production_system: str = "conventional"
    ) -> Tuple[Optional[StandardFaktor], Dict[str, any]]:
        """
        Get standardfaktor for a specific cattle type, breed, and production system.

        Args:
            cattle_type: One of "malkekøer", "kvier", "tyre_stude"
            breed: "heavy_breed" (Holstein) or "jersey"
            production_system: "conventional" or "organic"

        Returns:
            Tuple of (StandardFaktor or None, metadata dict with source info)

        Note:
            Current cattle standardfaktorer are estimates pending extraction
            of Tables 2a-2f from KB_21_5397_AP2. Data quality is marked as "estimated".

        Examples:
            >>> lookup = StandardfaktorerLookup()
            >>> faktor, meta = lookup.get_cattle_standardfaktor("malkekøer", "heavy_breed")
            >>> print(f"Dairy cow TS: {faktor.fe_value} kg TS/day")
            Dairy cow TS: 23.5 kg TS/day
            >>> print(f"Data quality: {meta['data_quality']}")
            Data quality: estimated
        """
        try:
            # Build lookup key
            if cattle_type == "malkekøer":
                key = f"{breed}_{production_system}"
            else:
                key = breed

            cattle_data = self.data["data"]["kvæg"][cattle_type][key]

            # For cattle, TS (dry matter) per day is the standard unit
            fe_value = cattle_data.get("ts_per_day", 0)

            faktor = StandardFaktor(
                fe_value=fe_value,
                system_type=cattle_data["system_type"],
                description=cattle_data["description"],
                source=cattle_data["source"],
                data_quality=cattle_data.get("data_quality", "standard")
            )

            metadata = {
                "source": cattle_data["source"],
                "source_document": self.source_info["source"],
                "system_type": cattle_data["system_type"],
                "data_quality": cattle_data.get("data_quality", "standard"),
                "unit": "kg TS/day"
            }

            # Add milk production if available
            if "milk_production_kg_per_year" in cattle_data:
                metadata["milk_production_kg_per_year"] = cattle_data["milk_production_kg_per_year"]

            # Add warning if estimated
            if cattle_data.get("data_quality") == "estimated":
                metadata["warning"] = "Using estimated value - awaiting Tables 2a-2f extraction from KB_21_5397_AP2"

            return faktor, metadata

        except KeyError as e:
            return None, {
                "error": f"Standardfaktor not found for {cattle_type} / {breed} / {production_system}",
                "exception": str(e)
            }

    def get_feed_energy_conversion(
        self,
        animal_category: str,
        animal_type: str
    ) -> Tuple[Optional[float], Dict[str, any]]:
        """
        Get MJ per FE conversion factor for a specific animal type.

        Args:
            animal_category: "pigs" or "cattle"
            animal_type: Specific type (e.g., "sows", "finishers", "dairy_cows")

        Returns:
            Tuple of (MJ per FE value or None, metadata dict)

        Examples:
            >>> lookup = StandardfaktorerLookup()
            >>> mj_per_fe, meta = lookup.get_feed_energy_conversion("pigs", "sows")
            >>> print(f"Sows: {mj_per_fe} MJ/FE")
            Sows: 17.5 MJ/FE
        """
        try:
            conversion_data = self.data["feed_energy_conversion"][animal_category][animal_type]
            mj_per_fe = conversion_data["mj_per_fe"]

            metadata = {
                "source": conversion_data["source"],
                "description": conversion_data["description"],
                "data_quality": "standard"
            }

            return mj_per_fe, metadata

        except KeyError as e:
            return None, {
                "error": f"Feed energy conversion not found for {animal_category} / {animal_type}",
                "exception": str(e)
            }

    def list_available_pig_types(self) -> list:
        """
        List all available pig types in standardfaktorer.

        Returns:
            List of pig type keys
        """
        return list(self.data["data"]["grise"].keys())

    def list_available_cattle_types(self) -> list:
        """
        List all available cattle types in standardfaktorer.

        Returns:
            List of cattle type keys
        """
        return list(self.data["data"]["kvæg"].keys())

    def get_source_info(self) -> Dict[str, str]:
        """
        Get information about the standardfaktorer source document.

        Returns:
            Dictionary with source information
        """
        return self.source_info.copy()


# Convenience functions for direct access

_lookup = None

def get_lookup() -> StandardfaktorerLookup:
    """
    Get or create singleton standardfaktorer lookup instance.

    Returns:
        StandardfaktorerLookup instance
    """
    global _lookup
    if _lookup is None:
        _lookup = StandardfaktorerLookup()
    return _lookup


def lookup_pig_fe(pig_type: str, production_system: str = "conventional") -> Tuple[Optional[float], Dict]:
    """
    Convenience function to lookup pig FE value.

    Args:
        pig_type: "årssøer", "smågrise", or "slagtesvin"
        production_system: "conventional", "organic", or "frats"

    Returns:
        Tuple of (FE value or None, metadata dict)

    Examples:
        >>> fe, meta = lookup_pig_fe("årssøer", "organic")
        >>> print(f"Organic sows: {fe} FE/year from {meta['source']}")
        Organic sows: 1843 FE/year from KB_21_5397_AP2 page 28, SEGES 2021
    """
    lookup = get_lookup()
    faktor, metadata = lookup.get_pig_standardfaktor(pig_type, production_system)
    if faktor:
        return faktor.fe_value, metadata
    return None, metadata


def lookup_cattle_ts(cattle_type: str, breed: str = "heavy_breed",
                      production_system: str = "conventional") -> Tuple[Optional[float], Dict]:
    """
    Convenience function to lookup cattle TS (dry matter) per day.

    Args:
        cattle_type: "malkekøer", "kvier", or "tyre_stude"
        breed: "heavy_breed" or "jersey"
        production_system: "conventional" or "organic"

    Returns:
        Tuple of (kg TS/day or None, metadata dict)

    Examples:
        >>> ts, meta = lookup_cattle_ts("malkekøer", "jersey")
        >>> print(f"Jersey dairy: {ts} kg TS/day")
        Jersey dairy: 18.5 kg TS/day
    """
    lookup = get_lookup()
    faktor, metadata = lookup.get_cattle_standardfaktor(cattle_type, breed, production_system)
    if faktor:
        return faktor.fe_value, metadata
    return None, metadata


# Example usage and testing
if __name__ == "__main__":
    print("=== Standardfaktorer Lookup Module ===\n")

    lookup = StandardfaktorerLookup()

    # Show source info
    print("Source Information:")
    source_info = lookup.get_source_info()
    for key, value in source_info.items():
        print(f"  {key}: {value}")
    print()

    # Test pig lookups
    print("Pig Standardfaktorer:")
    for pig_type in lookup.list_available_pig_types():
        for system in ["conventional", "organic"]:
            faktor, meta = lookup.get_pig_standardfaktor(pig_type, system)
            if faktor:
                print(f"  {pig_type} ({system}): {faktor.fe_value} FE")
                print(f"    Source: {meta['source']}")
                print(f"    Quality: {meta['data_quality']}")
    print()

    # Test cattle lookups
    print("Cattle Standardfaktorer:")
    for cattle_type in lookup.list_available_cattle_types():
        faktor, meta = lookup.get_cattle_standardfaktor(cattle_type)
        if faktor:
            print(f"  {cattle_type}: {faktor.fe_value} kg TS/day")
            print(f"    Source: {meta['source']}")
            print(f"    Quality: {meta['data_quality']}")
            if "warning" in meta:
                print(f"    ⚠️  {meta['warning']}")
    print()

    # Test convenience functions
    print("Convenience Function Tests:")
    fe, meta = lookup_pig_fe("årssøer", "organic")
    print(f"  Organic sows FE: {fe}")

    ts, meta = lookup_cattle_ts("malkekøer", "heavy_breed")
    print(f"  Dairy cow TS: {ts} kg/day")
    print(f"  Data quality: {meta['data_quality']}")
