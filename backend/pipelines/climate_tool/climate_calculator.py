"""
Climate Calculator Orchestration Module

This module orchestrates all emission calculations for farm climate impact assessment.
It integrates cattle, field, and other emission sources using existing formula modules.

Data Flow:
1. data_loader.py → Fetch raw data from GCS (Danish schema)
2. data_transformer.py → Transform to structured objects (LivestockSummary, FieldSummary, FertilizerSummary)
3. climate_calculator.py (this file) → Convert to formula module inputs → Calculate emissions
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import logging

# Import data structures from transformer
from data_transformer import (
    LivestockSummary,
    FieldSummary,
    FertilizerSummary,
    IntegratedFarmTransformer,
)

# Import existing formula modules
from formulas.kvaeg import enterisk_metan, stald_og_lager, bedriftsaftryk
from formulas.marker import (
    goedning_og_nitrifikationshaemmer,
    kulstofbalance,
    afgroederester,
    nitratudvaskning,
    organogene_jorde,
    kalkning,
)

logger = logging.getLogger(__name__)


@dataclass
class EmissionCategory:
    """
    Represents emissions from a specific category with data quality tracking.
    """

    name: str
    co2e_kg: float
    data_quality: str  # "complete", "estimated", "unavailable"
    sub_sources: Dict[str, float] = field(default_factory=dict)

    def add_sub_source(self, name: str, co2e_kg: float) -> None:
        """Add a sub-source emission to this category."""
        self.sub_sources[name] = co2e_kg

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "co2e_kg": self.co2e_kg,
            "data_quality": self.data_quality,
            "sub_sources": self.sub_sources,
        }


@dataclass
class EmissionReport:
    """
    Complete emission report for a farm for a specific year.
    """

    cvr: str
    year: int
    total_co2e_kg: float
    categories: List[EmissionCategory]
    intensity_metrics: Dict[str, float]
    data_completeness: float

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "cvr": self.cvr,
            "year": self.year,
            "total_co2e_kg": self.total_co2e_kg,
            "categories": [cat.to_dict() for cat in self.categories],
            "intensity_metrics": self.intensity_metrics,
            "data_completeness": self.data_completeness,
        }

    def get_category(self, name: str) -> Optional[EmissionCategory]:
        """Retrieve a specific emission category by name."""
        for cat in self.categories:
            if cat.name == name:
                return cat
        return None


class FarmClimateCalculator:
    """
    Main orchestrator for farm climate impact calculations.

    Coordinates data loading, emission calculations across all categories,
    and intensity metric computation.
    """

    def __init__(self, data_loader):
        """
        Initialize calculator with data loader.

        Args:
            data_loader: ClimateDataLoader instance that loads farm data from GCS
        """
        self.data_loader = data_loader

    def calculate_emissions(self, cvr: str, year: int) -> EmissionReport:
        """
        Calculate complete emissions for a farm for a given year.

        Args:
            cvr: Danish company registration number (8 digits)
            year: Year to calculate emissions for

        Returns:
            EmissionReport with all emission categories and metrics
        """
        logger.info(f"Calculating emissions for CVR {cvr}, year {year}")

        # 1. Load raw data from GCS
        livestock_df = self.data_loader.load_livestock(cvr, year)
        field_df = self.data_loader.load_fields(cvr, year)
        fertilizer_df = self.data_loader.load_fertilizer(cvr, year)

        # 2. Transform to structured format
        farm_data = IntegratedFarmTransformer.transform_all(livestock_df, field_df, fertilizer_df)

        # 3. Calculate emissions by category
        categories = []

        # Cattle emissions (if applicable)
        if "cattle" in farm_data["livestock"]:
            cattle_cat = self._calculate_cattle_emissions(farm_data["livestock"]["cattle"])
            if cattle_cat:
                categories.append(cattle_cat)
                logger.info(f"Cattle emissions: {cattle_cat.co2e_kg:.1f} kg CO2e")

        # Field emissions (if applicable)
        if farm_data["fertilizer"] is not None and len(farm_data["fields"]) > 0:
            field_cat = self._calculate_field_emissions(farm_data["fields"], farm_data["fertilizer"])
            if field_cat:
                categories.append(field_cat)
                logger.info(f"Field emissions: {field_cat.co2e_kg:.1f} kg CO2e")

        # 4. Calculate total emissions
        total_co2e_kg = sum(cat.co2e_kg for cat in categories)

        # 5. Calculate intensity metrics
        intensity = self._calculate_intensity_metrics(total_co2e_kg, farm_data)

        # 6. Calculate data completeness
        completeness = self._calculate_data_completeness(categories)

        logger.info(f"Total emissions: {total_co2e_kg:.1f} kg CO2e (completeness: {completeness:.1%})")

        # 7. Return report
        return EmissionReport(
            cvr=cvr,
            year=year,
            total_co2e_kg=total_co2e_kg,
            categories=categories,
            intensity_metrics=intensity,
            data_completeness=completeness,
        )

    def _calculate_cattle_emissions(self, cattle_summary: LivestockSummary) -> Optional[EmissionCategory]:
        """
        Calculate cattle emissions using formulas from formulas/kvaeg/.

        Args:
            cattle_summary: LivestockSummary object for cattle (from data_transformer.py)

        Returns:
            EmissionCategory for cattle, or None if no data
        """
        if cattle_summary is None or cattle_summary.total_count == 0:
            return None

        # Prepare data structure for enterisk_metan calculation
        dyretype_counts = self._prepare_cattle_data_for_enteric(cattle_summary)

        try:
            # Calculate enteric methane emissions
            enteric_co2e = enterisk_metan.beregn_co2e_enterisk_kvaeg_total(dyretype_counts)
            logger.debug(f"Enteric methane: {enteric_co2e:.1f} kg CO2e")
        except Exception as e:
            logger.warning(f"Failed to calculate enteric methane: {e}")
            enteric_co2e = 0.0

        # Stald og lager emissions require ARLA data (not available in Green Accounts)
        # TODO: Implement when ARLA FarmAhead data is integrated
        stald_lager_co2e = 0.0

        # Determine data quality
        data_quality = self._assess_cattle_data_quality(cattle_summary, dyretype_counts)

        # Create category with sub-sources
        cattle_category = EmissionCategory(
            name="cattle",
            co2e_kg=enteric_co2e + stald_lager_co2e,
            data_quality=data_quality,
        )

        cattle_category.add_sub_source("enteric_methane", enteric_co2e)
        if stald_lager_co2e > 0:
            cattle_category.add_sub_source("manure_storage", stald_lager_co2e)

        return cattle_category

    def _prepare_cattle_data_for_enteric(self, cattle_summary: LivestockSummary) -> Dict:
        """
        Transform LivestockSummary into structure needed by enteric methane formulas.

        Args:
            cattle_summary: LivestockSummary object for cattle

        Returns:
            Dictionary with structure expected by beregn_co2e_enterisk_kvaeg_total
            Format: {dyretype: {"count": int, "foderoptag_kg_ts_pr_dag": float, ...}, ...}
        """
        dyretype_counts = {}

        # Map cattle subtypes to formula dyretypes
        # Note: Green Accounts doesn't provide feed parameters (foderoptag, fedtsyre, ndf)
        # We'll use default values or flag as estimated
        for subtype, count in cattle_summary.subtypes.items():
            if count == 0:
                continue

            # Map English subtypes to formula dyretypes
            if subtype == "dairy_cows":
                # TODO: Determine if heavy (tung) or Jersey based on data
                # For now, assume heavy race as it's more common
                dyretype_counts["malkeko_tung_race"] = {
                    "count": count,
                    # Default values from reference_values (would need to load)
                    "foderoptag_kg_ts_pr_dag": 23.5,  # Typical for dairy cows
                    "fedtsyre_g_pr_kg_ts": 22.0,
                    "ndf_g_pr_kg_ts": 340.0,
                }
            elif subtype == "heifers":
                # Older heifers (> 6 months)
                dyretype_counts["opdraet_aeldre_tung"] = {
                    "count": count,
                    "foderoptag_kg_ts_pr_dag": 7.5,
                    "kraftfoderandel_procent": 10.0,
                    "fedtsyre_g_pr_kg_ts": 18.0,
                }
            elif subtype == "calves":
                # Young calves (0-6 months) - uses default values
                dyretype_counts["opdraet_0_6mdr_tung"] = {"count": count}
            elif subtype in ["young_bulls", "bulls"]:
                # Bulls for slaughter
                dyretype_counts["tyre_aeldre_tung"] = {
                    "count": count,
                    "foderoptag_kg_ts_pr_dag": 8.0,
                    "kraftfoderandel_procent": 15.0,
                    "fedtsyre_g_pr_kg_ts": 19.0,
                }
            else:
                logger.warning(f"Unknown cattle subtype: {subtype}, skipping enteric calculation")

        return dyretype_counts

    def _assess_cattle_data_quality(self, cattle_summary: LivestockSummary, dyretype_counts: Dict) -> str:
        """
        Assess data quality for cattle emissions calculation.

        Args:
            cattle_summary: LivestockSummary object
            dyretype_counts: Prepared data for formula modules

        Returns:
            Data quality string: "complete", "estimated", or "unavailable"
        """
        # Green Accounts provides animal counts but not feed parameters
        # Feed parameters are estimated using defaults
        if len(dyretype_counts) == 0:
            return "unavailable"

        # If we have counts but using default feed parameters
        return "estimated"

    def _calculate_field_emissions(
        self, field_summaries: List[FieldSummary], fertilizer_summary: FertilizerSummary
    ) -> Optional[EmissionCategory]:
        """
        Calculate field emissions using formulas from formulas/marker/.

        Args:
            field_summaries: List of FieldSummary objects (from data_transformer.py)
            fertilizer_summary: FertilizerSummary object (from data_transformer.py)

        Returns:
            EmissionCategory for fields, or None if no data
        """
        if not field_summaries or fertilizer_summary is None:
            return None

        total_co2e = 0.0
        n2o_fertilizer_total = 0.0
        c_balance_total = 0.0

        # Calculate N2O from fertilizer application (primary emission source)
        try:
            # Use simplified approach: total N applied across all fields
            # Formula expects: n_total_kg_ha, areal_ha, goedningstype
            n2o_kg, n2o_co2e = goedning_og_nitrifikationshaemmer.calculate_n2o_goedning(
                n_total_kg_ha=fertilizer_summary.avg_n_kg_per_ha,
                areal_ha=fertilizer_summary.total_area_ha,
                goedningstype="handelsgoedning",  # Assume commercial fertilizer (default)
                n_nitri_kg_ha=0.0,  # Nitrification inhibitor data not available in GKEA
            )
            n2o_fertilizer_total = n2o_co2e
            total_co2e += n2o_co2e
            logger.debug(f"N2O from fertilizer: {n2o_co2e:.1f} kg CO2e")
        except Exception as e:
            logger.warning(f"Failed to calculate N2O from fertilizer: {e}")

        # Other emission sources require more detailed data (not available in current sources)
        # TODO: Implement when additional data sources are integrated:
        # - Carbon balance: Needs crop residue data
        # - Nitrate leaching: Needs soil type and precipitation data
        # - Organic soils: Needs soil classification data
        # - Liming: Needs liming application data

        field_category = EmissionCategory(
            name="fields",
            co2e_kg=total_co2e,
            data_quality="estimated",  # Using simplified approach without detailed field-level data
        )

        if n2o_fertilizer_total > 0:
            field_category.add_sub_source("n2o_fertilizer", n2o_fertilizer_total)

        return field_category

    def _calculate_intensity_metrics(self, total_co2e_kg: float, farm_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate intensity metrics (emissions per unit production).

        Args:
            total_co2e_kg: Total emissions
            farm_data: Integrated farm data from IntegratedFarmTransformer

        Returns:
            Dictionary of intensity metrics
        """
        metrics = {}

        # CO2e per hectare (if farm has fields)
        total_ha = farm_data["metadata"].get("total_area_ha", 0)
        if total_ha > 0:
            metrics["co2e_per_ha"] = total_co2e_kg / total_ha

        # CO2e per animal (if livestock farm)
        total_animals = sum(summary.total_count for summary in farm_data["livestock"].values())
        if total_animals > 0:
            metrics["co2e_per_animal"] = total_co2e_kg / total_animals

        # TODO: Add more specific metrics when production data is available:
        # - CO2e per kg milk (needs milk production data from ARLA or CHR)
        # - CO2e per kg meat (needs slaughter weight data)
        # - CO2e per kg crop yield (needs yield data)

        return metrics

    def _calculate_data_completeness(self, categories: List[EmissionCategory]) -> float:
        """
        Calculate overall data completeness score.

        Args:
            categories: List of emission categories with data quality flags

        Returns:
            Float between 0 and 1 representing completeness
        """
        if not categories:
            return 0.0

        quality_scores = {"complete": 1.0, "estimated": 0.7, "unavailable": 0.0}

        total_score = sum(quality_scores.get(cat.data_quality, 0.0) for cat in categories)

        return total_score / len(categories)


# Example usage
if __name__ == "__main__":
    """
    Example demonstrating the complete climate calculation workflow.

    Prerequisites:
    - GCS bucket with Danish agricultural data
    - Credentials configured in backend/.env
    - data_loader.py and data_transformer.py modules
    """
    import sys
    from pathlib import Path

    # Add parent directory to path for imports
    parent_dir = Path(__file__).parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))

    from data_loader import ClimateDataLoader

    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Initialize loader with GCS credentials
    loader = ClimateDataLoader()

    # Initialize calculator
    calculator = FarmClimateCalculator(loader)

    # Example CVR (Arla test farm with known data)
    cvr = "31373077"
    year = 2023

    print(f"\n{'=' * 70}")
    print(f"Climate Emission Calculation: CVR {cvr}, Year {year}")
    print(f"{'=' * 70}\n")

    try:
        # Calculate emissions
        report = calculator.calculate_emissions(cvr=cvr, year=year)

        # Display results
        print(f"\n{'=' * 70}")
        print(f"EMISSION REPORT")
        print(f"{'=' * 70}")
        print(f"CVR: {report.cvr}")
        print(f"Year: {report.year}")
        print(f"Total CO2e: {report.total_co2e_kg:,.1f} kg ({report.total_co2e_kg / 1000:.1f} tonnes)")
        print(f"Data Completeness: {report.data_completeness:.1%}")

        print(f"\n{'Emission Categories':-^70}")
        for cat in report.categories:
            print(f"\n{cat.name.upper()}: {cat.co2e_kg:,.1f} kg CO2e ({cat.data_quality})")
            if cat.sub_sources:
                for source_name, source_co2e in cat.sub_sources.items():
                    print(f"  └─ {source_name}: {source_co2e:,.1f} kg CO2e")

        print(f"\n{'Intensity Metrics':-^70}")
        if report.intensity_metrics:
            for metric_name, metric_value in report.intensity_metrics.items():
                metric_display = metric_name.replace("_", " ").title()
                print(f"{metric_display}: {metric_value:.2f}")
        else:
            print("No intensity metrics available (insufficient production data)")

        print(f"\n{'=' * 70}\n")

    except Exception as e:
        logger.error(f"Failed to calculate emissions: {e}")
        import traceback

        traceback.print_exc()
