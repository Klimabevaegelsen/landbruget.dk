"""
Climate Calculator Orchestration Module

This module orchestrates all emission calculations for farm climate impact assessment.
It integrates cattle, field, and other emission sources using existing formula modules.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd

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
            data_loader: Object that loads farm data from various sources
        """
        self.data_loader = data_loader
        # TODO: Load constants from JSON files in reference_values/
        # This will be implemented when integrating with actual data sources

    def calculate_emissions(self, cvr: str, year: int) -> EmissionReport:
        """
        Calculate complete emissions for a farm for a given year.

        Args:
            cvr: Danish company registration number (8 digits)
            year: Year to calculate emissions for

        Returns:
            EmissionReport with all emission categories and metrics
        """
        # 1. Load data from various sources
        livestock_data = self.data_loader.load_livestock(cvr, year)
        field_data = self.data_loader.load_fields(cvr, year)
        fertilizer_data = self.data_loader.load_fertilizer(cvr, year)
        energy_data = self.data_loader.load_energy(cvr, year)

        # 2. Calculate emissions by category
        categories = []

        # Cattle emissions (if applicable)
        if livestock_data is not None and "cattle" in livestock_data:
            cattle_cat = self._calculate_cattle_emissions(livestock_data["cattle"])
            if cattle_cat:
                categories.append(cattle_cat)

        # Field emissions (if applicable)
        if field_data is not None and fertilizer_data is not None:
            field_cat = self._calculate_field_emissions(field_data, fertilizer_data)
            if field_cat:
                categories.append(field_cat)

        # Energy emissions (if applicable)
        if energy_data is not None:
            energy_cat = self._calculate_energy_emissions(energy_data)
            if energy_cat:
                categories.append(energy_cat)

        # 3. Calculate total emissions
        total_co2e_kg = sum(cat.co2e_kg for cat in categories)

        # 4. Calculate intensity metrics
        intensity = self._calculate_intensity_metrics(total_co2e_kg, livestock_data, field_data)

        # 5. Calculate data completeness
        completeness = self._calculate_data_completeness(categories)

        # 6. Return report
        return EmissionReport(
            cvr=cvr,
            year=year,
            total_co2e_kg=total_co2e_kg,
            categories=categories,
            intensity_metrics=intensity,
            data_completeness=completeness,
        )

    def _calculate_cattle_emissions(self, cattle_data: pd.DataFrame) -> Optional[EmissionCategory]:
        """
        Calculate cattle emissions using formulas from formulas/kvaeg/.

        Args:
            cattle_data: DataFrame with cattle herd information

        Returns:
            EmissionCategory for cattle, or None if no data
        """
        if cattle_data is None or cattle_data.empty:
            return None

        # Prepare data structure for enterisk_metan calculation
        dyretype_counts = self._prepare_cattle_data_for_enteric(cattle_data)

        # Calculate enteric methane emissions
        enteric_co2e = enterisk_metan.beregn_co2e_enterisk_kvaeg_total(dyretype_counts)

        # Calculate stald og lager emissions (if ARLA data available)
        stald_lager_co2e = self._calculate_stald_lager(cattle_data)

        # Create category with sub-sources
        cattle_category = EmissionCategory(
            name="cattle",
            co2e_kg=enteric_co2e + stald_lager_co2e,
            data_quality="complete" if self._has_complete_cattle_data(cattle_data) else "estimated",
        )

        cattle_category.add_sub_source("enteric_methane", enteric_co2e)
        cattle_category.add_sub_source("manure_storage", stald_lager_co2e)

        return cattle_category

    def _prepare_cattle_data_for_enteric(self, cattle_data: pd.DataFrame) -> Dict:
        """
        Transform cattle DataFrame into structure needed by enteric methane formulas.

        Args:
            cattle_data: Raw cattle data from data loader

        Returns:
            Dictionary with structure expected by beregn_co2e_enterisk_kvaeg_total
        """
        # TODO: Implement mapping from actual data structure to formula requirements
        # This is a placeholder showing expected structure
        dyretype_counts = {}

        # Example mapping (needs to match actual data structure):
        # if 'malkeko_tung_race' in cattle_data.columns:
        #     dyretype_counts['malkeko_tung_race'] = {
        #         'count': cattle_data['malkeko_tung_race'].iloc[0],
        #         'foderoptag_kg_ts_pr_dag': cattle_data['foderoptag'].iloc[0],
        #         'fedtsyre_g_pr_kg_ts': cattle_data['fedtsyre'].iloc[0],
        #         'ndf_g_pr_kg_ts': cattle_data['ndf'].iloc[0]
        #     }

        return dyretype_counts

    def _calculate_stald_lager(self, cattle_data: pd.DataFrame) -> float:
        """
        Calculate emissions from barn and manure storage.

        Uses ARLA FarmAhead data if available, otherwise estimates.

        Args:
            cattle_data: Cattle data including ARLA metrics if available

        Returns:
            CO2e from stald og lager (kg)
        """
        # Check if ARLA data is available
        if "arla_s_co2e" in cattle_data.columns:
            return stald_og_lager.calculate_co2_stald_lager(
                s_co2e=cattle_data["arla_s_co2e"].iloc[0],
                theta_maelk=cattle_data["arla_theta_maelk"].iloc[0],
                fpcm=cattle_data["arla_fpcm"].iloc[0],
                phi=cattle_data.get("phi", pd.Series([1.05])).iloc[0],
                n_ko=cattle_data["cow_count"].iloc[0],
            )

        # TODO: Implement estimation method if ARLA data not available
        return 0.0

    def _has_complete_cattle_data(self, cattle_data: pd.DataFrame) -> bool:
        """Check if cattle data is complete for accurate calculations."""
        required_cols = ["cow_count", "foderoptag", "fedtsyre", "ndf"]
        return all(col in cattle_data.columns for col in required_cols)

    def _calculate_field_emissions(
        self, field_data: pd.DataFrame, fertilizer_data: pd.DataFrame
    ) -> Optional[EmissionCategory]:
        """
        Calculate field emissions using formulas from formulas/marker/.

        Args:
            field_data: DataFrame with field/crop information
            fertilizer_data: DataFrame with fertilizer applications

        Returns:
            EmissionCategory for fields, or None if no data
        """
        if field_data is None or field_data.empty:
            return None

        total_co2e = 0.0
        field_category = EmissionCategory(name="fields", co2e_kg=0.0, data_quality="complete")

        # Iterate through each field/crop combination
        for idx, field in field_data.iterrows():
            # Get fertilizer data for this field
            field_fertilizer = self._get_field_fertilizer(field, fertilizer_data)

            # Calculate N2O from fertilizer application
            n2o_co2e = self._calculate_field_n2o(field, field_fertilizer)
            total_co2e += n2o_co2e

            # Calculate carbon balance
            c_balance_co2e = self._calculate_field_carbon_balance(field)
            total_co2e += c_balance_co2e

            # Calculate nitrate leaching (indirect N2O)
            leaching_co2e = self._calculate_nitrate_leaching(field, field_fertilizer)
            total_co2e += leaching_co2e

            # Calculate emissions from organic soils (if applicable)
            organic_soil_co2e = self._calculate_organic_soil(field)
            total_co2e += organic_soil_co2e

            # Calculate emissions from liming (if applicable)
            liming_co2e = self._calculate_liming(field)
            total_co2e += liming_co2e

        field_category.co2e_kg = total_co2e
        field_category.add_sub_source("n2o_fertilizer", n2o_co2e)
        field_category.add_sub_source("carbon_balance", c_balance_co2e)
        field_category.add_sub_source("nitrate_leaching", leaching_co2e)
        field_category.add_sub_source("organic_soils", organic_soil_co2e)
        field_category.add_sub_source("liming", liming_co2e)

        return field_category

    def _get_field_fertilizer(self, field: pd.Series, fertilizer_data: pd.DataFrame) -> pd.DataFrame:
        """Extract fertilizer data for specific field."""
        # TODO: Implement field-fertilizer matching logic
        return pd.DataFrame()

    def _calculate_field_n2o(self, field: pd.Series, fertilizer: pd.DataFrame) -> float:
        """
        Calculate N2O emissions from fertilizer application.

        Uses goedning_og_nitrifikationshaemmer module.
        """
        if fertilizer.empty:
            return 0.0

        total_n2o_co2e = 0.0

        # Calculate for each fertilizer type
        for _, fert in fertilizer.iterrows():
            n2o_kg, co2e_kg = goedning_og_nitrifikationshaemmer.calculate_n2o_goedning(
                n_total_kg_ha=fert["n_kg_ha"],
                areal_ha=field["area_ha"],
                goedningstype=fert["type"],  # "handelsgoedning", "husdyrgoedning", "afgraesning"
                n_nitri_kg_ha=fert.get("n_nitri_kg_ha", 0.0),
                handelsgoedning_detail_type=fert.get("detail_type", None),
            )
            total_n2o_co2e += co2e_kg

        return total_n2o_co2e

    def _calculate_field_carbon_balance(self, field: pd.Series) -> float:
        """
        Calculate carbon balance for field.

        Uses kulstofbalance module.
        """
        # Calculate crop residues (needs afgroederester module)
        a_over = 0.0  # TODO: Call afgroederester.calculate_A_over_kg_ts_ha
        a_under = 0.0  # TODO: Call afgroederester.calculate_A_under_kg_ts_ha

        c_afgroederest = kulstofbalance.calculate_C_afgroederest_kg_c_ha(
            a_over_kg_ts_ha=a_over, a_under_kg_ts_ha=a_under
        )

        c_organisk = kulstofbalance.calculate_C_organisk_goedning_kg_c_ha(
            n_hus_plus_afg_kg_n_ha=field.get("n_organic_kg_ha", 0.0)
        )

        co2e = kulstofbalance.calculate_co2e_kulstofbalance_mark(
            r_relativ_faktor=field.get("r_faktor", 1),
            areal_ha=field["area_ha"],
            c_afgroederest_kg_c_ha=c_afgroederest,
            c_organisk_kg_c_ha=c_organisk,
        )

        return co2e

    def _calculate_nitrate_leaching(self, field: pd.Series, fertilizer: pd.DataFrame) -> float:
        """Calculate indirect N2O from nitrate leaching."""
        # TODO: Implement using nitratudvaskning module
        return 0.0

    def _calculate_organic_soil(self, field: pd.Series) -> float:
        """Calculate emissions from organic soils."""
        # TODO: Implement using organogene_jorde module
        return 0.0

    def _calculate_liming(self, field: pd.Series) -> float:
        """Calculate emissions from liming."""
        # TODO: Implement using kalkning module
        return 0.0

    def _calculate_energy_emissions(self, energy_data: pd.DataFrame) -> Optional[EmissionCategory]:
        """
        Calculate emissions from energy use (diesel, electricity).

        Args:
            energy_data: DataFrame with energy consumption

        Returns:
            EmissionCategory for energy, or None if no data
        """
        if energy_data is None or energy_data.empty:
            return None

        # TODO: Implement energy emission calculations
        # Using formulas/import/diesel_maskinarbejde.py and formulas/import/el.py

        energy_category = EmissionCategory(name="energy", co2e_kg=0.0, data_quality="unavailable")

        return energy_category

    def _calculate_intensity_metrics(
        self, total_co2e_kg: float, livestock_data: Optional[pd.DataFrame], field_data: Optional[pd.DataFrame]
    ) -> Dict[str, float]:
        """
        Calculate intensity metrics (emissions per unit production).

        Args:
            total_co2e_kg: Total emissions
            livestock_data: Livestock data (for milk/meat production)
            field_data: Field data (for crop production)

        Returns:
            Dictionary of intensity metrics
        """
        metrics = {}

        # CO2e per kg milk (if dairy farm)
        if livestock_data is not None and "milk_kg" in livestock_data.columns:
            milk_kg = livestock_data["milk_kg"].sum()
            if milk_kg > 0:
                metrics["co2e_per_kg_milk"] = total_co2e_kg / milk_kg

        # CO2e per hectare (if crop farm)
        if field_data is not None and "area_ha" in field_data.columns:
            total_ha = field_data["area_ha"].sum()
            if total_ha > 0:
                metrics["co2e_per_ha"] = total_co2e_kg / total_ha

        # CO2e per animal unit (if livestock farm)
        if livestock_data is not None and "animal_units" in livestock_data.columns:
            animal_units = livestock_data["animal_units"].sum()
            if animal_units > 0:
                metrics["co2e_per_animal_unit"] = total_co2e_kg / animal_units

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
    # This is a placeholder - actual data loader will be implemented separately
    class MockDataLoader:
        def load_livestock(self, cvr: str, year: int):
            return None

        def load_fields(self, cvr: str, year: int):
            return None

        def load_fertilizer(self, cvr: str, year: int):
            return None

        def load_energy(self, cvr: str, year: int):
            return None

    # Example calculation
    calculator = FarmClimateCalculator(MockDataLoader())

    # Test with mock CVR
    report = calculator.calculate_emissions(cvr="12345678", year=2024)

    print(f"Climate Report for CVR {report.cvr}, Year {report.year}")
    print(f"Total CO2e: {report.total_co2e_kg:.2f} kg")
    print(f"Data Completeness: {report.data_completeness:.1%}")
    print(f"\nCategories:")
    for cat in report.categories:
        print(f"  {cat.name}: {cat.co2e_kg:.2f} kg CO2e ({cat.data_quality})")
        for source_name, source_co2e in cat.sub_sources.items():
            print(f"    - {source_name}: {source_co2e:.2f} kg CO2e")

    print(f"\nIntensity Metrics:")
    for metric_name, metric_value in report.intensity_metrics.items():
        print(f"  {metric_name}: {metric_value:.2f}")
