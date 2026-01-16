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
from formulas.kvaeg import enterisk_metan
from formulas.kvaeg.foder import calculate_feed_emissions_kvaeg
from formulas.kvaeg.ammoniak import calculate_nh3_emissions_kvaeg
from formulas.svin import (
    calculate_ch4_enteric_svin,
    calculate_manure_emissions_svin,
    calculate_feed_emissions_svin,
)
from formulas.svin.ammoniak import calculate_nh3_emissions_svin
from formulas.marker import (
    goedning_og_nitrifikationshaemmer,
    kulstofbalance,
    afgroederester,
    nitratudvaskning,
    organogene_jorde,
    kalkning,
)
from formulas.marker.ammoniak import calculate_nh3_field_per_ha

# Import standardfaktorer module for feed intake lookups
from standardfaktorer import lookup_pig_fe, lookup_cattle_ts

logger = logging.getLogger(__name__)


@dataclass
class EmissionCategory:
    """
    Represents emissions from a specific category with data quality tracking.

    Includes both GHG emissions (CO2e) and environmental emissions (NH3).
    """

    name: str
    co2e_kg: float
    data_quality: str  # "measured", "calculated", "standard", "estimated", "unavailable"
    sub_sources: Dict[str, float] = field(default_factory=dict)
    nh3_kg: float = 0.0  # NH3 emissions (kg NH3) - not converted to CO2e
    nh3_sub_sources: Dict[str, float] = field(default_factory=dict)

    def add_sub_source(self, name: str, co2e_kg: float) -> None:
        """Add a GHG sub-source emission to this category."""
        self.sub_sources[name] = co2e_kg

    def add_nh3_sub_source(self, name: str, nh3_kg: float) -> None:
        """Add an NH3 sub-source emission to this category."""
        self.nh3_sub_sources[name] = nh3_kg
        self.nh3_kg += nh3_kg

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = {
            "name": self.name,
            "co2e_kg": self.co2e_kg,
            "data_quality": self.data_quality,
            "sub_sources": self.sub_sources,
        }
        if self.nh3_kg > 0:
            result["nh3_kg"] = self.nh3_kg
            result["nh3_sub_sources"] = self.nh3_sub_sources
        return result


@dataclass
class EmissionReport:
    """
    Complete emission report for a farm for a specific year.

    Includes both GHG emissions (CO2e) and environmental emissions (NH3).
    """

    cvr: str
    year: int
    total_co2e_kg: float
    categories: List[EmissionCategory]
    intensity_metrics: Dict[str, float]
    data_completeness: float
    total_nh3_kg: float = 0.0  # Total NH3 emissions (kg NH3) - not converted to CO2e

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = {
            "cvr": self.cvr,
            "year": self.year,
            "total_co2e_kg": self.total_co2e_kg,
            "categories": [cat.to_dict() for cat in self.categories],
            "intensity_metrics": self.intensity_metrics,
            "data_completeness": self.data_completeness,
        }
        if self.total_nh3_kg > 0:
            result["total_nh3_kg"] = self.total_nh3_kg
            result["environmental_note"] = "NH3 emissions contribute to acidification and eutrophication, not climate change directly"
        return result

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

        # 1b. Check organic certification
        organic_status = self.data_loader.load_organic_certification(cvr, year)

        # 2. Transform to structured format with organic status
        farm_data = IntegratedFarmTransformer.transform_all(
            livestock_df, field_df, fertilizer_df, organic_status
        )

        # 3. Calculate emissions by category
        categories = []

        # Cattle emissions (if applicable)
        if "cattle" in farm_data["livestock"]:
            cattle_cat = self._calculate_cattle_emissions(farm_data["livestock"]["cattle"], cvr, year)
            if cattle_cat:
                categories.append(cattle_cat)
                logger.info(f"Cattle emissions: {cattle_cat.co2e_kg:.1f} kg CO2e")

        # Pig emissions (if applicable)
        if "pigs" in farm_data["livestock"]:
            pig_cat = self._calculate_pig_emissions(farm_data["livestock"]["pigs"])
            if pig_cat:
                categories.append(pig_cat)
                logger.info(f"Pig emissions: {pig_cat.co2e_kg:.1f} kg CO2e")

        # Field emissions (if applicable)
        # Calculate even without fertilizer data - crop residues, carbon balance, and liming can still be computed
        if len(farm_data["fields"]) > 0:
            field_cat = self._calculate_field_emissions(farm_data["fields"], farm_data["fertilizer"], cvr, year)
            if field_cat:
                categories.append(field_cat)
                logger.info(f"Field emissions: {field_cat.co2e_kg:.1f} kg CO2e")

        # 4. Calculate total emissions
        total_co2e_kg = sum(cat.co2e_kg for cat in categories)

        # 5. Calculate intensity metrics
        intensity = self._calculate_intensity_metrics(total_co2e_kg, farm_data)

        # 6. Calculate data completeness
        completeness = self._calculate_data_completeness(categories)

        # 7. Calculate total NH3 emissions across all categories
        total_nh3_kg = sum(cat.nh3_kg for cat in categories)

        logger.info(f"Total emissions: {total_co2e_kg:.1f} kg CO2e (completeness: {completeness:.1%})")
        if total_nh3_kg > 0:
            logger.info(f"Total NH3 emissions: {total_nh3_kg:.1f} kg NH3 (acidification/eutrophication)")

        # 8. Return report
        return EmissionReport(
            cvr=cvr,
            year=year,
            total_co2e_kg=total_co2e_kg,
            categories=categories,
            intensity_metrics=intensity,
            data_completeness=completeness,
            total_nh3_kg=total_nh3_kg,
        )

    def _calculate_cattle_emissions(self, cattle_summary: LivestockSummary, cvr: str, year: int) -> Optional[EmissionCategory]:
        """
        Calculate cattle emissions using formulas from formulas/kvaeg/.

        Args:
            cattle_summary: LivestockSummary object for cattle (from data_transformer.py)
            cvr: Company CVR number (for breed detection)
            year: Year (for breed detection)

        Returns:
            EmissionCategory for cattle, or None if no data
        """
        if cattle_summary is None or cattle_summary.total_count == 0:
            return None

        # Detect breed from CHR data (Jersey vs heavy breed)
        is_jersey = self._detect_jersey_breed(cvr, year)
        if is_jersey:
            logger.info("Detected Jersey cattle breed from CHR data")
        else:
            logger.debug("Using heavy breed cattle (default or detected from CHR)")

        # Prepare data structure for enterisk_metan calculation
        dyretype_counts = self._prepare_cattle_data_for_enteric(cattle_summary, is_jersey)

        try:
            # Calculate enteric methane emissions
            enteric_co2e = enterisk_metan.beregn_co2e_enterisk_kvaeg_total(dyretype_counts)
            logger.debug(f"Enteric methane: {enteric_co2e:.1f} kg CO2e")
        except Exception as e:
            logger.warning(f"Failed to calculate enteric methane: {e}")
            enteric_co2e = 0.0

        # Calculate feed production emissions
        # Source: KB_21_5397_AP2 pages 24-25, SEGES 2021
        total_feed_co2e = 0.0
        production_system = cattle_summary.production_system  # Get from organic detection
        for subtype, count in cattle_summary.subtypes.items():
            if count == 0:
                continue

            # Map subtypes to cattle types and get feed intake from standardfaktorer
            # Pass production_system to lookup for organic/conventional differentiation
            breed_str = "jersey" if is_jersey else "heavy_breed"
            if subtype == "dairy_cows":
                ts_per_day, meta = lookup_cattle_ts("malkekøer", breed_str, production_system)
                cattle_type = "malkekøer"
            elif subtype == "heifers":
                ts_per_day, meta = lookup_cattle_ts("kvier", breed_str, production_system)
                cattle_type = "kvier"
            elif subtype in ["young_bulls", "bulls"]:
                ts_per_day, meta = lookup_cattle_ts("tyre_stude", breed_str, production_system)
                cattle_type = "tyre_stude"
            elif subtype == "calves":
                # Calves have minimal purchased feed (mostly milk replacer)
                # Skip for now as not in standardfaktorer
                continue
            else:
                logger.warning(f"Unknown cattle subtype for feed: {subtype}")
                continue

            if ts_per_day is None:
                logger.warning(f"Failed to lookup TS intake for {subtype}: {meta}")
                continue

            logger.debug(f"Cattle {subtype} TS from standardfaktorer: {ts_per_day} kg/day (source: {meta.get('source', 'unknown')})")

            try:
                feed_result = calculate_feed_emissions_kvaeg(
                    cattle_type=cattle_type,
                    antal_dyr=count,
                    ts_per_day=ts_per_day,
                )
                total_feed_co2e += feed_result["co2e_kg"]
                logger.debug(f"  {subtype} feed: {feed_result['co2e_kg']:.1f} kg CO2e ({count} animals)")
            except Exception as e:
                logger.warning(f"Failed to calculate feed emissions for {subtype}: {e}")

        # Stald og lager emissions require ARLA data (not available in Green Accounts)
        stald_lager_co2e = 0.0

        # Calculate NH3 emissions from manure management
        # Source: SR671 2023 national data (DCE Aarhus University, 2025)
        total_nh3_kg = 0.0
        for subtype, count in cattle_summary.subtypes.items():
            if count == 0:
                continue

            # Map subtypes to NH3 calculation types
            if subtype == "dairy_cows":
                cattle_nh3_type = "malkekøer"
            elif subtype == "heifers":
                cattle_nh3_type = "kvier"  # Will map to opdræt_6_mdr_kælvning
            elif subtype in ["young_bulls", "bulls"]:
                cattle_nh3_type = "tyre_stude"  # Will map to tyre_6_mdr_slagtning
            elif subtype == "calves":
                cattle_nh3_type = "opdræt_0_6_mdr"
            else:
                logger.warning(f"Unknown cattle subtype for NH3: {subtype}")
                continue

            try:
                nh3_result = calculate_nh3_emissions_kvaeg(
                    cattle_type=cattle_nh3_type,
                    antal_dyr=count,
                )
                total_nh3_kg += nh3_result["nh3_kg"]
                logger.debug(f"  {subtype} NH3: {nh3_result['nh3_kg']:.1f} kg NH3 ({count} animals)")
            except Exception as e:
                logger.warning(f"Failed to calculate NH3 emissions for {subtype}: {e}")

        # Determine data quality
        data_quality = self._assess_cattle_data_quality(cattle_summary, dyretype_counts)

        # Create category with sub-sources
        cattle_category = EmissionCategory(
            name="cattle",
            co2e_kg=enteric_co2e + stald_lager_co2e + total_feed_co2e,
            data_quality=data_quality,
            nh3_kg=total_nh3_kg,
        )

        cattle_category.add_sub_source("enteric_methane", enteric_co2e)
        if stald_lager_co2e > 0:
            cattle_category.add_sub_source("manure_storage", stald_lager_co2e)
        if total_feed_co2e > 0:
            cattle_category.add_sub_source("feed_production", total_feed_co2e)
        if total_nh3_kg > 0:
            cattle_category.add_nh3_sub_source("manure_management_nh3", total_nh3_kg)

        return cattle_category

    def _detect_jersey_breed(self, cvr: str, year: int) -> bool:
        """
        Detect if farm has primarily Jersey cattle breed from CHR data.

        Args:
            cvr: Company CVR number
            year: Year

        Returns:
            True if Jersey breed detected (>50% of dairy cows), False otherwise
        """
        try:
            breeds_df = self.data_loader.load_cattle_breeds(cvr, year)
            if breeds_df.empty:
                return False

            # Check if Jersey is the dominant breed
            total_animals = breeds_df['animal_count'].sum()
            jersey_count = breeds_df[breeds_df['breed'].str.contains('Jersey', case=False, na=False)]['animal_count'].sum()

            if jersey_count > 0:
                jersey_ratio = jersey_count / total_animals
                logger.debug(f"Jersey breed ratio: {jersey_ratio:.1%} ({jersey_count}/{total_animals})")
                return jersey_ratio > 0.5

            return False
        except Exception as e:
            logger.debug(f"Could not detect breed from CHR: {e}")
            return False

    def _prepare_cattle_data_for_enteric(self, cattle_summary: LivestockSummary, is_jersey: bool = False) -> Dict:
        """
        Transform LivestockSummary into structure needed by enteric methane formulas.

        Args:
            cattle_summary: LivestockSummary object for cattle
            is_jersey: True if Jersey breed, False for heavy breed

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
                # Use breed detection from CHR data
                if is_jersey:
                    dyretype_counts["malkeko_jersey"] = {
                        "count": count,
                        "foderoptag_kg_ts_pr_dag": 19.0,  # Jersey cows eat less
                        "fedtsyre_g_pr_kg_ts": 22.0,
                        "ndf_g_pr_kg_ts": 340.0,
                    }
                else:
                    dyretype_counts["malkeko_tung_race"] = {
                        "count": count,
                        "foderoptag_kg_ts_pr_dag": 23.5,  # Heavy breed
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
            Data quality string: "measured", "calculated", "standard", "estimated", or "unavailable"
        """
        if len(dyretype_counts) == 0:
            return "unavailable"

        # Green Accounts provides measured animal counts
        # Feed parameters use KB_21_5397_AP2 Standardfaktorer (Tables 2a-2f, SEGES 2021)
        # Quality upgraded from "estimated" to "standard" as of 2026-01-11
        return "standard"

    def _calculate_pig_emissions(self, pig_summary: LivestockSummary) -> Optional[EmissionCategory]:
        """
        Calculate pig emissions using formulas from formulas/svin/.

        Args:
            pig_summary: LivestockSummary object for pigs (from data_transformer.py)

        Returns:
            EmissionCategory for pigs, or None if no data
        """
        if pig_summary is None or pig_summary.total_count == 0:
            return None

        total_enteric_co2e = 0.0
        total_manure_co2e = 0.0
        total_feed_co2e = 0.0
        total_nh3_kg = 0.0

        # Process each pig subtype
        for subtype, count in pig_summary.subtypes.items():
            if count == 0:
                continue

            try:
                # Map English subtypes to Danish pig types and determine parameters
                # Use production_system from pig_summary (set by organic detection)
                dyretype, is_organic, avg_weight, fe_per_animal = self._map_pig_subtype(
                    subtype, count, pig_summary.production_system
                )

                # Calculate enteric fermentation CH4
                enteric_result = calculate_ch4_enteric_svin(
                    dyretype=dyretype,
                    antal_dyr=count,
                    is_organic=is_organic,
                )
                total_enteric_co2e += enteric_result["co2e_kg"]

                # Calculate manure emissions (CH4 + N2O)
                # Default to slurry system with slatted floor (most common)
                manure_result = calculate_manure_emissions_svin(
                    antal_dyr=count,
                    average_body_weight_kg=avg_weight,
                    housing_system="gylle",
                    housing_type="Sengestald med spalter (kanal, bagskyl eller ringkanal)",
                )
                total_manure_co2e += manure_result["total_co2e_kg"]

                # Calculate feed emissions
                feed_result = calculate_feed_emissions_svin(
                    dyretype=dyretype,
                    antal_dyr=count,
                    fe_per_animal=fe_per_animal,
                )
                total_feed_co2e += feed_result["co2e_kg"]

                # Calculate NH3 emissions from manure management
                # Source: SR671 2023 national data (DCE Aarhus University, 2025)
                nh3_result = calculate_nh3_emissions_svin(
                    dyretype=dyretype,
                    antal_dyr=count,
                )
                total_nh3_kg += nh3_result["nh3_kg"]

                logger.debug(
                    f"Pig {subtype} ({count} animals): "
                    f"enteric={enteric_result['co2e_kg']:.1f}, "
                    f"manure={manure_result['total_co2e_kg']:.1f}, "
                    f"feed={feed_result['co2e_kg']:.1f} kg CO2e, "
                    f"nh3={nh3_result['nh3_kg']:.2f} kg NH3"
                )

            except Exception as e:
                logger.warning(f"Failed to calculate emissions for pig subtype {subtype}: {e}")
                continue

        # Create category with sub-sources
        pig_category = EmissionCategory(
            name="pigs",
            co2e_kg=total_enteric_co2e + total_manure_co2e + total_feed_co2e,
            data_quality="standard",  # Using KB_21_5397_AP2 Standardfaktorer (SEGES 2021)
            nh3_kg=total_nh3_kg,
        )

        pig_category.add_sub_source("enteric_methane", total_enteric_co2e)
        pig_category.add_sub_source("manure_management", total_manure_co2e)
        pig_category.add_sub_source("feed_production", total_feed_co2e)
        if total_nh3_kg > 0:
            pig_category.add_nh3_sub_source("manure_management_nh3", total_nh3_kg)

        return pig_category

    def _map_pig_subtype(self, subtype: str, count: int, production_system: str = "conventional") -> tuple[str, bool, float, float]:
        """
        Map English pig subtype to Danish type with parameters using Standardfaktorer.

        Now uses KB_21_5397_AP2 Standardfaktorer (page 28, SEGES 2021) for accurate
        feed intake values instead of hardcoded defaults.

        Args:
            subtype: English subtype name (e.g., "finishers", "sows")
            count: Number of animals
            production_system: Production system ("conventional" or "organic") - from organic detection

        Returns:
            Tuple of (dyretype, is_organic, avg_weight_kg, fe_per_animal)

        Source: KB_21_5397_AP2 page 28 - Standardfaktorer for Danish livestock
        """
        # Use production_system from organic detection (already determined)
        is_organic = (production_system == "organic")

        # Map subtypes and lookup standardfaktorer
        if subtype in ["sows", "søer", "breeding_sows"]:
            fe_value, meta = lookup_pig_fe("årssøer", production_system)
            if fe_value is None:
                logger.error(f"Failed to lookup standardfaktor for sows: {meta}")
                fe_value = 1492  # Fallback
            logger.debug(f"Sows FE from standardfaktorer: {fe_value} (source: {meta.get('source', 'unknown')})")
            return (
                "søer",
                is_organic,
                220.0,  # Average sow weight (kg)
                fe_value,  # FE per year from standardfaktorer
            )
        elif subtype in ["weaners", "smågrise", "piglets"]:
            fe_value, meta = lookup_pig_fe("smågrise", production_system)
            if fe_value is None:
                logger.error(f"Failed to lookup standardfaktor for weaners: {meta}")
                avg_weight_gain = 24.3 if not is_organic else 16.0
                fe_per_kg = 1.87 if not is_organic else 2.11
                fe_value = avg_weight_gain * fe_per_kg
            logger.debug(f"Weaners FE from standardfaktorer: {fe_value} (source: {meta.get('source', 'unknown')})")
            return (
                "smågrise",
                is_organic,
                30.0,  # Average weaner weight (kg)
                fe_value,  # FE per animal from standardfaktorer
            )
        elif subtype in ["finishers", "slagtesvin", "slaughter_pigs", "fattening_pigs"]:
            fe_value, meta = lookup_pig_fe("slagtesvin", production_system)
            if fe_value is None:
                logger.error(f"Failed to lookup standardfaktor for finishers: {meta}")
                fe_value = 227.0  # Fallback
            logger.debug(f"Finishers FE from standardfaktorer: {fe_value} (source: {meta.get('source', 'unknown')})")
            return (
                "slagtesvin",
                is_organic,
                100.0,  # Average finisher weight (kg)
                fe_value,  # FE per animal from standardfaktorer
            )
        else:
            # Default to finishers if unknown
            logger.warning(f"Unknown pig subtype: {subtype}, defaulting to finishers")
            fe_value, _ = lookup_pig_fe("slagtesvin", "conventional")
            return ("slagtesvin", False, 100.0, fe_value or 227.0)

    def _calculate_field_emissions(
        self, field_summaries: List[FieldSummary], fertilizer_summary: FertilizerSummary,
        cvr: str, year: int
    ) -> Optional[EmissionCategory]:
        """
        Calculate field emissions using formulas from formulas/marker/.

        Args:
            field_summaries: List of FieldSummary objects (from data_transformer.py)
            fertilizer_summary: FertilizerSummary object (from data_transformer.py)
            cvr: Company CVR number (needed to load NLES5 and lavbundsjord data)
            year: Year (needed to load NLES5 and lavbundsjord data)

        Returns:
            EmissionCategory for fields, or None if no data
        """
        if not field_summaries:
            return None

        total_co2e = 0.0
        n2o_fertilizer_total = 0.0
        n2o_leaching_total = 0.0
        lavbundsjord_total = 0.0
        crop_residue_total = 0.0
        carbon_balance_total = 0.0
        liming_total = 0.0
        nles5_df = None  # Initialize for later use in data quality check

        # Calculate N2O from fertilizer application (primary emission source)
        # Only if fertilizer data is available
        if fertilizer_summary is not None:
            try:
                n2o_kg, n2o_co2e = goedning_og_nitrifikationshaemmer.calculate_n2o_goedning(
                    n_total_kg_ha=fertilizer_summary.avg_n_kg_per_ha,
                    areal_ha=fertilizer_summary.total_area_ha,
                    goedningstype="handelsgoedning",
                    n_nitri_kg_ha=0.0,
                )
                n2o_fertilizer_total = n2o_co2e
                total_co2e += n2o_co2e
                logger.debug(f"N2O from fertilizer application: {n2o_co2e:.1f} kg CO2e")
            except Exception as e:
                logger.warning(f"Failed to calculate N2O from fertilizer: {e}")
        else:
            logger.debug("No fertilizer data available - skipping N2O fertilizer calculation")

        # Calculate N2O from nitrate leaching using ACTUAL NLES5 nitrogen washout data
        try:
            nles5_df = self.data_loader.load_nles5_nitrogen(cvr, year)

            if not nles5_df.empty:
                # Use actual calculated nitrogen washout values from NLES5 model
                for _, row in nles5_df.iterrows():
                    nitrogen_washout_kg_ha = row.get('nitrogen_washout_kg_ha', 0.0)
                    area_ha = row.get('area_ha', 0.0)

                    if nitrogen_washout_kg_ha > 0 and area_ha > 0:
                        n2o_kg, co2e_kg = nitratudvaskning.calculate_n2o_nitratudvaskning(
                            t=nitrogen_washout_kg_ha,
                            h=area_ha
                        )
                        n2o_leaching_total += co2e_kg

                total_co2e += n2o_leaching_total
                logger.debug(f"N2O from nitrate leaching (NLES5): {n2o_leaching_total:.1f} kg CO2e from {len(nles5_df)} fields")
            else:
                logger.debug(f"No NLES5 nitrogen data available for CVR {cvr}, year {year} - nitrate leaching = 0")
        except Exception as e:
            logger.warning(f"Failed to calculate nitrate leaching from NLES5 data: {e}")

        # Calculate CO2 from lavbundsjord (organic/peat soils) using actual wetland data
        try:
            lavbundsjord_df = self.data_loader.load_lavbundsjord(cvr, year)

            if not lavbundsjord_df.empty:
                # Use actual carbon percentage data from wetlands intersections
                for _, row in lavbundsjord_df.iterrows():
                    area_ha = row.get('area_ha', 0.0)
                    carbon_pct = row.get('carbon_pct', '')

                    if area_ha > 0 and carbon_pct in ['6-12%', '>12%']:
                        # Calculate emissions using actual formula
                        co2e_kg = organogene_jorde.calculate_co2_organogene_jorde(
                            h=area_ha,
                            i_omdrift=True,  # Assume in rotation (default conservative)
                            lav_vandstand=False,  # Assume normal water level
                            kulstof_percentage=carbon_pct
                        )
                        lavbundsjord_total += co2e_kg

                total_co2e += lavbundsjord_total
                logger.debug(f"CO2 from lavbundsjord: {lavbundsjord_total:.1f} kg CO2e from {len(lavbundsjord_df)} intersections")
            else:
                logger.debug(f"No lavbundsjord data available for CVR {cvr}, year {year}")
        except Exception as e:
            logger.warning(f"Failed to calculate lavbundsjord emissions: {e}")

        # Calculate N2O from crop residues using crop_code-based parameters
        # Only calculates for crops with known parameters (exact match by crop_code)
        total_area_ha = 0.0
        crops_with_residues = 0
        try:
            for field in field_summaries:
                total_area_ha += field.total_area_ha

                if field.crop_code is None:
                    continue

                # Use afgroederester convenience function for crop residue N2O
                result = afgroederester.calculate_crop_residue_emissions(
                    crop_code=field.crop_code,
                    yield_kg_ha=field.avg_yield_kg_ha or 5000,  # Default yield if not available
                    area_ha=field.total_area_ha,
                    straw_incorporated=True,  # Default: straw ploughed in
                    yield_incorporated=False,  # Default: harvest removed
                )

                if result is not None:
                    a_over, a_under, n_total, co2e = result
                    crop_residue_total += co2e
                    crops_with_residues += 1

                    # Calculate carbon balance for this field
                    c_residue = kulstofbalance.calculate_C_afgroederest_kg_c_ha(a_over, a_under)

                    # Default N from organic fertilizer (husdyrgødning) - use avg from fertilizer summary
                    # This is N from animal manure applied to fields
                    if fertilizer_summary is not None:
                        n_hus_kg_ha = fertilizer_summary.avg_n_kg_per_ha * 0.3  # ~30% from organic sources
                    else:
                        # Default to 0 if no fertilizer data - conservative estimate
                        n_hus_kg_ha = 0.0

                    c_organic = kulstofbalance.calculate_C_organisk_goedning_kg_c_ha(n_hus_kg_ha)

                    # Calculate carbon balance CO2e (R=1 for most crops = relative to DK average)
                    # Note: Negative values indicate carbon sequestration
                    co2e_carbon = kulstofbalance.calculate_co2e_kulstofbalance_mark(
                        r_relativ_faktor=1,  # Relative to Danish average
                        areal_ha=field.total_area_ha,
                        c_afgroederest_kg_c_ha=c_residue,
                        c_organisk_kg_c_ha=c_organic,
                    )
                    carbon_balance_total += co2e_carbon

            total_co2e += crop_residue_total
            total_co2e += carbon_balance_total  # Can be negative (sequestration)
            logger.debug(
                f"Crop residues N2O: {crop_residue_total:.1f} kg CO2e from {crops_with_residues} crop types"
            )
            logger.debug(
                f"Carbon balance: {carbon_balance_total:.1f} kg CO2e ({'sequestration' if carbon_balance_total < 0 else 'emission'})"
            )
        except Exception as e:
            logger.warning(f"Failed to calculate crop residue/carbon emissions: {e}")

        # Calculate CO2 from liming (default 170 kg/ha for all farmland)
        try:
            if total_area_ha > 0:
                liming_total = kalkning.calculate_co2_kalkning_bedrift(total_area_ha)
                total_co2e += liming_total
                logger.debug(f"CO2 from liming: {liming_total:.1f} kg CO2e for {total_area_ha:.1f} ha")
        except Exception as e:
            logger.warning(f"Failed to calculate liming emissions: {e}")

        # Calculate NH3 emissions from synthetic fertilizer application
        # Source: KB_21_5397_AP2 Table 22 (SEGES 2021)
        nh3_fertilizer_total = 0.0
        if fertilizer_summary is not None and fertilizer_summary.avg_n_kg_per_ha > 0:
            try:
                nh3_result = calculate_nh3_field_per_ha(
                    n_kg_per_ha=fertilizer_summary.avg_n_kg_per_ha,
                    area_ha=fertilizer_summary.total_area_ha,
                    fertilizer_type=None,  # Use default weighted average
                )
                nh3_fertilizer_total = nh3_result["nh3_kg"]
                logger.debug(
                    f"NH3 from synthetic fertilizer: {nh3_fertilizer_total:.1f} kg NH3 "
                    f"({fertilizer_summary.avg_n_kg_per_ha:.1f} kg N/ha × {fertilizer_summary.total_area_ha:.1f} ha)"
                )
            except Exception as e:
                logger.warning(f"Failed to calculate field NH3 from fertilizer: {e}")
        else:
            logger.debug("No fertilizer data available - skipping field NH3 calculation")

        field_category = EmissionCategory(
            name="fields",
            co2e_kg=total_co2e,
            data_quality="complete" if (nles5_df is not None and not nles5_df.empty) else "estimated",
            nh3_kg=nh3_fertilizer_total,
        )

        if n2o_fertilizer_total > 0:
            field_category.add_sub_source("n2o_fertilizer_direct", n2o_fertilizer_total)
        if n2o_leaching_total > 0:
            field_category.add_sub_source("n2o_leaching_indirect", n2o_leaching_total)
        if lavbundsjord_total > 0:
            field_category.add_sub_source("co2_lavbundsjord", lavbundsjord_total)
        if crop_residue_total > 0:
            field_category.add_sub_source("n2o_crop_residues", crop_residue_total)
        if carbon_balance_total != 0:
            field_category.add_sub_source("co2_carbon_balance", carbon_balance_total)
        if liming_total > 0:
            field_category.add_sub_source("co2_liming", liming_total)

        # Add NH3 sub-sources (environmental emissions, not GHG)
        if nh3_fertilizer_total > 0:
            field_category.add_nh3_sub_source("fertilizer_application_nh3", nh3_fertilizer_total)

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
