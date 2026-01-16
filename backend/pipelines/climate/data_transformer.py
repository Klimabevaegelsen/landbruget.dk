"""
Data Transformer Module - Bridge GCS Danish Schema to Calculator Input

This module transforms raw data from GCS (with Danish field names and values)
into the structured format expected by the climate calculator.

Data Sources:
- Green Accounts (gr): Livestock data with Danish column names (c_2001, c_2006, etc.)
- GKEA: Fertilizer application data (total_n_kvote, faktisk_areal_ha)
- FVM: Field boundary data (bfe_nummer, afgroede, areal_ha)

Transformations:
- Map Danish species names to English categories
- Extract animal counts and production metrics
- Aggregate fertilizer applications by field
- Structure data for calculator compatibility
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


# Species mapping: Danish to English
SPECIES_MAPPING = {
    "Kvæg": "cattle",
    "Kvaeg": "cattle",  # Alternative spelling
    "Svin": "pigs",
    "Høns": "poultry",
    "Hoens": "poultry",  # Alternative spelling
    "Får": "sheep",
    "Faar": "sheep",  # Alternative spelling
    "Geder": "goats",
    "Heste": "horses",
    "Mink": "mink",
    "Kaniner": "rabbits",
}

# Cattle sub-types mapping (c_2004 field detail)
CATTLE_SUBTYPE_MAPPING = {
    "Malkekøer": "dairy_cows",
    "Malkekoer": "dairy_cows",
    "Ammekøer": "suckler_cows",
    "Ammekoer": "suckler_cows",
    "Kvier": "heifers",
    "Kalve": "calves",
    "Ungtyre": "young_bulls",
    "Stude": "steers",
    "Tyre": "bulls",
}

# Pig sub-types mapping
PIG_SUBTYPE_MAPPING = {
    "Søer": "sows",
    "Soer": "sows",
    "Smågrise": "piglets",
    "Smaagrise": "piglets",
    "Slagtesvin": "finishers",
    "Polte": "gilts",
}

# Poultry sub-types mapping
POULTRY_SUBTYPE_MAPPING = {
    "Hønniker": "layers",
    "Honniker": "layers",
    "Slagtekyllinger": "broilers",
    "Ænder": "ducks",
    "Aender": "ducks",
    "Gæs": "geese",
    "Gaes": "geese",
    "Kalkuner": "turkeys",
}


@dataclass
class LivestockSummary:
    """
    Structured summary of livestock data by species and type.
    """

    species: str  # 'cattle', 'pigs', 'poultry', etc.
    total_count: int
    total_n_production_kg: float
    subtypes: Dict[str, int] = field(default_factory=dict)  # e.g., {'dairy_cows': 120}
    housing_systems: Dict[str, int] = field(default_factory=dict)  # e.g., {'loose_housing': 100}
    production_system: str = "conventional"  # 'conventional' or 'organic'

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "species": self.species,
            "total_count": self.total_count,
            "total_n_production_kg": self.total_n_production_kg,
            "subtypes": self.subtypes,
            "housing_systems": self.housing_systems,
            "production_system": self.production_system,
        }


@dataclass
class FieldSummary:
    """
    Structured summary of field data by crop type.
    """

    crop_type: str
    total_area_ha: float
    field_count: int
    crop_code: Optional[int] = None  # FVM crop code for emission parameter lookup
    avg_yield_kg_ha: Optional[float] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "crop_type": self.crop_type,
            "total_area_ha": self.total_area_ha,
            "field_count": self.field_count,
            "crop_code": self.crop_code,
            "avg_yield_kg_ha": self.avg_yield_kg_ha,
        }


@dataclass
class FertilizerSummary:
    """
    Structured summary of fertilizer application data.
    """

    total_n_kg: float
    total_area_ha: float
    avg_n_kg_per_ha: float
    field_count: int

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "total_n_kg": self.total_n_kg,
            "total_area_ha": self.total_area_ha,
            "avg_n_kg_per_ha": self.avg_n_kg_per_ha,
            "field_count": self.field_count,
        }


class GreenAccountsTransformer:
    """
    Transform Green Accounts (gr) livestock data from Danish schema to calculator input.

    Input Schema (GCS):
    - cvr_number: Company CVR (8 digits)
    - c_2001: Species name (Danish, e.g., "Kvæg", "Svin")
    - c_2004: Type detail (Danish, e.g., "Malkekøer", "Søer")
    - c_2006: Animal count (integer)
    - c_2016: Total N production (kg)
    - c_2005: Housing system type (Danish)
    - c_2030: Housing system code (integer)

    Output: Structured LivestockSummary objects by species
    """

    @staticmethod
    def transform(df: pd.DataFrame) -> Dict[str, LivestockSummary]:
        """
        Transform Green Accounts DataFrame to structured livestock summaries.

        Args:
            df: Raw Green Accounts DataFrame with Danish column names

        Returns:
            Dictionary mapping species (English) to LivestockSummary objects
            Example: {'cattle': LivestockSummary(...), 'pigs': LivestockSummary(...)}

        Example:
            >>> gr_df = loader.load_livestock(cvr="31373077", year=2023)
            >>> livestock = GreenAccountsTransformer.transform(gr_df)
            >>> print(livestock['cattle'].total_count)
            120
            >>> print(livestock['cattle'].subtypes)
            {'dairy_cows': 100, 'heifers': 20}
        """
        if df is None or df.empty:
            logger.warning("Empty livestock DataFrame provided")
            return {}

        # Validate required columns
        required_cols = ["c_2001", "c_2006"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return {}

        results = {}

        # Group by species (c_2001)
        for species_danish, group in df.groupby("c_2001"):
            # Map Danish species name to English
            species_english = SPECIES_MAPPING.get(str(species_danish), "unknown")

            if species_english == "unknown":
                logger.warning(f"Unknown species: {species_danish}")
                continue

            # Extract animal counts (c_2006)
            # NOTE: c_2006 may be string with concatenated values - convert to numeric first
            total_count = int(pd.to_numeric(group["c_2006"], errors="coerce").fillna(0).sum())

            # Extract N production (c_2016) if available
            # NOTE: c_2016 may be string with concatenated values - convert to numeric first
            if "c_2016" in group.columns:
                total_n = float(pd.to_numeric(group["c_2016"], errors="coerce").fillna(0).sum())
            else:
                total_n = 0.0

            # Build subtypes dict from c_2004 (type detail)
            subtypes = {}
            if "c_2004" in group.columns:
                for subtype_danish, subgroup in group.groupby("c_2004"):
                    # Map based on species
                    if species_english == "cattle":
                        subtype_english = CATTLE_SUBTYPE_MAPPING.get(str(subtype_danish), str(subtype_danish).lower())
                    elif species_english == "pigs":
                        subtype_english = PIG_SUBTYPE_MAPPING.get(str(subtype_danish), str(subtype_danish).lower())
                    elif species_english == "poultry":
                        subtype_english = POULTRY_SUBTYPE_MAPPING.get(str(subtype_danish), str(subtype_danish).lower())
                    else:
                        subtype_english = str(subtype_danish).lower()

                    # Convert to numeric to handle concatenated string values
                    subtypes[subtype_english] = int(pd.to_numeric(subgroup["c_2006"], errors="coerce").fillna(0).sum())

            # Build housing systems dict from c_2005 (housing type)
            housing_systems = {}
            if "c_2005" in group.columns:
                for housing_danish, housing_group in group.groupby("c_2005"):
                    # Store housing system counts (convert to numeric to handle string concatenation)
                    housing_systems[str(housing_danish)] = int(
                        pd.to_numeric(housing_group["c_2006"], errors="coerce").fillna(0).sum()
                    )

            # Create summary object
            results[species_english] = LivestockSummary(
                species=species_english,
                total_count=total_count,
                total_n_production_kg=total_n,
                subtypes=subtypes,
                housing_systems=housing_systems,
            )

        logger.info(f"Transformed livestock data: {list(results.keys())}")
        return results

    @staticmethod
    def get_species_breakdown(livestock_summaries: Dict[str, LivestockSummary]) -> pd.DataFrame:
        """
        Convert livestock summaries to a readable DataFrame.

        Args:
            livestock_summaries: Output from transform()

        Returns:
            DataFrame with columns: species, subtype, count, n_production_kg
        """
        rows = []

        for species, summary in livestock_summaries.items():
            for subtype, count in summary.subtypes.items():
                rows.append(
                    {
                        "species": species,
                        "subtype": subtype,
                        "count": count,
                        "n_production_kg": summary.total_n_production_kg * (count / summary.total_count)
                        if summary.total_count > 0
                        else 0,
                    }
                )

        return pd.DataFrame(rows)


class GKEATransformer:
    """
    Transform GKEA fertilizer data from Danish schema to calculator input.

    Input Schema (GCS):
    - cvr_number: Company CVR (8 digits)
    - total_n_kvote: Total N applied (kg) - PRIMARY FIELD for N2O calculation
    - faktisk_areal_ha: Actual field area (hectares)
    - marknummer: Field number
    - year: Agricultural year

    Output: Structured FertilizerSummary object
    """

    @staticmethod
    def transform(df: pd.DataFrame) -> Optional[FertilizerSummary]:
        """
        Transform GKEA DataFrame to structured fertilizer summary.

        Args:
            df: Raw GKEA DataFrame with fertilizer application data

        Returns:
            FertilizerSummary object with aggregated fertilizer metrics

        Example:
            >>> gkea_df = loader.load_fertilizer(cvr="31373077", year=2024)
            >>> fert = GKEATransformer.transform(gkea_df)
            >>> print(fert.total_n_kg)
            12500.5
            >>> print(fert.avg_n_kg_per_ha)
            125.0
        """
        if df is None or df.empty:
            logger.warning("Empty fertilizer DataFrame provided")
            return None

        # Validate required columns
        required_cols = ["total_n_kvote", "faktisk_areal_ha"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return None

        # Handle missing or invalid data
        df_clean = df.copy()
        df_clean["total_n_kvote"] = pd.to_numeric(df_clean["total_n_kvote"], errors="coerce").fillna(0)
        df_clean["faktisk_areal_ha"] = pd.to_numeric(df_clean["faktisk_areal_ha"], errors="coerce").fillna(0)

        # Calculate aggregates
        total_n = float(df_clean["total_n_kvote"].sum())
        total_area = float(df_clean["faktisk_areal_ha"].sum())
        field_count = len(df_clean)

        # Calculate average N per hectare
        avg_n_per_ha = total_n / total_area if total_area > 0 else 0.0

        summary = FertilizerSummary(
            total_n_kg=total_n,
            total_area_ha=total_area,
            avg_n_kg_per_ha=avg_n_per_ha,
            field_count=field_count,
        )

        logger.info(
            f"Transformed fertilizer data: {total_n:.1f} kg N across {total_area:.1f} ha ({field_count} fields)"
        )
        return summary

    @staticmethod
    def calculate_n2o_emissions(fertilizer_summary: FertilizerSummary) -> float:
        """
        Calculate N2O emissions from fertilizer application.

        Formula: total_n_kg * 0.01 * (44/28) * 298 = kg CO2e

        Args:
            fertilizer_summary: Output from transform()

        Returns:
            N2O emissions in kg CO2e
        """
        if fertilizer_summary is None:
            return 0.0

        # IPCC Tier 1 emission factor: 1% of applied N is emitted as N2O-N
        emission_factor = 0.01

        # Convert N2O-N to N2O: molecular weight ratio (44/28)
        n2o_conversion = 44 / 28

        # N2O global warming potential (GWP): 273 kg CO2e per kg N2O (IPCC AR6, 2021)
        gwp_n2o = 273

        co2e_kg = fertilizer_summary.total_n_kg * emission_factor * n2o_conversion * gwp_n2o

        logger.info(f"Calculated N2O emissions: {co2e_kg:.1f} kg CO2e from fertilizer")
        return co2e_kg


class FVMTransformer:
    """
    Transform FVM field data from Danish schema to calculator input.

    Input Schema (GCS):
    - cvr: Company CVR (8 digits)
    - bfe_nummer: Cadastral field ID
    - afgroede: Crop type (Danish)
    - areal_ha: Field area (hectares)
    - year: Agricultural year

    Output: List of FieldSummary objects by crop type
    """

    # Common Danish crop names to English
    CROP_MAPPING = {
        "Vinterhvede": "winter_wheat",
        "Vårhvede": "spring_wheat",
        "Varhvede": "spring_wheat",
        "Vinterbyg": "winter_barley",
        "Vårbyg": "spring_barley",
        "Varbyg": "spring_barley",
        "Havre": "oats",
        "Rug": "rye",
        "Triticale": "triticale",
        "Majs": "maize",
        "Raps": "rapeseed",
        "Kløver": "clover",
        "Kloever": "clover",
        "Græs": "grass",
        "Graes": "grass",
        "Lucerne": "alfalfa",
        "Sukkerroer": "sugar_beet",
        "Kartofler": "potatoes",
        "Ærter": "peas",
        "Aerter": "peas",
        "Bønner": "beans",
        "Boenner": "beans",
        "Brak": "fallow",
        "Vedvarende græs": "permanent_grass",
        "Vedvarende graes": "permanent_grass",
    }

    @staticmethod
    def transform(df: pd.DataFrame) -> List[FieldSummary]:
        """
        Transform FVM DataFrame to structured field summaries.

        Args:
            df: Raw FVM DataFrame with field boundary data

        Returns:
            List of FieldSummary objects, one per crop type

        Example:
            >>> fvm_df = loader.load_fields(cvr="31373077", year=2024)
            >>> fields = FVMTransformer.transform(fvm_df)
            >>> for field in fields:
            ...     print(f"{field.crop_type}: {field.total_area_ha:.1f} ha")
            winter_wheat: 45.2 ha
            grass: 28.5 ha
        """
        if df is None or df.empty:
            logger.warning("Empty field DataFrame provided")
            return []

        # Validate required columns
        # FVM data may use different column names depending on version
        area_col = None
        crop_col = None

        # Check for area column variants
        if "areal_ha" in df.columns:
            area_col = "areal_ha"
        elif "area_ha" in df.columns:
            area_col = "area_ha"
        elif "areal" in df.columns:
            area_col = "areal"

        # Check for crop column variants
        if "afgroede" in df.columns:
            crop_col = "afgroede"
        elif "crop_name" in df.columns:
            crop_col = "crop_name"
        elif "afgroedekode" in df.columns:
            crop_col = "afgroedekode"

        # Check for crop_code column (FVM uses Afgkode -> crop_code mapping)
        crop_code_col = None
        if "crop_code" in df.columns:
            crop_code_col = "crop_code"
        elif "afgkode" in df.columns:
            crop_code_col = "afgkode"
        elif "afgroede_kode" in df.columns:
            crop_code_col = "afgroede_kode"

        if not area_col or not crop_col:
            logger.error(f"Missing required columns. Available: {df.columns.tolist()}")
            logger.error(
                f"Looking for area column (areal_ha/area_ha/areal) and crop column (afgroede/crop_name/afgroedekode)"
            )
            return []

        # Handle missing or invalid data
        df_clean = df.copy()
        df_clean[area_col] = pd.to_numeric(df_clean[area_col], errors="coerce").fillna(0)

        # Convert crop_code to numeric if present
        if crop_code_col:
            df_clean[crop_code_col] = pd.to_numeric(df_clean[crop_code_col], errors="coerce")

        results = []

        # Group by crop_code if available (more reliable), otherwise by crop name
        group_col = crop_code_col if crop_code_col else crop_col

        for group_value, group in df_clean.groupby(group_col):
            # Map Danish crop name to English
            if crop_code_col and group_col == crop_code_col:
                # Get crop name from group (may have duplicates, take first non-null)
                crop_names = group[crop_col].dropna().unique()
                crop_danish = crop_names[0] if len(crop_names) > 0 else str(group_value)
                crop_code = int(group_value) if pd.notna(group_value) else None
            else:
                crop_danish = group_value
                # Try to get crop_code from group if column exists
                if crop_code_col:
                    codes = group[crop_code_col].dropna().unique()
                    crop_code = int(codes[0]) if len(codes) > 0 and pd.notna(codes[0]) else None
                else:
                    crop_code = None

            crop_english = FVMTransformer.CROP_MAPPING.get(str(crop_danish), str(crop_danish).lower().replace(" ", "_"))

            total_area = float(group[area_col].sum())
            field_count = len(group)

            summary = FieldSummary(
                crop_type=crop_english,
                total_area_ha=total_area,
                field_count=field_count,
                crop_code=crop_code
            )

            results.append(summary)

        logger.info(f"Transformed field data: {len(results)} crop types")
        return results

    @staticmethod
    def get_total_area(field_summaries: List[FieldSummary]) -> float:
        """
        Calculate total field area across all crops.

        Args:
            field_summaries: Output from transform()

        Returns:
            Total area in hectares
        """
        return sum(fs.total_area_ha for fs in field_summaries)

    @staticmethod
    def get_crop_breakdown(field_summaries: List[FieldSummary]) -> pd.DataFrame:
        """
        Convert field summaries to a readable DataFrame.

        Args:
            field_summaries: Output from transform()

        Returns:
            DataFrame with columns: crop_type, total_area_ha, field_count
        """
        rows = [fs.to_dict() for fs in field_summaries]
        return pd.DataFrame(rows)


class IntegratedFarmTransformer:
    """
    Orchestrates all transformers to create complete farm data structure.

    This class combines outputs from all specialized transformers into a single
    structured format suitable for the climate calculator.
    """

    @staticmethod
    def transform_all(
        livestock_df: Optional[pd.DataFrame],
        field_df: Optional[pd.DataFrame],
        fertilizer_df: Optional[pd.DataFrame],
        organic_status: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Transform all farm data sources into integrated structure.

        Args:
            livestock_df: Raw Green Accounts livestock data
            field_df: Raw FVM field data
            fertilizer_df: Raw GKEA fertilizer data
            organic_status: Optional dict with organic certification status from load_organic_certification()

        Returns:
            Dictionary with structured farm data:
            {
                'livestock': {species: LivestockSummary, ...},
                'fields': [FieldSummary, ...],
                'fertilizer': FertilizerSummary,
                'metadata': {
                    'has_livestock': bool,
                    'has_fields': bool,
                    'has_fertilizer': bool,
                    'total_area_ha': float,
                    'organic_status': dict  # Organic certification info
                }
            }

        Example:
            >>> from data_loader import ClimateDataLoader
            >>> loader = ClimateDataLoader()
            >>> livestock = loader.load_livestock(cvr="31373077", year=2023)
            >>> fields = loader.load_fields(cvr="31373077", year=2024)
            >>> fert = loader.load_fertilizer(cvr="31373077", year=2024)
            >>> organic = loader.load_organic_certification(cvr="31373077", year=2024)
            >>>
            >>> farm_data = IntegratedFarmTransformer.transform_all(livestock, fields, fert, organic)
            >>> print(farm_data['metadata']['organic_status'])
            {'has_organic_fields': True, 'is_organic_livestock': True}
        """
        # Transform each data source
        livestock = (
            GreenAccountsTransformer.transform(livestock_df)
            if livestock_df is not None and not livestock_df.empty
            else {}
        )

        fields = FVMTransformer.transform(field_df) if field_df is not None and not field_df.empty else []

        fertilizer = (
            GKEATransformer.transform(fertilizer_df) if fertilizer_df is not None and not fertilizer_df.empty else None
        )

        # Apply organic status to livestock if detected
        if organic_status and organic_status.get('is_organic_livestock', False):
            logger.info(f"Applying organic production system to livestock")
            for species, summary in livestock.items():
                summary.production_system = "organic"
                logger.debug(f"  {species}: production_system = organic")

        # Calculate metadata
        total_area = FVMTransformer.get_total_area(fields) if fields else 0.0

        metadata = {
            "has_livestock": len(livestock) > 0,
            "has_fields": len(fields) > 0,
            "has_fertilizer": fertilizer is not None,
            "total_area_ha": total_area,
            "livestock_species": list(livestock.keys()),
            "crop_types": [f.crop_type for f in fields],
            "organic_status": organic_status if organic_status else {
                'has_organic_fields': False,
                'is_organic_livestock': False
            }
        }

        result = {
            "livestock": livestock,
            "fields": fields,
            "fertilizer": fertilizer,
            "metadata": metadata,
        }

        logger.info(f"Integrated farm data: {metadata}")
        return result

    @staticmethod
    def to_farm_data_object(integrated_data: Dict[str, Any]) -> "FarmData":
        """
        Convert integrated data structure to FarmData object for calculator.

        Args:
            integrated_data: Output from transform_all()

        Returns:
            FarmData object compatible with climate_calculator.py

        Note:
            This requires importing FarmData from farm_data.py
        """
        from farm_data import FarmData

        farm_data = FarmData()

        # Populate animal counts
        for species, summary in integrated_data["livestock"].items():
            for subtype, count in summary.subtypes.items():
                animal_key = f"{species}_{subtype}"
                farm_data.animal_counts[animal_key] = count

        # Populate field areas
        for field_summary in integrated_data["fields"]:
            farm_data.field_areas[field_summary.crop_type] = field_summary.total_area_ha

        # Populate fertilizer applications
        if integrated_data["fertilizer"] is not None:
            fert = integrated_data["fertilizer"]
            if fert.total_area_ha > 0:
                # Store average N application rate
                farm_data.fertilizer_applications["avg_n_kg_ha"] = fert.avg_n_kg_per_ha

        return farm_data


# Example usage
if __name__ == "__main__":
    """
    Example demonstrating the complete data transformation workflow.
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

    # Initialize loader
    loader = ClimateDataLoader()

    # Example CVR (Arla test farm)
    cvr = "31373077"
    year = 2023

    print(f"\n{'=' * 60}")
    print(f"Data Transformation Example: CVR {cvr}, Year {year}")
    print(f"{'=' * 60}\n")

    # Load raw data
    print("1. Loading raw data from GCS...")
    livestock_df = loader.load_livestock(cvr=cvr, year=year)
    field_df = loader.load_fields(cvr=cvr, year=2024)  # Use 2024 for fields
    fertilizer_df = loader.load_fertilizer(cvr=cvr, year=2024)

    # Transform livestock data
    print("\n2. Transforming livestock data...")
    if not livestock_df.empty:
        livestock = GreenAccountsTransformer.transform(livestock_df)
        for species, summary in livestock.items():
            print(f"\n{species.upper()}:")
            print(f"  Total Count: {summary.total_count}")
            print(f"  N Production: {summary.total_n_production_kg:.1f} kg")
            print(f"  Subtypes: {summary.subtypes}")

    # Transform field data
    print("\n3. Transforming field data...")
    if not field_df.empty:
        fields = FVMTransformer.transform(field_df)
        print(f"\nTotal Area: {FVMTransformer.get_total_area(fields):.1f} ha")
        print(f"Crop Types: {len(fields)}")
        for field in fields[:5]:  # Show first 5
            print(f"  {field.crop_type}: {field.total_area_ha:.1f} ha ({field.field_count} fields)")

    # Transform fertilizer data
    print("\n4. Transforming fertilizer data...")
    if not fertilizer_df.empty:
        fertilizer = GKEATransformer.transform(fertilizer_df)
        if fertilizer:
            print(f"\nTotal N Applied: {fertilizer.total_n_kg:.1f} kg")
            print(f"Total Area: {fertilizer.total_area_ha:.1f} ha")
            print(f"Average N/ha: {fertilizer.avg_n_kg_per_ha:.1f} kg/ha")
            print(f"Fields: {fertilizer.field_count}")

            # Calculate N2O emissions
            n2o_co2e = GKEATransformer.calculate_n2o_emissions(fertilizer)
            print(f"N2O Emissions: {n2o_co2e:.1f} kg CO2e")

    # Integrated transformation
    print("\n5. Integrated transformation...")
    integrated = IntegratedFarmTransformer.transform_all(livestock_df, field_df, fertilizer_df)
    print(f"\nFarm Metadata:")
    for key, value in integrated["metadata"].items():
        print(f"  {key}: {value}")

    print(f"\n{'=' * 60}")
    print("Transformation complete!")
    print(f"{'=' * 60}\n")
