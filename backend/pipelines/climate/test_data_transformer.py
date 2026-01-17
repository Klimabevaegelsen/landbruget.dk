"""
Unit tests for data_transformer.py

Tests the transformation logic from Danish GCS schema to English calculator schema.
Uses actual GCS data structure patterns discovered during exploration.

Updated to use DuckDB relations instead of pandas DataFrames after refactoring.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# Add climate to path
climate_path = Path(__file__).parent
if str(climate_path) not in sys.path:
    sys.path.insert(0, str(climate_path))


# Global connection for tests to avoid connection being closed prematurely
_test_conn = duckdb.connect()


def df_to_duckdb_relation(df: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Convert pandas DataFrame to DuckDB relation for testing."""
    return _test_conn.from_df(df)


from data_transformer import (  # noqa: E402
    FertilizerSummary,
    FieldSummary,
    FVMTransformer,
    GKEATransformer,
    GreenAccountsTransformer,
    IntegratedFarmTransformer,
    LivestockSummary,
)


class TestGreenAccountsTransformer:
    """Test Green Accounts livestock data transformation."""

    def test_transform_cattle_data(self):
        """Test transformation of cattle livestock data."""
        # Mock Green Accounts data (actual GCS schema)
        df = pd.DataFrame(
            {
                "cvr_number": ["12345678", "12345678", "12345678"],
                "c_2001": ["Kvæg", "Kvæg", "Kvæg"],  # Species
                "c_2004": ["Malkekøer", "Kvier", "Kalve"],  # Type detail
                "c_2006": [100, 20, 15],  # Animal count
                "c_2016": [12000.0, 800.0, 200.0],  # N production kg
                "c_2005": ["Løsdrift", "Løsdrift", "Stald"],  # Housing type
            }
        )
        rel = df_to_duckdb_relation(df)

        result = GreenAccountsTransformer.transform(rel)

        assert "cattle" in result
        cattle = result["cattle"]
        assert isinstance(cattle, LivestockSummary)
        assert cattle.species == "cattle"
        assert cattle.total_count == 135  # 100 + 20 + 15
        assert cattle.total_n_production_kg == 13000.0  # 12000 + 800 + 200
        assert "dairy_cows" in cattle.subtypes
        assert cattle.subtypes["dairy_cows"] == 100
        assert "heifers" in cattle.subtypes
        assert cattle.subtypes["heifers"] == 20

    def test_transform_pigs_data(self):
        """Test transformation of pig livestock data."""
        df = pd.DataFrame(
            {
                "cvr_number": ["87654321", "87654321"],
                "c_2001": ["Svin", "Svin"],
                "c_2004": ["Søer", "Slagtesvin"],
                "c_2006": [250, 1200],
                "c_2016": [5000.0, 18000.0],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = GreenAccountsTransformer.transform(rel)

        assert "pigs" in result
        pigs = result["pigs"]
        assert pigs.total_count == 1450
        assert pigs.total_n_production_kg == 23000.0
        assert "sows" in pigs.subtypes
        assert pigs.subtypes["sows"] == 250
        assert "finishers" in pigs.subtypes
        assert pigs.subtypes["finishers"] == 1200

    def test_transform_multiple_species(self):
        """Test transformation with multiple species in same dataset."""
        df = pd.DataFrame(
            {
                "cvr_number": ["11111111"] * 4,
                "c_2001": ["Kvæg", "Kvæg", "Svin", "Høns"],
                "c_2004": ["Malkekøer", "Kvier", "Søer", "Hønniker"],
                "c_2006": [80, 15, 100, 5000],
                "c_2016": [9600.0, 600.0, 2000.0, 1000.0],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = GreenAccountsTransformer.transform(rel)

        assert len(result) == 3  # cattle, pigs, poultry
        assert "cattle" in result
        assert "pigs" in result
        assert "poultry" in result

        assert result["cattle"].total_count == 95
        assert result["pigs"].total_count == 100
        assert result["poultry"].total_count == 5000

    def test_transform_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        # Pass None for empty data (DuckDB relation cannot be created from empty DataFrame easily)
        result = GreenAccountsTransformer.transform(None)
        assert result == {}

    def test_transform_missing_columns(self):
        """Test handling of missing required columns."""
        df = pd.DataFrame({"cvr_number": ["12345678"], "some_other_col": ["value"]})
        rel = df_to_duckdb_relation(df)

        result = GreenAccountsTransformer.transform(rel)
        assert result == {}

    def test_get_species_breakdown(self):
        """Test conversion to readable list of dicts."""
        livestock = {
            "cattle": LivestockSummary(
                species="cattle",
                total_count=100,
                total_n_production_kg=12000.0,
                subtypes={"dairy_cows": 80, "heifers": 20},
            )
        }

        result = GreenAccountsTransformer.get_species_breakdown(livestock)

        assert len(result) == 2
        assert all("species" in d for d in result)
        assert all("subtype" in d for d in result)
        assert all("count" in d for d in result)
        assert sum(d["count"] for d in result) == 100


class TestGKEATransformer:
    """Test GKEA fertilizer data transformation."""

    def test_transform_fertilizer_data(self):
        """Test transformation of fertilizer application data."""
        df = pd.DataFrame(
            {
                "cvr_number": ["12345678"] * 3,
                "total_n_kvote": [1500.0, 2000.0, 1800.0],
                "faktisk_areal_ha": [12.5, 16.0, 14.5],
                "marknummer": ["M1", "M2", "M3"],
                "year": [2024, 2024, 2024],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = GKEATransformer.transform(rel)

        assert isinstance(result, FertilizerSummary)
        assert result.total_n_kg == 5300.0  # 1500 + 2000 + 1800
        assert result.total_area_ha == 43.0  # 12.5 + 16.0 + 14.5
        assert result.field_count == 3
        assert result.avg_n_kg_per_ha == pytest.approx(123.26, rel=0.01)

    def test_transform_with_missing_values(self):
        """Test handling of missing/null values."""
        df = pd.DataFrame(
            {
                "cvr_number": ["12345678", "12345678"],
                "total_n_kvote": [1500.0, None],
                "faktisk_areal_ha": [12.5, 10.0],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = GKEATransformer.transform(rel)

        assert result is not None
        assert result.total_n_kg == 1500.0
        assert result.total_area_ha == 22.5

    def test_transform_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        # Pass None for empty data
        result = GKEATransformer.transform(None)
        assert result is None

    def test_calculate_n2o_emissions(self):
        """Test N2O emission calculation from fertilizer."""
        fert = FertilizerSummary(
            total_n_kg=10000.0, total_area_ha=100.0, avg_n_kg_per_ha=100.0, field_count=10
        )

        co2e = GKEATransformer.calculate_n2o_emissions(fert)

        # Formula: 10000 * 0.01 * (44/28) * 273 (IPCC AR6, 2021 GWP value)
        expected = 10000.0 * 0.01 * (44 / 28) * 273
        assert co2e == pytest.approx(expected, rel=0.01)

    def test_calculate_n2o_emissions_none(self):
        """Test N2O calculation with None input."""
        co2e = GKEATransformer.calculate_n2o_emissions(None)
        assert co2e == 0.0


class TestFVMTransformer:
    """Test FVM field data transformation."""

    def test_transform_field_data(self):
        """Test transformation of field boundary data."""
        df = pd.DataFrame(
            {
                "cvr": ["12345678"] * 4,
                "bfe_nummer": ["BFE001", "BFE002", "BFE003", "BFE004"],
                "afgroede": ["Vinterhvede", "Vinterhvede", "Vårbyg", "Græs"],
                "areal_ha": [15.5, 18.2, 12.0, 8.5],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = FVMTransformer.transform(rel)

        assert len(result) == 3  # 3 unique crop types
        assert all(isinstance(fs, FieldSummary) for fs in result)

        # Find winter wheat
        winter_wheat = next(fs for fs in result if fs.crop_type == "winter_wheat")
        assert winter_wheat.total_area_ha == pytest.approx(33.7, rel=0.01)  # 15.5 + 18.2
        assert winter_wheat.field_count == 2

        # Find spring barley
        spring_barley = next(fs for fs in result if fs.crop_type == "spring_barley")
        assert spring_barley.total_area_ha == 12.0
        assert spring_barley.field_count == 1

    def test_crop_name_mapping(self):
        """Test Danish to English crop name mapping."""
        df = pd.DataFrame(
            {
                "cvr": ["12345678"] * 3,
                "bfe_nummer": ["BFE001", "BFE002", "BFE003"],
                "afgroede": ["Majs", "Raps", "Sukkerroer"],
                "areal_ha": [10.0, 15.0, 8.0],
            }
        )
        rel = df_to_duckdb_relation(df)

        result = FVMTransformer.transform(rel)

        crop_types = {fs.crop_type for fs in result}
        assert "maize" in crop_types
        assert "rapeseed" in crop_types
        assert "sugar_beet" in crop_types

    def test_transform_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        # Pass None for empty data
        result = FVMTransformer.transform(None)
        assert result == []

    def test_get_total_area(self):
        """Test total area calculation."""
        fields = [
            FieldSummary(crop_type="wheat", total_area_ha=25.0, field_count=2),
            FieldSummary(crop_type="barley", total_area_ha=18.5, field_count=1),
            FieldSummary(crop_type="grass", total_area_ha=10.2, field_count=3),
        ]

        total = FVMTransformer.get_total_area(fields)
        assert total == pytest.approx(53.7, rel=0.01)

    def test_get_crop_breakdown(self):
        """Test conversion to list of dicts."""
        fields = [
            FieldSummary(crop_type="wheat", total_area_ha=25.0, field_count=2),
            FieldSummary(crop_type="barley", total_area_ha=18.5, field_count=1),
        ]

        result = FVMTransformer.get_crop_breakdown(fields)

        assert len(result) == 2
        assert all("crop_type" in d for d in result)
        assert all("total_area_ha" in d for d in result)
        assert sum(d["total_area_ha"] for d in result) == pytest.approx(43.5, rel=0.01)


class TestIntegratedFarmTransformer:
    """Test integrated farm data transformation."""

    def test_transform_all_complete_data(self):
        """Test transformation with all data sources available."""
        # Mock livestock data
        livestock_df = pd.DataFrame(
            {
                "cvr_number": ["12345678", "12345678"],
                "c_2001": ["Kvæg", "Kvæg"],
                "c_2004": ["Malkekøer", "Kvier"],
                "c_2006": [100, 20],
                "c_2016": [12000.0, 800.0],
            }
        )
        livestock_rel = df_to_duckdb_relation(livestock_df)

        # Mock field data
        field_df = pd.DataFrame(
            {
                "cvr": ["12345678"] * 2,
                "bfe_nummer": ["BFE001", "BFE002"],
                "afgroede": ["Vinterhvede", "Græs"],
                "areal_ha": [25.0, 15.0],
            }
        )
        field_rel = df_to_duckdb_relation(field_df)

        # Mock fertilizer data
        fertilizer_df = pd.DataFrame(
            {
                "cvr_number": ["12345678", "12345678"],
                "total_n_kvote": [3000.0, 1800.0],
                "faktisk_areal_ha": [25.0, 15.0],
            }
        )
        fertilizer_rel = df_to_duckdb_relation(fertilizer_df)

        result = IntegratedFarmTransformer.transform_all(livestock_rel, field_rel, fertilizer_rel)

        assert "livestock" in result
        assert "fields" in result
        assert "fertilizer" in result
        assert "metadata" in result

        metadata = result["metadata"]
        assert metadata["has_livestock"] is True
        assert metadata["has_fields"] is True
        assert metadata["has_fertilizer"] is True
        assert metadata["total_area_ha"] == 40.0
        assert "cattle" in metadata["livestock_species"]

    def test_transform_all_partial_data(self):
        """Test transformation with only some data sources."""
        # Only livestock data
        livestock_df = pd.DataFrame(
            {
                "cvr_number": ["12345678"],
                "c_2001": ["Kvæg"],
                "c_2004": ["Malkekøer"],
                "c_2006": [100],
                "c_2016": [12000.0],
            }
        )
        livestock_rel = df_to_duckdb_relation(livestock_df)

        result = IntegratedFarmTransformer.transform_all(livestock_rel, None, None)

        assert result["metadata"]["has_livestock"] is True
        assert result["metadata"]["has_fields"] is False
        assert result["metadata"]["has_fertilizer"] is False
        assert result["metadata"]["total_area_ha"] == 0.0

    def test_transform_all_empty_data(self):
        """Test transformation with no data."""
        result = IntegratedFarmTransformer.transform_all(None, None, None)

        assert result["metadata"]["has_livestock"] is False
        assert result["metadata"]["has_fields"] is False
        assert result["metadata"]["has_fertilizer"] is False

    def test_to_farm_data_object(self):
        """Test conversion to FarmData object."""
        # Create integrated data
        livestock_df = pd.DataFrame(
            {
                "cvr_number": ["12345678"],
                "c_2001": ["Kvæg"],
                "c_2004": ["Malkekøer"],
                "c_2006": [100],
                "c_2016": [12000.0],
            }
        )
        livestock_rel = df_to_duckdb_relation(livestock_df)

        field_df = pd.DataFrame(
            {
                "cvr": ["12345678"],
                "bfe_nummer": ["BFE001"],
                "afgroede": ["Vinterhvede"],
                "areal_ha": [25.0],
            }
        )
        field_rel = df_to_duckdb_relation(field_df)

        integrated = IntegratedFarmTransformer.transform_all(livestock_rel, field_rel, None)

        # Convert to FarmData
        farm_data = IntegratedFarmTransformer.to_farm_data_object(integrated)

        assert farm_data is not None
        assert hasattr(farm_data, "animal_counts")
        assert hasattr(farm_data, "field_areas")
        assert "cattle_dairy_cows" in farm_data.animal_counts
        assert farm_data.animal_counts["cattle_dairy_cows"] == 100
        assert "winter_wheat" in farm_data.field_areas
        assert farm_data.field_areas["winter_wheat"] == 25.0


class TestDataStructures:
    """Test data structure classes."""

    def test_livestock_summary_to_dict(self):
        """Test LivestockSummary serialization."""
        summary = LivestockSummary(
            species="cattle",
            total_count=100,
            total_n_production_kg=12000.0,
            subtypes={"dairy_cows": 80, "heifers": 20},
            housing_systems={"loose_housing": 100},
        )

        result = summary.to_dict()

        assert result["species"] == "cattle"
        assert result["total_count"] == 100
        assert result["subtypes"]["dairy_cows"] == 80

    def test_field_summary_to_dict(self):
        """Test FieldSummary serialization."""
        summary = FieldSummary(
            crop_type="wheat", total_area_ha=25.5, field_count=2, avg_yield_kg_ha=7500.0
        )

        result = summary.to_dict()

        assert result["crop_type"] == "wheat"
        assert result["total_area_ha"] == 25.5
        assert result["avg_yield_kg_ha"] == 7500.0

    def test_fertilizer_summary_to_dict(self):
        """Test FertilizerSummary serialization."""
        summary = FertilizerSummary(
            total_n_kg=5000.0, total_area_ha=50.0, avg_n_kg_per_ha=100.0, field_count=5
        )

        result = summary.to_dict()

        assert result["total_n_kg"] == 5000.0
        assert result["avg_n_kg_per_ha"] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
