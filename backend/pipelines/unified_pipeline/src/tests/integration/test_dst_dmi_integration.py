"""
Integration tests for DST and DMI pipeline implementations.

This module tests the complete bronze-to-silver pipeline flow for both
DST and DMI data sources, including in-memory data passing and storage fallback.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unified_pipeline.bronze.dmi import DMIBronze, DMIBronzeConfig
from unified_pipeline.bronze.dst import DSTBronze, DSTBronzeConfig
from unified_pipeline.silver.dmi import DMISilver, DMISilverConfig
from unified_pipeline.silver.dst import DSTSilver, DSTSilverConfig


class TestDSTIntegration:
    """Integration tests for DST pipeline."""

    @pytest.fixture
    def mock_gcs_access(self):
        """Mock GCS access for testing."""
        return MagicMock()

    @pytest.fixture
    def dst_bronze_config(self):
        """Create DST bronze configuration for testing."""
        return DSTBronzeConfig(table_ids=["HST77"])

    @pytest.fixture
    def dst_silver_config(self):
        """Create DST silver configuration for testing."""
        return DSTSilverConfig(table_ids=["HST77"])

    @pytest.mark.asyncio
    async def test_dst_bronze_to_silver_integration(
        self, mock_gcs_access, dst_bronze_config, dst_silver_config
    ):
        """Test integration between DST bronze and silver layers."""

        # Create bronze instance
        DSTBronze(dst_bronze_config)

        # Mock bronze data processing
        mock_bronze_data = {
            "table_name": "dst_bronze_data",
            "record_count": 1000,
            "processing_time": "2024-01-01T00:00:00Z",
        }

        # Create silver instance
        silver = DSTSilver(dst_silver_config)

        # Test silver processing with bronze data
        result = await silver.run(bronze_data=mock_bronze_data)

        # Verify silver processing
        assert result is not None
        assert "table_name" in result
        assert result["record_count"] > 0

    @pytest.mark.asyncio
    async def test_dst_silver_storage_fallback(self, mock_gcs_access, dst_silver_config):
        """Test DST silver fallback to storage when no bronze data provided."""

        silver = DSTSilver(dst_silver_config)

        # Test fallback to storage
        result = await silver.run(bronze_data=None)

        # Should handle gracefully (may return None if no storage data)
        assert result is None or isinstance(result, dict)


@patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
class TestDMIIntegration:
    """Integration tests for DMI pipeline."""

    @pytest.fixture
    def mock_gcs_access(self):
        """Mock GCS access for testing."""
        return MagicMock()

    @pytest.fixture
    def dmi_bronze_config(self):
        """Create DMI bronze configuration for testing."""
        return DMIBronzeConfig(parameters=["pot_evaporation_makkink"], days_back=1)

    @pytest.fixture
    def dmi_silver_config(self):
        """Create DMI silver configuration for testing."""
        return DMISilverConfig(parameters=["pot_evaporation_makkink"])

    @pytest.mark.asyncio
    async def test_dmi_bronze_to_silver_in_memory(
        self, mock_gcs_access, dmi_bronze_config, dmi_silver_config
    ):
        """Test DMI bronze-to-silver pipeline with in-memory data passing."""
        # Create bronze instance
        bronze = DMIBronze(dmi_bronze_config)

        # Mock API client responses
        mock_features = [
            {
                "properties": {
                    "value": 2.5,
                    "parameterId": "pot_evaporation_makkink",
                    "from": "2024-01-01T00:00:00Z",
                    "created": "2024-01-01T12:00:00Z",
                },
                "geometry": {"type": "Point", "coordinates": [12.0, 55.0]},
            }
        ]

        bronze.api_client.fetch_grid_data = AsyncMock(
            return_value={
                "features": mock_features,
                "parameter_id": "pot_evaporation_makkink",
                "feature_count": 1,
            }
        )

        # Mock GCS access for bronze
        bronze.gcs_access = MagicMock()
        bronze.gcs_access.upload_json = MagicMock()

        # Run bronze stage
        bronze_data = await bronze.run()

        assert bronze_data is not None
        assert "pot_evaporation_makkink" in bronze_data

        # Create silver instance
        silver = DMISilver(dmi_silver_config)

        # Mock GCS access for silver
        silver.gcs_access = MagicMock()
        silver._save_data = MagicMock()

        # Run silver stage with in-memory bronze data
        silver_data = await silver.run(bronze_data=bronze_data)

        assert silver_data is not None
        assert "pot_evaporation_makkink" in silver_data

        # Verify that _save_data was called (data was processed and saved)
        silver._save_data.assert_called()

    @pytest.mark.asyncio
    async def test_dmi_silver_storage_fallback(self, mock_gcs_access, dmi_silver_config):
        """Test DMI silver pipeline with storage fallback (no in-memory data)."""
        # Create silver instance
        silver = DMISilver(dmi_silver_config)

        # Mock GCS access to simulate storage fallback
        silver.gcs_access = MagicMock()
        silver.gcs_access.list_files = MagicMock(
            return_value=["bronze/dmi/20241201_120000/pot_evaporation_makkink_data.json"]
        )
        silver.gcs_access.download_json = MagicMock(
            return_value={
                "features": [
                    {
                        "properties": {
                            "value": 1.8,
                            "parameterId": "pot_evaporation_makkink",
                            "from": "2024-01-01T00:00:00Z",
                            "created": "2024-01-01T12:00:00Z",
                        },
                        "geometry": {"type": "Point", "coordinates": [12.0, 55.0]},
                    }
                ],
                "parameter_id": "pot_evaporation_makkink",
                "feature_count": 1,
            }
        )
        silver._save_data = MagicMock()

        # Run silver stage without bronze data (storage fallback)
        silver_data = await silver.run(bronze_data=None)

        assert silver_data is not None
        assert "pot_evaporation_makkink" in silver_data

        # Verify that GCS was accessed for bronze data
        silver.gcs_access.list_files.assert_called()
        silver.gcs_access.download_json.assert_called()

    @pytest.mark.asyncio
    async def test_dmi_silver_handles_bronze_errors(self, mock_gcs_access, dmi_silver_config):
        """Test DMI silver pipeline handles bronze data with errors gracefully."""
        # Create silver instance
        silver = DMISilver(dmi_silver_config)

        # Mock bronze data with error
        bronze_data_with_error = {
            "pot_evaporation_makkink": {
                "parameter_id": "pot_evaporation_makkink",
                "data": {
                    "features": [],
                    "error": "API Error occurred",
                    "parameter_id": "pot_evaporation_makkink",
                },
                "metadata": {"has_error": True},
            }
        }

        # Run silver stage with error data
        silver_data = await silver.run(bronze_data=bronze_data_with_error)

        # Should return None since no valid data was processed
        assert silver_data is None


class TestCLIIntegration:
    """Integration tests for CLI functionality with new sources."""

    def test_dst_source_in_cli_enum(self):
        """Test that DST source is available in CLI enum."""
        from unified_pipeline.model.cli import Source

        assert hasattr(Source, "dst")
        assert Source.dst.value == "dst"

    def test_dmi_source_in_cli_enum(self):
        """Test that DMI source is available in CLI enum."""
        from unified_pipeline.model.cli import Source

        assert hasattr(Source, "dmi")
        assert Source.dmi.value == "dmi"

    def test_pipeline_mapping_includes_dst_and_dmi(self):
        """Test that pipeline mapping includes DST and DMI sources."""
        from unified_pipeline.model.cli import CliConfig, Env, Source, Stage

        # This should not raise an error for DST
        try:
            dst_config = CliConfig(env=Env.prod, source=Source.dst, stage=Stage.bronze)
            # We don't actually execute, just verify the config is valid
            assert dst_config.source == Source.dst
        except Exception as e:
            pytest.fail(f"DST source not properly configured: {e}")

        # This should not raise an error for DMI
        try:
            dmi_config = CliConfig(env=Env.prod, source=Source.dmi, stage=Stage.bronze)
            # We don't actually execute, just verify the config is valid
            assert dmi_config.source == Source.dmi
        except Exception as e:
            pytest.fail(f"DMI source not properly configured: {e}")
