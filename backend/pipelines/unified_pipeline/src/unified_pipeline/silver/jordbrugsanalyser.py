"""
Silver layer processing for Jordbrugsanalyser Marker data.

Bronze now parses the WFS GML into compact structured parquet. Silver reads
those structured rows, validates geometry in EPSG:25832, and keeps the output
schema required by the FVM WFS EjerNr CVR bridge.
"""

from typing import Any, ClassVar

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.jordbrugsanalyser_gml import FIELD_MAPPING, NAMESPACES
from unified_pipeline.util.timing import AsyncTimer


class JordbrugsanalyserSilverConfig(BaseJobConfig):
    """Configuration for Jordbrugsanalyser silver processing."""

    name: str = "Danish Jordbrugsanalyser Markers Silver"
    type: str = "wfs"
    description: str = "Processed agricultural marker data from Jordbrugsanalyser"
    dataset: str = "jordbrugsanalyser_markers"
    bronze_dataset: str = "jordbrugsanalyser_markers"
    bucket: str = "landbruget-data"

    start_year: int = 2012
    end_year: int = 2024

    namespaces: ClassVar[dict[str, str]] = NAMESPACES
    field_mapping: ClassVar[dict[str, tuple[str, Any]]] = FIELD_MAPPING

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class JordbrugsanalyserSilver(BaseSource[JordbrugsanalyserSilverConfig], SilverJobInterface):
    """Silver processing for structured Jordbrugsanalyser bronze data."""

    def __init__(self, config: JordbrugsanalyserSilverConfig):
        super().__init__(config)

    def _resolve_bronze_table(self, year: int, bronze_data: Any | None = None) -> str | None:
        """Resolve structured bronze data from memory or storage for one year."""
        bronze_dataset_name = f"{self.config.bronze_dataset}_{year}"

        if isinstance(bronze_data, dict) and str(year) in bronze_data:
            year_data = bronze_data[str(year)]
            if isinstance(year_data, str):
                return year_data
            if isinstance(year_data, list) and all(
                isinstance(item, str) and item.startswith("saved_to_storage_") for item in year_data
            ):
                self.log.info(f"Bronze year {year} was saved to storage; reading from storage")
            else:
                return self._read_bronze_data(bronze_dataset_name, self.config.bucket, year_data)
        elif bronze_data is not None:
            return self._read_bronze_data(bronze_dataset_name, self.config.bucket, bronze_data)

        return self._read_bronze_data(bronze_dataset_name, self.config.bucket)

    def _process_year_data(self, year: int, bronze_data: Any | None = None) -> str | None:
        """
        Process one year of structured bronze rows into the stable silver schema.

        The output schema is intentionally limited to:
        owner_number, field_block, field_number, geometry, year.
        """
        try:
            bronze_table = self._resolve_bronze_table(year, bronze_data)
            if not bronze_table:
                self.log.warning(f"No bronze data found for year {year}")
                return None

            try:
                self.conn.execute("INSTALL spatial")
                self.conn.execute("LOAD spatial")
            except Exception:
                pass

            required_columns = {"owner_number", "field_block", "field_number", "geometry", "year"}
            existing_columns = {
                row[1]
                for row in self.conn.execute(f"PRAGMA table_info('{bronze_table}')").fetchall()
            }
            missing_columns = required_columns - existing_columns
            if missing_columns:
                self.log.error(
                    f"Bronze table {bronze_table} is missing required structured columns: "
                    f"{sorted(missing_columns)}"
                )
                return None

            table_name = f"jordbrugsanalyser_markers_{year}"
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT
                    CAST(owner_number AS BIGINT) AS owner_number,
                    CAST(field_block AS VARCHAR) AS field_block,
                    CAST(field_number AS VARCHAR) AS field_number,
                    geometry,
                    CAST(year AS INTEGER) AS year
                FROM {bronze_table}
                WHERE owner_number IS NOT NULL
                  AND field_block IS NOT NULL
                  AND field_number IS NOT NULL
                  AND geometry IS NOT NULL
                  AND TRY(ST_IsValid(geometry)) = true
            """)

            valid_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if valid_count == 0:
                self.log.error(f"No valid structured features found for year {year}")
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                return None

            self.log.info(f"Created {valid_count:,} silver features for year {year}")
            return table_name

        except Exception as e:
            self.log.error(f"Error processing year {year}: {e}")
            return None

    async def run(self, bronze_data: Any | None = None) -> None:
        """Run silver processing for configured years."""
        self.log.info("Running Jordbrugsanalyser Markers silver job")

        async with AsyncTimer("Total Jordbrugsanalyser silver processing time"):
            for year in range(self.config.start_year, self.config.end_year + 1):
                try:
                    self.log.info(f"Processing silver layer for year {year}")

                    processed_table = self._process_year_data(year, bronze_data)
                    if processed_table is None:
                        self.log.warning(f"Year {year}: No data to save")
                        continue

                    total_features = self.conn.execute(
                        f"SELECT COUNT(*) FROM {processed_table}"
                    ).fetchone()[0]
                    dataset_name = f"{self.config.dataset}_{year}"
                    self.log.info(f"Saving {total_features:,} features for year {year}")

                    storage_path = self.save_data_direct(
                        processed_table, dataset_name, self.config.bucket, "silver"
                    )
                    self.log.info(f"Year {year}: Silver data saved successfully to {storage_path}")

                    unique_blocks = self.conn.execute(
                        f"SELECT COUNT(DISTINCT field_block) FROM {processed_table} "
                        f"WHERE field_block IS NOT NULL"
                    ).fetchone()[0]
                    self.log.info(f"Year {year} statistics:")
                    self.log.info(f"  - Total features: {total_features:,}")
                    self.log.info(f"  - Unique field blocks: {unique_blocks}")

                    self.conn.execute(f"DROP TABLE IF EXISTS {processed_table}")

                except Exception as e:
                    self.log.error(f"Failed to process year {year}: {e}")
                    continue

            self.log.info("Jordbrugsanalyser Markers silver job completed successfully")
