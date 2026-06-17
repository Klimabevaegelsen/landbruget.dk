"""Silver layer processing for Danish afforestation areas."""

import json
from typing import Any, ClassVar

from common.geometry_validator import validate_and_normalize_to_utm

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed

USE_UTM_PROCESSING = True


class SkovrejsningSilverConfig(BaseJobConfig):
    """Configuration for skovrejsning silver processing."""

    dataset: str = "skovrejsning"
    bucket: str = "landbruget-data"
    storage_batch_size: int = 5000
    category_mapping: ClassVar[dict[str, str]] = {"Uønsket": "undesired", "Ønsket": "desired"}


class SkovrejsningSilver(BaseSource[SkovrejsningSilverConfig], SilverJobInterface):
    """Silver layer processor for Danish afforestation areas."""

    def __init__(self, config: SkovrejsningSilverConfig):
        """Initialize the skovrejsning silver processor."""
        super().__init__(config)
        self.log.info("SkovrejsningSilver: Using unified cloud storage access and DuckDB")

    def _normalize_property(self, value: Any) -> str | None:
        if value is None:
            return None
        value_str = str(value).strip()
        return value_str if value_str else None

    def _read_payload_rows(self, raw_data) -> list[tuple[Any, ...]] | None:
        if raw_data is None:
            self.log.warning("No raw data to process")
            return None

        if isinstance(raw_data, str):
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {raw_data}").fetchone()[0]
            if row_count == 0:
                self.log.warning("No raw data to process")
                return None
            rows = self.conn.execute(f"SELECT payload FROM {raw_data}").fetchall()
        elif isinstance(raw_data, dict) and "payload" in raw_data:
            rows = [(payload,) for payload in raw_data["payload"]]
        else:
            self.log.warning(f"Unexpected raw_data type: {type(raw_data)}")
            return None

        if not rows:
            self.log.warning("No raw data to process")
            return None
        return rows

    @timed(name="Processing bronze GeoJSON data")  # type: ignore
    def _process_geojson_data(self, raw_data) -> str | None:
        """Process bronze GeoJSON payloads into a DuckDB spatial feature table."""
        raw_rows = self._read_payload_rows(raw_data)
        if raw_rows is None:
            return None

        features = []
        for index, row_tuple in enumerate(raw_rows):
            try:
                payload = row_tuple[0]
                collection = json.loads(payload)
                for feature in collection.get("features", []):
                    geometry = feature.get("geometry")
                    if not geometry:
                        continue

                    properties = feature.get("properties") or {}
                    omraadenavn = self._normalize_property(properties.get("omraadenavn"))
                    features.append(
                        {
                            "geometry_geojson": json.dumps(geometry),
                            "omraadenavn": omraadenavn,
                            "komnavn": self._normalize_property(properties.get("komnavn")),
                            "plangrund": self._normalize_property(properties.get("plangrund")),
                            "plannr": self._normalize_property(properties.get("plannr")),
                            "temanavn": self._normalize_property(properties.get("temanavn")),
                            "datovedt": self._normalize_property(properties.get("datovedt")),
                            "uuid": self._normalize_property(properties.get("uuid")),
                            "objekt_id": self._normalize_property(properties.get("objekt_id")),
                            "category": self.config.category_mapping.get(
                                omraadenavn or "", "neutral"
                            ),
                        }
                    )
            except Exception as e:
                self.log.error(f"Error processing row {index}: {e!s}", exc_info=True)
                raise e

        self.log.info(f"Parsed {len(features):,} features from GeoJSON data")

        if not features:
            self.log.warning("No features extracted from GeoJSON data")
            return None

        staging_table = "skovrejsning_features_raw"
        table_name = "skovrejsning_processed_features"
        columns = list(features[0].keys())

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {staging_table} (
                {", ".join([f"{col} VARCHAR" for col in columns])}
            )
        """)

        placeholders = ", ".join(["?" for _ in columns])
        for feature in features:
            values = [feature.get(col) for col in columns]
            self.conn.execute(
                f"""
                INSERT INTO {staging_table} ({", ".join(columns)})
                VALUES ({placeholders})
            """,
                values,
            )

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                omraadenavn,
                komnavn,
                plangrund,
                plannr,
                temanavn,
                TRY_CAST(datovedt AS TIMESTAMP) as datovedt,
                uuid,
                objekt_id,
                category,
                ST_GeomFromGeoJSON(geometry_geojson) as geometry_spatial,
                CAST(NULL AS VARCHAR) as geometry,
                CAST(NULL AS DOUBLE) as area_ha
            FROM {staging_table}
            WHERE geometry_geojson IS NOT NULL
        """)

        if USE_UTM_PROCESSING:
            validate_and_normalize_to_utm(
                self.conn, table_name, self.config.dataset, geometry_column="geometry_spatial"
            )

        self.conn.execute(f"""
            UPDATE {table_name} SET
                geometry = ST_AsText(geometry_spatial),
                area_ha = ST_Area(geometry_spatial) / 10000
            WHERE geometry_spatial IS NOT NULL
        """)

        feature_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"Created DuckDB table '{table_name}' with {feature_count:,} features")

        self.conn.execute(f"DROP TABLE IF EXISTS {staging_table}")

        return table_name

    @timed(name="Creating dissolved table using DuckDB-spatial")  # type: ignore
    def _create_dissolved_df(self, input_table_name: str, dataset: str) -> str:
        """Create dissolved geometries grouped by skovrejsning category."""
        dissolved_table_name = "skovrejsning_dissolved_features"

        self.log.info(f"Creating dissolved geometries for {dataset} by category")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {dissolved_table_name} AS
            SELECT
                category,
                ST_Union_Agg(geometry_spatial) as geometry,
                current_timestamp as dissolved_at
            FROM {input_table_name}
            WHERE geometry_spatial IS NOT NULL
                AND ST_IsValid(geometry_spatial)
            GROUP BY category
        """)

        total_count = self.conn.execute(f"SELECT COUNT(*) FROM {dissolved_table_name}").fetchone()[
            0
        ]

        if total_count == 0:
            self.log.warning("No valid geometries to dissolve")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {dissolved_table_name} AS
                SELECT
                    CAST(NULL AS VARCHAR) as category,
                    CAST(NULL AS GEOMETRY) as geometry,
                    CAST(NULL AS TIMESTAMP) as dissolved_at
                WHERE FALSE
            """)

        return dissolved_table_name

    async def run(self, bronze_data=None) -> str | None:
        """Run the complete skovrejsning silver layer job."""
        async with AsyncTimer("Running Skovrejsning silver job"):
            self.log.info("Running Skovrejsning silver job")

            raw_data = self._read_bronze_data(
                self.config.dataset, self.config.bucket, bronze_data=bronze_data
            )
            if raw_data is None:
                self.log.error("No bronze data found")
                return None

            table_name = self._process_geojson_data(raw_data)
            if table_name is None:
                self.log.error("No processed data created")
                return None

            dissolved_table_name = self._create_dissolved_df(table_name, self.config.dataset)

            self.save_data_direct(table_name, self.config.dataset, self.config.bucket, "silver")
            self.save_data_direct(
                dissolved_table_name,
                f"{self.config.dataset}_dissolved",
                self.config.bucket,
                "silver",
            )

            self.log.info("Skovrejsning silver job completed successfully")
            return table_name
