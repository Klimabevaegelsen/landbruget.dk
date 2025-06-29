import json
import os
from typing import Any, Optional

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.schema.spf_su import SpfSuResponse
from unified_pipeline.util.gcs_util import GCSUtil


class SpfSuSilverConfig(BaseJobConfig):
    name: str = "Danish SPF SU"
    dataset: str = "spf_su"
    type: str = "wfs"
    description: str = "SPF SU from WFS"
    load_dotenv()
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class SpfSuSilver(BaseSource[SpfSuSilverConfig], SilverJobInterface):
    def __init__(self, config: SpfSuSilverConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)

    def _validate_and_transform(self, data: list[dict]):
        """Parse and flatten bronze JSON data into tables using Pydantic schema and DuckDB."""
        # ✅ MIGRATION: Use DuckDB for table creation instead of wasteful  conversions
        import duckdb

        temp_conn = duckdb.connect()

        parsed = [SpfSuResponse.parse_obj(item) for item in data]

        # ✅ MIGRATION: Farm owner details - direct table operations (no  conversion)
        farm_owner_details = [item.ownerDetailInfo.dict() for item in parsed]
        temp_conn.register("temp_farm_owner_details", farm_owner_details)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE farm_owner_details_final AS SELECT * FROM temp_farm_owner_details"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "farm_owner_details_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_owner_details",
            temp_conn,
        )

        # ✅ MIGRATION: Farm certificate - direct table operations (no  conversion)
        farm_certificate = [item.ownerDetailInfo.danishCertificate.dict() for item in parsed]
        temp_conn.register("temp_farm_certificate", farm_certificate)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE farm_certificate_final AS SELECT * FROM temp_farm_certificate"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "farm_certificate_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_certificate",
            temp_conn,
        )

        # ✅ MIGRATION: Farm general health summary - direct table operations (no  conversion)
        farm_general_health_summary = [item.ownerDetailInfo.healthData.dict() for item in parsed]
        temp_conn.register("temp_farm_health", farm_general_health_summary)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE farm_health_final AS SELECT * FROM temp_farm_health"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "farm_health_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_general_health_summary",
            temp_conn,
        )

        # ✅ MIGRATION: Farm salmonella data - direct table operations (no  conversion)
        farm_salmonella_data = [item.ownerDetailInfo.salmonellaData.dict() for item in parsed]
        temp_conn.register("temp_farm_salmonella", farm_salmonella_data)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE farm_salmonella_final AS SELECT * FROM temp_farm_salmonella"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "farm_salmonella_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_salmonella_data",
            temp_conn,
        )

        # Farm disease control status
        farm_disease_control_status = []
        for data in parsed:
            for item in data.healthStatus.healthControlInfo:
                farm_disease_control_status.append(
                    {
                        "farm_id": data.ownerDetailInfo.chrNumber,
                        "disease": item.disease,
                        "last_sample": item.lastSample,
                        "next_sample": item.nextSample,
                    }
                )
        temp_conn.register("temp_disease_control", farm_disease_control_status)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE disease_control_final AS SELECT * FROM temp_disease_control"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "disease_control_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_disease_control_status",
            temp_conn,
        )

        # ✅ MIGRATION: Farm veterinarians - direct table operations (no  conversion)
        farm_veterinarians = [item.healthStatus.veterinarians for item in parsed]
        temp_conn.register("temp_veterinarians", farm_veterinarians)
        temp_conn.execute(
            "CREATE OR REPLACE TABLE veterinarians_final AS SELECT * FROM temp_veterinarians"
        )
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "veterinarians_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "farm_veterinarians",
            temp_conn,
        )

        # ✅ MIGRATION: Delivery options - direct table operations (no  conversion)
        deliveryOptions = [item.healthStatus.deliveryOptions for item in parsed]
        temp_conn.register("temp_delivery", deliveryOptions)
        temp_conn.execute("CREATE OR REPLACE TABLE delivery_final AS SELECT * FROM temp_delivery")
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "delivery_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "deliveryOptions",
            temp_conn,
        )

        # ✅ MIGRATION: Reception options - direct table operations (no  conversion)
        receptionOptions = [item.healthStatus.receptionOptions for item in parsed]
        temp_conn.register("temp_reception", receptionOptions)
        temp_conn.execute("CREATE OR REPLACE TABLE reception_final AS SELECT * FROM temp_reception")
        # ❌ ELIMINATED: No more wasteful  conversion
        self._save_data(
            "reception_final",
            self.config.dataset,
            self.config.bucket,
            "silver",
            "receptionOptions",
            temp_conn,
        )

        temp_conn.close()

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the SPF SU silver layer job.

        This method processes SPF SU data from the bronze layer (either in-memory or from storage)
        and transforms it into multiple structured datasets.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            None
        """
        self.log.info("Running SPF SU silver layer job")

        # Read data with support for in-memory passing
        if bronze_data is not None:
            self.log.info("Using bronze data from memory (in-memory data passing)")
            if isinstance(bronze_data, list):
                data = bronze_data
            else:
                self.log.error(f"Expected list from bronze stage, got {type(bronze_data)}")
                return
        else:
            # Fallback to reading from storage
            self.log.info("Reading bronze data from storage (fallback)")
            bronze_df = self._read_bronze_data(self.config.dataset, self.config.bucket)
            if bronze_df is None:
                self.log.error("Bronze data not found")
                return
            self.log.info("Bronze data found and loaded")
            # Extract the data from the  - bronze data is stored as JSON strings
            try:
                if "payload" in bronze_df.columns:
                    # Handle case where data is stored as JSON strings in payload column
                    data = []
                    for payload in bronze_df["payload"]:
                        if isinstance(payload, str):
                            data.append(json.loads(payload))
                        else:
                            data.append(payload)
                else:
                    # Handle case where data is stored directly
                    data = bronze_df.to_dict("records")
            except Exception as e:
                self.log.error(f"Failed to extract data from bronze : {e}")
                return

        self.log.info("Bronze data read successfully")
        # Transform bronze data via schema
        self._validate_and_transform(data)
