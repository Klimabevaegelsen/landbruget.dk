from pydantic import ConfigDict
from dotenv import load_dotenv
from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
import os
import json
from unified_pipeline.schema.spf_su import SpfSuResponse
import pandas as pd

class SpfSuSilverConfig(BaseJobConfig):
    name: str = "Danish SPF SU"
    dataset: str = "spf_su"
    type: str = "wfs"
    description: str = "SPF SU from WFS"
    load_dotenv()
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")
    
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

class SpfSuSilver(BaseSource[SpfSuSilverConfig]):
    
    def __init__(self, config: SpfSuSilverConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
        
    def _validate_and_transform(self, data: list[dict]) -> pd.DataFrame:
        """Parse and flatten bronze JSON data into a DataFrame using Pydantic schema."""
        parsed = [SpfSuResponse.parse_obj(item) for item in data]
        farm_owner_details = [item.ownerDetailInfo.dict() for item in parsed]
        self._save_data(pd.DataFrame(farm_owner_details), self.config.dataset, self.config.bucket, 'silver', 'farm_owner_details')
        
        farm_certificate = [item.ownerDetailInfo.danishCertificate.dict() for item in parsed]
        self._save_data(pd.DataFrame(farm_certificate), self.config.dataset, self.config.bucket, 'silver', 'farm_certificate')
        
        farm_general_health_summary = [item.ownerDetailInfo.healthData.dict() for item in parsed]
        self._save_data(pd.DataFrame(farm_general_health_summary), self.config.dataset, self.config.bucket, 'silver', 'farm_general_health_summary')
        
        farm_salmonella_data = [item.ownerDetailInfo.salmonellaData.dict() for item in parsed]
        self._save_data(pd.DataFrame(farm_salmonella_data), self.config.dataset, self.config.bucket, 'silver', 'farm_salmonella_data')
        
        farm_disease_control_status = []
        for data in parsed:
            for item in data.healthStatus.healthControlInfo:
                farm_disease_control_status.append({
                    'farm_id': data.ownerDetailInfo.chrNumber,
                    'disease': item.disease,
                    'last_sample': item.lastSample,
                    'next_sample': item.nextSample
                })
        self._save_data(pd.DataFrame(farm_disease_control_status), self.config.dataset, self.config.bucket, 'silver', 'farm_disease_control_status')
        
        farm_veterinarians = [item.healthStatus.veterinarians for item in parsed]
        self._save_data(pd.DataFrame(farm_veterinarians), self.config.dataset, self.config.bucket, 'silver', 'farm_veterinarians')
        
        deliveryOptions = [item.healthStatus.deliveryOptions for item in parsed]
        self._save_data(pd.DataFrame(deliveryOptions), self.config.dataset, self.config.bucket, 'silver', 'deliveryOptions')
        
        receptionOptions = [item.healthStatus.receptionOptions for item in parsed]
        self._save_data(pd.DataFrame(receptionOptions), self.config.dataset, self.config.bucket, 'silver', 'receptionOptions')
            
    async def run(self) -> None:
        self.log.info("Running SPF SU silver layer job")
        bronze_path = self._get_latest_bronze_path(self.config.dataset, self.config.bucket)
        if bronze_path is None:
            self.log.error("Bronze data not found")
            return
        self.log.info(f"Bronze data found at {bronze_path}")
        with open(bronze_path, "r") as f:
            bronze_data = json.load(f)
        self.log.info("Bronze data read successfully")
        # Transform bronze data via schema
        self._validate_and_transform(bronze_data)