from abc import ABC, abstractmethod
from google.cloud import storage
import geopandas as gpd
from shapely.geometry import shape
import pyarrow as pa
import logging
from typing import Optional, Dict, Any
import time
import os
import pandas as pd
from .sources.utils.geometry_validator import validate_and_transform_geometries
from google.cloud import secretmanager

logger = logging.getLogger(__name__)

class Source(ABC):
    """Abstract base class for data sources"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get('enabled', True)
        self.project_id = config.get('project_id')
        
        self.use_google_secrets = config.get('use_google_secrets', False)
        if self.use_google_secrets:
            self.secrets_client = secretmanager.SecretManagerServiceClient()
        
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket('landbrugsdata-raw-data')
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from config, environment variables, or Google Secret Manager"""
        if 'secrets' in self.config and key in self.config['secrets']:
            return self.config['secrets'][key]
            
        env_value = os.environ.get(key.upper())
        if env_value:
            return env_value
            
        if self.use_google_secrets and self.project_id:
            name = f"projects/{self.project_id}/secrets/{key}/versions/latest"
            try:
                response = self.secrets_client.access_secret_version(request={"name": name})
                return response.payload.data.decode("UTF-8")
            except Exception as e:
                logger.error(f"Error accessing secret {key}: {str(e)}")
                
        return None
    
    @abstractmethod
    async def fetch(self):
        """Fetch data from source"""
        pass

    @abstractmethod
    async def sync(self) -> Optional[int]:
        """Sync data to storage, returns number of records synced or None on failure"""
        pass
