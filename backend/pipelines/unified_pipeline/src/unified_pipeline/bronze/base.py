from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from unified_pipeline.model.base_config import BaseJobConfig
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

T = TypeVar("T", bound=BaseJobConfig)


class BaseSource(Generic[T], ABC):
    """Base class for all data sources that fetch and store data"""

    def __init__(self, config: T, gcs_util: GCSUtil) -> None:
        self.config = config
        self.gcs_util = gcs_util
        self.log = Logger.get_logger()

    @abstractmethod
    async def run(self) -> None:
        """Run the data source"""
        pass
