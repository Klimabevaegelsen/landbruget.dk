"""
Base classes for data sources in the unified pipeline.

This module defines the abstract base classes that all data sources in
the unified pipeline must implement. It provides common functionality and
enforces a consistent interface across different data sources and stages.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger


class BaseJobConfig(BaseModel):
    """
    Base configuration model for all data sources.

    This class defines common configuration properties that all data sources
    share and serves as a foundation for source-specific configuration models.
    It uses Pydantic for validation and type checking.

    All specific source configurations should inherit from this class and
    add their own specific configuration parameters.

    Example:
        >>> class MySourceConfig(BaseJobConfig):
        >>>     input_path: str
        >>>     output_bucket: str
    """


T = TypeVar("T", bound=BaseJobConfig)


class BaseSource(Generic[T], ABC):
    """
    Abstract base class for all data sources in the unified pipeline.

    This class defines the common interface and shared functionality that
    all data sources must implement. It handles configuration management,
    logging, and access to GCS utilities.

    Type parameter:
        T: Configuration type that extends BaseJobConfig

    Attributes:
        config: Source-specific configuration object
        gcs_util: Google Cloud Storage utility instance
        log: Logger instance for this source
    """

    def __init__(self, config: T, gcs_util: GCSUtil) -> None:
        """
        Initialize a new data source.

        Args:
            config: Source-specific configuration object
            gcs_util: Google Cloud Storage utility instance for cloud storage operations
        """
        self.config = config
        self.gcs_util = gcs_util
        self.log = Logger.get_logger()

    @abstractmethod
    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method must be implemented by all concrete source classes.
        It should handle the entire process of fetching, transforming, and
        storing data according to the source's specific requirements.

        Returns:
            None

        Raises:
            NotImplementedError: If the concrete class does not implement this method
        """
        pass
