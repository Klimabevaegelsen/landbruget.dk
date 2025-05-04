from pydantic import BaseModel, ConfigDict


class BaseJobConfig(BaseModel):
    """
    Base configuration for all data sources.
    This class defines common configuration properties that all data sources share.
    """
