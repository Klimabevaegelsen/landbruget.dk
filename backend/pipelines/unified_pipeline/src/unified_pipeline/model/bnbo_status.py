from asyncio import Semaphore

from aiohttp import ClientTimeout
from pydantic import ConfigDict

from unified_pipeline.model.base_config import BaseJobConfig


class BNBOStatusConfig(BaseJobConfig):
    """
    Configuration for BNBO status data source.
    """

    name: str = "Danish BNBO Status"
    dataset: str = "bnbo_status"
    type: str = "wfs"
    description: str = "Municipal status for well-near protection areas (BNBO)"
    url: str = "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs"
    layer: str = "dai:status_bnbo"
    frequency: str = "weekly"
    enabled: bool = True
    # bucket: str = "landbrugsdata-raw-data"
    bucket: str = "rahul_apeability"
    create_dissolved: bool = True

    batch_size: int = 100
    max_concurrent: int = 3
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: ClientTimeout = ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    status_mapping: dict[str, str] = {
        "Frivillig aftale tilbudt (UDGÅET)": "Action Required",
        "Gennemgået, indsats nødvendig": "Action Required",
        "Ikke gennemgået (default værdi)": "Action Required",
        "Gennemgået, indsats ikke nødvendig": "Completed",
        "Indsats gennemført": "Completed",
        "Ingen erhvervsmæssig anvendelse af pesticider": "Completed",
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
