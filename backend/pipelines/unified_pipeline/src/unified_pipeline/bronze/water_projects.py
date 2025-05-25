import asyncio
import ssl
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from typing import Optional

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class WaterProjectsBronzeConfig(BaseJobConfig):
    name: str = "Danish Water Projects Map"
    dataset: str = "water_projects"
    type: str = "wfs"
    description: str = "Water projects from various Danish programs"
    url: str = "https://geodata.fvm.dk/geoserver/wfs"
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"

    batch_size: int = 100
    max_concurrent: int = 3
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    layers: list[str] = [
        "N2000_projekter:Hydrologi_E",
        "N2000_projekter:Hydrologi_F",
        "Ovrige_projekter:Vandloebsrestaurering_E",
        "Ovrige_projekter:Vandloebsrestaurering_F",
        "Vandprojekter:Fosfor_E_samlet",
        "Vandprojekter:Fosfor_F_samlet",
        "Vandprojekter:Kvaelstof_E_samlet",
        "Vandprojekter:Kvaelstof_F_samlet",
        "Vandprojekter:Lavbund_E_samlet",
        "Vandprojekter:Lavbund_F_samlet",
        "Vandprojekter:Private_vaadomraader",
        "Vandprojekter:Restaurering_af_aadale_2024",
        "vandprojekter:kla_projektforslag",
        "vandprojekter:kla_projektomraader",
        "Klima_lavbund_demarkation___offentlige_projekter:0",
    ]
    url_mapping: dict[str, str] = {
        "vandprojekter:kla_projektforslag": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
        "vandprojekter:kla_projektomraader": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
        "Klima_lavbund_demarkation___offentlige_projekter:0": "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer",
    }
    service_types: dict[str, str] = {"Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"}

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WaterProjectsBronze(BaseSource[WaterProjectsBronzeConfig]):
    def __init__(self, config: WaterProjectsBronzeConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

    def _get_params(self, layer, start_index=0):
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": layer,
            "STARTINDEX": str(start_index),
            "COUNT": str(self.config.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(
        self, session: aiohttp.ClientSession, layer: str, url: str, start_index: int
    ) -> dict:
        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Fetching chunk for layer {layer} starting at index {start_index} to {start_index + self.config.batch_size}"
            ),
        ):
            params = self._get_params(layer, start_index)
            self.log.debug(
                f"Trying to fetch data from {url} for layer {layer} "
                f"with index {start_index} to {start_index + self.config.batch_size} "
                f"with params {params}"
            )
            try:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        err_msg = f"Failed to fetch data. Status: {response.status}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    try:
                        text = await response.text()
                    except UnicodeDecodeError:
                        # Handle decoding errors by using 'replace' strategy
                        text = await response.text(errors="replace")
                        self.log.warning(
                            f"Handled Unicode decoding error in response for layer "
                            f"{layer} at index {start_index}"
                        )

                    root = ET.fromstring(text)
                    return {
                        "text": text,
                        "start_index": start_index,
                        "total_features": int(root.get("numberMatched", "0")),
                        "returned_features": int(root.get("numberReturned", "0")),
                    }
            except ET.ParseError as e:
                err_msg = f"Failed to parse XML response: {e}"
                self.log.error(err_msg, exc_info=True)
                raise Exception(err_msg)
            except Exception as e:
                err_msg = f"Error fetching chunk: {e}"
                self.log.error(err_msg, exc_info=True)
                raise Exception(err_msg)

    async def _fetch_arcgis_data(
        self, session: aiohttp.ClientSession, layer: str, url: str
    ) -> list[str]:
        async with (
            self.config.request_semaphore,
            AsyncTimer(f"Fetching ArcGIS data for layer {layer}"),
        ):
            self.log.debug(f"Fetching data for layer {layer} from {url}")
            # Extract layer ID from the full layer string
            # This will get "0" from "Klima_lavbund_demarkation___offentlige_projekter:0"
            layer_id = layer.split(":")[1]

            params = {
                "f": "json",
                "where": "1=1",
                "outFields": "*",
                "geometryPrecision": "6",
                "outSR": "25832",
                "returnGeometry": "true",
            }
            try:
                fetch_url = f"{url}/{layer_id}/query"
                self.log.debug(f"Fetching data from {fetch_url} with params {params}")
                async with session.get(fetch_url, params=params) as response:
                    if response.status != 200:
                        response_err = await response.text()
                        err_msg = f"Failed to fetch ArcGIS data. Status: {response.status}, Error: {response_err[:500]}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    json_data = await response.json()  # Just check if this is a valid JSON response
                    total_features = len(json_data.get("features", []))
                    self.log.info(f"Total features fetched  ArcGIS: {total_features}")

                    try:
                        response_text = await response.text()
                    except UnicodeDecodeError:
                        # Handle decoding errors by using 'replace' strategy
                        self.log.warning(
                            f"Unicode decode error when reading response text for ArcGIS layer {layer}. "
                            f"Using 'replace' strategy."
                        )
                        response_text = await response.text(errors="replace")

                    return [response_text]
            except Exception as e:
                err_msg = f"Error fetching data: {e}"
                self.log.error(err_msg)
                raise Exception(err_msg)

    async def _fetch_wfs_data(
        self, session: aiohttp.ClientSession, layer: str, url: str
    ) -> list[str]:
        raw_features = []
        self.log.info(f"Fetching WFS data for layer: {layer} from {url}")
        async with AsyncTimer(f"Fetching WFS data for layer {layer}"):
            raw_data = await self._fetch_chunk(session, layer, url, 0)
            total_features = raw_data["total_features"]
            returned_features = raw_data["returned_features"]
            raw_features.append(raw_data["text"])
            fetched_features_count = returned_features
            self.log.info(f"Fetched {fetched_features_count} out of {total_features}")

            # Create a list of tasks for all remaining chunks to fetch
            tasks = []
            for start_index in range(returned_features, total_features, self.config.batch_size):
                tasks.append(self._fetch_chunk(session, layer, url, start_index))

            # Fetch all chunks in parallel using asyncio.gather
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process the results
            for result in results:
                if isinstance(result, Exception):
                    self.log.error(f"Error occurred while fetching chunk: {result}")
                    raise result

                if isinstance(result, dict):
                    raw_features.append(result["text"])
                    fetched_features_count += result["returned_features"]
                    self.log.debug(f"Processed chunk with {result['returned_features']} features")
                else:
                    self.log.error(f"Unexpected result type: {type(result)}")

            self.log.info(f"Fetched all {fetched_features_count} out of {total_features} features")
            return raw_features

    async def _fetch_raw_data(self) -> Optional[list[str]]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with (
            aiohttp.ClientSession(headers=self.config.headers, connector=connector) as session,
            AsyncTimer("Fetching raw data for Water Projects bronze job"),
        ):
            for layer in self.config.layers:
                self.log.info(f"Fetching data for layer: {layer}")
                url = self.config.url_mapping.get(layer, self.config.url)
                service_type = self.config.service_types.get(layer, "wfs")
                try:
                    if service_type == "arcgis":
                        raw_data = await self._fetch_arcgis_data(session, layer, url)
                    else:
                        raw_data = await self._fetch_wfs_data(session, layer, url)
                    if not raw_data:
                        self.log.warning(f"No data fetched for layer: {layer}")
                        continue
                    self.log.info(f"Fetched {len(raw_data)} features for layer: {layer}")
                    raw_features.extend(raw_data)
                except Exception as e:
                    self.log.error(f"Error occured while fetching chunk: {e}")
                    raise e
        if not raw_features:
            self.log.warning("No raw features fetched")
            return None
        self.log.info(f"Total raw features fetched: {len(raw_features)}")
        return raw_features

    async def run(self) -> None:
        async with AsyncTimer("Running Water Projects bronze job for"):
            self.log.info("Running Water Projects bronze job")
            raw_data = await self._fetch_raw_data()
            if raw_data is None:
                self.log.error("Failed to fetch raw data")
                return
            self.log.info("Fetched raw data successfully")
            self._save_raw_data(raw_data, self.config.dataset, self.config.name, self.config.bucket)
            self.log.info("Saved raw data successfully")
            self.log.info("Water Projects bronze job completed successfully")
