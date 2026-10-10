"""Bronze ingestion for DAGI administrative boundaries from Datafordeler WFS."""

import asyncio
import json
import os
import re
import ssl
from datetime import UTC, datetime
from typing import Any

import aiohttp
import certifi
from common.dagi_coverage import validate_dagi_coverage
from lxml import etree
from pydantic import Field
from pyproj import Transformer
from shapely import make_valid
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import transform
from tenacity import retry, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer

GML_NAMESPACE = "http://www.opengis.net/gml/3.2"
DAGI_NAMESPACE = "http://data.gov.dk/schemas/dagi/2/gml3sfp/dagi10"
WFS_NAMESPACE = "http://www.opengis.net/wfs/2.0"
NAMESPACES = {"dagi10": DAGI_NAMESPACE, "gml": GML_NAMESPACE, "wfs": WFS_NAMESPACE}
WFS_ENDPOINT = "https://wfs.datafordeler.dk/DAGIM/DAGI_10MULTIGEOM_GMLSFP/1.0.0/WFS"
TO_WGS84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


class DAGIBronzeConfig(BaseJobConfig):
    """Configuration for DAGI's Datafordeler WFS source."""

    name: str = "Danish Administrative Geographic Division"
    type: str = "datafordeler_wfs"
    description: str = "Administrative geographic divisions from Datafordeler DAGI WFS"
    dataset: str = "dagi"
    bucket: str = Field(
        default_factory=lambda: (
            os.getenv("STORAGE_BUCKET")
            or os.getenv("R2_BUCKET")
            or os.getenv("GCS_BUCKET", "landbruget-data")
        )
    )
    base_url: str = WFS_ENDPOINT
    endpoints: dict[str, str] = Field(
        default_factory=lambda: {
            "kommuner": "Kommuneinddeling",
            "regioner": "Regionsinddeling",
            "landsdele": "Landsdel",
            "postnumre": "Postnummerinddeling",
        }
    )
    page_sizes: dict[str, int] = Field(
        default_factory=lambda: {
            "kommuner": 25,
            "regioner": 1,
            "landsdele": 2,
            "postnumre": 200,
        }
    )
    timeout: int = Field(default=300, description="WFS request timeout in seconds")
    max_concurrent_requests: int = Field(default=4)
    retries: int = Field(default=3)


class DAGIBronze(BaseSource[DAGIBronzeConfig], BronzeJobInterface):
    """Fetch DAGI GML and publish raw pages plus compatible WGS84 GeoJSON."""

    def __init__(self, config: DAGIBronzeConfig):
        super().__init__(config)
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    def _get_api_key(self) -> str:
        api_key = os.getenv("DATAFORDELER_API_KEY") or os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
        if not api_key:
            raise ValueError(
                "Missing Datafordeler API key: set DATAFORDELER_API_KEY "
                "or DATAFORDELER_GRAPHQL_API_KEY"
            )
        return api_key

    def _get_params(self, type_name: str, start_index: int, count: int) -> dict[str, str]:
        """Build WFS parameters without ever putting credentials in a logged URL."""
        return {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typenames": f"dagi10:{type_name}",
            "srsName": "EPSG:25832",
            "count": str(count),
            "startIndex": str(start_index),
            "apiKey": self._get_api_key(),
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        layer_name: str,
        type_name: str,
        start_index: int,
        count: int,
    ) -> bytes:
        """Fetch one raw GML page. Logs identify the page but never include its URL."""
        async with self.semaphore:
            params = self._get_params(type_name, start_index, count)
            try:
                self.log.info(
                    "Fetching DAGI WFS layer {} page at startIndex {} (count {})",
                    layer_name,
                    start_index,
                    count,
                )
                async with session.get(self.config.base_url, params=params) as response:
                    response.raise_for_status()
                    return await response.read()
            except aiohttp.ClientError as exc:
                self.log.error(
                    "HTTP error fetching DAGI WFS layer {} page {} ({})",
                    layer_name,
                    start_index,
                    type(exc).__name__,
                )
                raise
            except TimeoutError as exc:
                self.log.error(
                    "Timeout fetching DAGI WFS layer {} page {} ({})",
                    layer_name,
                    start_index,
                    type(exc).__name__,
                )
                raise

    async def _fetch_layer_data(
        self,
        session: aiohttp.ClientSession,
        layer_name: str,
        type_name: str,
    ) -> list[bytes]:
        """Fetch every page until Datafordeler returns fewer members than requested."""
        page_size = self.config.page_sizes[layer_name]
        pages = []
        start_index = 0

        while True:
            page = await self._fetch_page(session, layer_name, type_name, start_index, page_size)
            pages.append(page)
            member_count = self._count_members(page)
            if member_count < page_size:
                return pages
            start_index += page_size

    async def _fetch_all_layers(self) -> dict[str, list[bytes]]:
        """Fetch regions first, then the remaining layers with bounded concurrency."""
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        # Datafordeler chains to Sectigo R46, which some Python builds' default stores lack.
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            pages: dict[str, list[bytes]] = {}
            if "regioner" in self.config.endpoints:
                pages["regioner"] = await self._fetch_layer_data(
                    session, "regioner", self.config.endpoints["regioner"]
                )

            other_layers = [
                (layer, type_name)
                for layer, type_name in self.config.endpoints.items()
                if layer != "regioner"
            ]
            results = await asyncio.gather(
                *(
                    self._fetch_layer_data(session, layer, type_name)
                    for layer, type_name in other_layers
                )
            )
            pages.update(
                {layer: result for (layer, _), result in zip(other_layers, results, strict=True)}
            )
            return pages

    @staticmethod
    def _count_members(page: bytes) -> int:
        root = etree.fromstring(
            page,
            parser=etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True),
        )
        members = root.xpath(".//wfs:member | .//gml:featureMember", namespaces=NAMESPACES)
        return len(members)

    @staticmethod
    def _pos_list_coordinates(pos_list: etree._Element) -> list[tuple[float, float]]:
        text = (pos_list.text or "").strip()
        values = [float(value) for value in text.split()]
        dimension_value = pos_list.get("srsDimension") or pos_list.get(
            f"{{{GML_NAMESPACE}}}srsDimension"
        )
        dimension = int(dimension_value or 2)
        if dimension < 2 or len(values) % dimension:
            raise ValueError("DAGI GML posList must contain complete coordinate tuples")
        coordinates = [
            tuple(values[index : index + 2]) for index in range(0, len(values), dimension)
        ]
        if len(coordinates) < 3:
            raise ValueError("DAGI GML polygon ring has fewer than three coordinates")
        if coordinates[0] != coordinates[-1]:
            coordinates.append(coordinates[0])
        if len(coordinates) < 4:
            raise ValueError("DAGI GML polygon ring has fewer than four closed coordinates")
        return coordinates

    def _parse_polygon(self, polygon_element: etree._Element) -> Polygon:
        exterior_nodes = polygon_element.xpath(
            "./gml:exterior/gml:LinearRing/gml:posList", namespaces=NAMESPACES
        )
        if not exterior_nodes:
            raise ValueError("DAGI GML polygon is missing its exterior ring")
        exterior = self._pos_list_coordinates(exterior_nodes[0])
        interior_nodes = polygon_element.xpath(
            "./gml:interior/gml:LinearRing/gml:posList", namespaces=NAMESPACES
        )
        interiors = [self._pos_list_coordinates(node) for node in interior_nodes]
        return Polygon(exterior, interiors)

    def _repair_geometry(self, geometry: MultiPolygon, layer_name: str) -> MultiPolygon:
        if geometry.is_valid:
            return geometry

        repaired = make_valid(geometry)
        if isinstance(repaired, Polygon):
            polygons = [repaired]
        elif isinstance(repaired, MultiPolygon):
            polygons = list(repaired.geoms)
        else:
            polygons = [
                part for part in getattr(repaired, "geoms", []) if isinstance(part, Polygon)
            ]
        if not polygons:
            raise ValueError(f"DAGI {layer_name} geometry repair produced no polygonal geometry")

        repaired_geometry = MultiPolygon(polygons)
        if not repaired_geometry.is_valid:
            raise ValueError(f"DAGI {layer_name} geometry remains invalid after make_valid")
        self._repaired_geometry_count = getattr(self, "_repaired_geometry_count", 0) + 1
        return repaired_geometry

    def _parse_geometry(self, geometry_element: etree._Element, layer_name: str) -> MultiPolygon:
        polygon_elements = geometry_element.xpath(
            ".//gml:Polygon | .//gml:PolygonPatch", namespaces=NAMESPACES
        )
        polygons = [self._parse_polygon(element) for element in polygon_elements]
        if not polygons:
            raise ValueError(f"DAGI {layer_name} feature has no Polygon or PolygonPatch geometry")
        return self._repair_geometry(MultiPolygon(polygons), layer_name)

    def _parse_gml_page(self, layer_name: str, page: bytes | str) -> list[dict[str, Any]]:
        """Parse one GML page into source attributes and EPSG:25832 geometries."""
        if isinstance(page, str):
            page = page.encode("utf-8")
        root = etree.fromstring(
            page,
            parser=etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True),
        )
        members = root.xpath(".//wfs:member/* | .//gml:featureMember/*", namespaces=NAMESPACES)
        records = []
        for feature in members:
            attributes = {}
            geometry_element = None
            for child in feature:
                local_name = etree.QName(child).localname
                if local_name == "geometri":
                    geometry_element = child
                else:
                    attributes[local_name] = (child.text or "").strip()
            if geometry_element is None:
                raise ValueError(f"DAGI {layer_name} feature is missing geometri")
            records.append(
                {
                    "attributes": attributes,
                    "geometry_utm": self._parse_geometry(geometry_element, layer_name),
                }
            )
        return records

    def _parse_layer(self, layer_name: str, pages: list[bytes]) -> list[dict[str, Any]]:
        self._repaired_geometry_count = 0
        records = [record for page in pages for record in self._parse_gml_page(layer_name, page)]
        self.log.info(
            "Repaired {} invalid DAGI {} geometries",
            self._repaired_geometry_count,
            layer_name,
        )
        return records

    @staticmethod
    def _layer_code(layer_name: str, attributes: dict[str, str]) -> str:
        source_fields = {
            "kommuner": "kommunekode",
            "regioner": "regionskode",
            "landsdele": "NUTS3vaerdi",
            "postnumre": "postnummer",
        }
        field = source_fields[layer_name]
        code = str(attributes.get(field, "")).strip()
        if layer_name in {"kommuner", "postnumre"}:
            code = code.zfill(4)
        return code

    def _validate_layer(self, layer_name: str, records: list[dict[str, Any]]) -> None:
        expected_counts = {
            "kommuner": (98, 100),
            "regioner": (5, 5),
            "landsdele": (11, 11),
            "postnumre": (1000, None),
        }
        minimum, maximum = expected_counts[layer_name]
        if len(records) < minimum or (maximum is not None and len(records) > maximum):
            expected = f"{minimum}..{maximum}" if maximum != minimum else str(minimum)
            raise ValueError(
                f"DAGI {layer_name} returned {len(records)} features; expected {expected}"
            )

        codes = [self._layer_code(layer_name, record["attributes"]) for record in records]
        if any(not code for code in codes):
            raise ValueError(f"DAGI {layer_name} has an empty feature code")
        if len(codes) != len(set(codes)):
            raise ValueError(f"DAGI {layer_name} has duplicate feature codes")

        if layer_name in {"kommuner", "postnumre"}:
            invalid = [code for code in codes if not re.fullmatch(r"\d{4}", code)]
            if invalid:
                raise ValueError(f"DAGI {layer_name} has invalid four-digit codes: {invalid[:3]}")
        if layer_name == "landsdele":
            invalid = [code for code in codes if not re.fullmatch(r"DK\d{3}", code)]
            if invalid:
                raise ValueError(f"DAGI landsdele has invalid NUTS3 codes: {invalid[:3]}")

        for record in records:
            geometry = record.get("geometry_utm")
            if geometry is None or geometry.is_empty or not geometry.is_valid:
                raise ValueError(f"DAGI {layer_name} has a missing or invalid geometry")

    @staticmethod
    def _required_attribute(attributes: dict[str, str], name: str) -> str:
        value = str(attributes.get(name, "")).strip()
        if not value:
            raise ValueError(f"DAGI feature is missing required attribute {name}")
        return value

    @staticmethod
    def _boolean_attribute(attributes: dict[str, str], name: str) -> bool:
        value = DAGIBronze._required_attribute(attributes, name).casefold()
        if value not in {"true", "false"}:
            raise ValueError(f"DAGI feature has invalid boolean attribute {name}: {value}")
        return value == "true"

    @staticmethod
    def _to_wgs84_geometry(geometry):
        return transform(TO_WGS84.transform, geometry)

    @staticmethod
    def _landsdel_region(
        landsdel_geometry: MultiPolygon, regions: list[dict[str, Any]]
    ) -> tuple[str, str]:
        point = landsdel_geometry.representative_point()
        matches = [
            region
            for region in regions
            if region["geometry_utm"].contains(point) or region["geometry_utm"].covers(point)
        ]
        if not matches:
            raise ValueError("No region contains landsdel representative point")
        attributes = matches[0]["attributes"]
        return (
            DAGIBronze._required_attribute(attributes, "regionskode"),
            DAGIBronze._required_attribute(attributes, "navn"),
        )

    def _build_geojson(
        self,
        layer_name: str,
        records: list[dict[str, Any]],
        regions: list[dict[str, Any]],
        fetch_timestamp: str,
    ) -> dict[str, Any]:
        region_names = {
            self._required_attribute(region["attributes"], "regionskode"): self._required_attribute(
                region["attributes"], "navn"
            )
            for region in regions
        }
        features = []
        for record in records:
            attributes = record["attributes"]
            geometry_utm = record["geometry_utm"]
            dagi_id = self._required_attribute(attributes, "id.lokalId")
            name = self._required_attribute(attributes, "navn")

            if layer_name == "kommuner":
                region_code = self._required_attribute(attributes, "regionskode")
                if region_code not in region_names:
                    raise ValueError(f"DAGI kommune references unknown region code {region_code}")
                properties = {
                    "kode": self._layer_code(layer_name, attributes),
                    "navn": name,
                    "regionskode": region_code,
                    "regionsnavn": region_names[region_code],
                    "udenforkommuneinddeling": self._boolean_attribute(
                        attributes, "udenforKommuneinddeling"
                    ),
                    "dagi_id": dagi_id,
                }
            elif layer_name == "regioner":
                properties = {
                    "kode": self._layer_code(layer_name, attributes),
                    "navn": name,
                    "nuts2": self._required_attribute(attributes, "NUTS2vaerdi"),
                    "dagi_id": dagi_id,
                }
            elif layer_name == "landsdele":
                region_code, region_name = self._landsdel_region(geometry_utm, regions)
                properties = {
                    "nuts3": self._layer_code(layer_name, attributes),
                    "navn": name,
                    "regionskode": region_code,
                    "regionsnavn": region_name,
                    "dagi_id": dagi_id,
                }
            elif layer_name == "postnumre":
                properties = {
                    "nr": self._layer_code(layer_name, attributes),
                    "navn": name,
                    "ergadepostnummer": self._boolean_attribute(attributes, "erGadepostnummer"),
                    "stormodtager": False,
                    "dagi_id": dagi_id,
                }
            else:
                raise ValueError(f"Unsupported DAGI layer: {layer_name}")

            properties.update(
                {
                    "_source": "datafordeler_dagi_wfs",
                    "_source_crs": "EPSG:25832",
                    "_fetch_timestamp": fetch_timestamp,
                }
            )
            geometry_wgs84 = self._to_wgs84_geometry(geometry_utm)
            features.append(
                {
                    "type": "Feature",
                    "properties": properties,
                    "geometry": mapping(geometry_wgs84),
                }
            )
        return {"type": "FeatureCollection", "features": features}

    @staticmethod
    def _validate_geojson(layer_name: str, collection: dict[str, Any]) -> None:
        for feature in collection["features"]:
            geometry = shape(feature["geometry"])
            if geometry.is_empty or not geometry.is_valid:
                raise ValueError(f"DAGI {layer_name} has an invalid WGS84 geometry")
            validate_dagi_coverage(geometry.bounds, "EPSG:4326", layer_name=layer_name)

    def _save_raw_page(self, layer_name: str, page_number: int, page: bytes) -> str:
        dataset_name = f"{self.config.dataset}_{layer_name}"
        storage_path = (
            f"{self.config.bucket}/bronze/{dataset_name}/{self.date_pattern}/raw/"
            f"{dataset_name}_page{page_number:03}.gml"
        )
        with self.storage.fs.open(storage_path, "wb") as raw_file:
            raw_file.write(page)
        return storage_path

    async def run(self) -> dict[str, str] | None:
        """Fetch, validate, store, and return the four compatible DAGI GeoJSON layers."""
        try:
            async with AsyncTimer("DAGI bronze layer processing") as timer:
                self.log.info("Starting DAGI Datafordeler WFS bronze processing")
                layer_pages = await self._fetch_all_layers()
                expected_layers = set(self.config.endpoints)
                if set(layer_pages) != expected_layers:
                    raise RuntimeError("DAGI WFS did not return every configured layer")

                for layer_name, pages in layer_pages.items():
                    for page_number, page in enumerate(pages, start=1):
                        path = self._save_raw_page(layer_name, page_number, page)
                        self.log.info("Saved raw DAGI GML page to {}", path)

                parsed = {
                    layer_name: self._parse_layer(layer_name, pages)
                    for layer_name, pages in layer_pages.items()
                }
                for layer_name, records in parsed.items():
                    self._validate_layer(layer_name, records)

                fetch_timestamp = datetime.now(UTC).isoformat()
                regions = parsed["regioner"]
                serialized_layers: dict[str, tuple[str, str]] = {}
                for layer_name in self.config.endpoints:
                    collection = self._build_geojson(
                        layer_name, parsed[layer_name], regions, fetch_timestamp
                    )
                    self._validate_geojson(layer_name, collection)
                    geojson = json.dumps(collection, ensure_ascii=False, allow_nan=False)
                    dataset_name = f"{self.config.dataset}_{layer_name}"
                    storage_path = (
                        f"{self.config.bucket}/bronze/{dataset_name}/{self.date_pattern}/"
                        f"{dataset_name}.json"
                    )
                    serialized_layers[layer_name] = (geojson, storage_path)

                # Build and serialize every layer before publishing any compatible JSON.
                output = {}
                layer_paths = {}
                for layer_name, (geojson, storage_path) in serialized_layers.items():
                    self.storage.upload_json_string(geojson, storage_path)
                    output[layer_name] = geojson
                    layer_paths[layer_name] = storage_path

                manifest = {
                    "snapshot_id": self.date_pattern,
                    "complete": True,
                    "layers": layer_paths,
                }
                manifest_path = (
                    f"{self.config.bucket}/bronze/{self.config.dataset}/"
                    f"{self.date_pattern}/completion.json"
                )
                self.storage.upload_json_string(
                    json.dumps(manifest, ensure_ascii=False, allow_nan=False), manifest_path
                )

                self.log.info(
                    "DAGI bronze processing completed in {:.2f}s for layers {}",
                    timer.elapsed(),
                    list(output),
                )
                return output
        except Exception as exc:
            # The exception text from an HTTP client can contain the signed URL.
            self.log.error("DAGI bronze processing failed ({})", type(exc).__name__)
            raise
