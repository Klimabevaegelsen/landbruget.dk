"""
GeoDanmark WFS Data Fetcher for the BBR Buildings Pipeline.

This module handles fetching sample data from GeoDanmark WFS for building
cross-reference and enhanced classification.
"""

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests

from config import Settings


class GeoDanmarkWFSFetcher:
    """Fetches sample data from GeoDanmark WFS for building cross-reference."""

    def __init__(self, settings: Settings, logger: logging.Logger):
        """
        Initialize the GeoDanmark WFS fetcher.

        Args:
            settings: Pipeline settings
            logger: Logger instance
        """
        self.settings = settings
        self.logger = logger
        self.session = requests.Session()

        # GeoDanmark WFS requires authentication - fail if credentials are missing
        if not self.settings.has_datafordeler_credentials:
            raise ValueError(
                "GeoDanmark WFS requires authentication. "
                "Please set DATAFORDELER_USERNAME and DATAFORDELER_PASSWORD environment variables."
            )

        self.logger.info("Using authenticated access to GeoDanmark WFS")

    def fetch_samples(self, output_dir: Path, max_features: int = 1000, return_data: bool = False):
        """
        Fetch sample data from GeoDanmark WFS.

        Args:
            output_dir: Directory to save the sample data
            max_features: Maximum number of features to fetch per layer
        """

    def fetch_building_geometries(
        self, output_dir: Path, building_ids: list, return_data: bool = False
    ):
        """
        Fetch building geometries from GeoDanmark WFS for specific building IDs.

        Args:
            output_dir: Directory to save the geometry data
            building_ids: List of BBRUUID building IDs to fetch geometries for
            return_data: Whether to return data for in-memory processing
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(
            f"Starting GeoDanmark WFS geometry fetch for {len(building_ids):,} building IDs to {run_dir}"
        )

        try:
            # Process building IDs in batches to avoid overwhelming the WFS service
            batch_size = 1000  # Reasonable batch size for WFS queries
            all_geometries = []

            total_batches = (len(building_ids) + batch_size - 1) // batch_size
            self.logger.info(
                f"Processing {len(building_ids):,} building IDs in {total_batches} batches of {batch_size}"
            )

            for i in range(0, len(building_ids), batch_size):
                batch_ids = building_ids[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                self.logger.info(
                    f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} IDs)"
                )

                try:
                    batch_geometries = self._fetch_building_batch_geometries(batch_ids)
                    all_geometries.extend(batch_geometries)

                    self.logger.info(
                        f"Batch {batch_num}: Retrieved {len(batch_geometries)} geometries"
                    )

                except Exception as e:
                    self.logger.error(f"Failed to fetch batch {batch_num}: {e}")
                    # Continue with other batches rather than failing completely
                    continue

            # Save combined geometries
            geometries_data = {
                "type": "FeatureCollection",
                "features": all_geometries,
                "metadata": {
                    "total_requested": len(building_ids),
                    "total_retrieved": len(all_geometries),
                    "timestamp": timestamp,
                    "source": "geodanmark_wfs_geometries",
                },
            }

            # Save to file
            geometries_file = run_dir / "building_geometries.json"
            with open(geometries_file, "w", encoding="utf-8") as f:
                json.dump(geometries_data, f, indent=2, ensure_ascii=False)

            self.logger.info(
                f"Successfully retrieved {len(all_geometries):,} building geometries out of {len(building_ids):,} requested"
            )

            # Save metadata
            self._save_geometries_metadata(run_dir, building_ids, all_geometries)

            # Optionally return data for in-memory processing
            if return_data:
                return {
                    "geometries": all_geometries,
                    "metadata": geometries_data["metadata"],
                    "output_dir": run_dir,
                }

            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch building geometries: {e}")
            raise

    def _fetch_building_batch_geometries(self, building_ids: list) -> list:
        """
        Fetch geometries for a batch of building IDs.

        Args:
            building_ids: List of BBRUUID building IDs

        Returns:
            List of GeoJSON features with geometries
        """
        # Create CQL filter for the building IDs
        # GeoDanmark WFS uses BBRUUID field to link to BBR
        ids_filter = "BBRUUID IN ('" + "','".join(building_ids) + "')"

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "gdk60:Bygning",  # Building footprints
            "outputFormat": "application/json",
            "CQL_FILTER": ids_filter,
            "srsName": "EPSG:4326",  # Ensure consistent projection
        }

        # Add authentication parameters
        if self.settings.has_datafordeler_credentials:
            params.update(
                {
                    "username": self.settings.datafordeler_username,
                    "password": self.settings.datafordeler_password,
                }
            )

        try:
            response = self.session.get(
                self.settings.geodanmark_wfs_url,
                params=params,
                timeout=60,  # Longer timeout for potentially large responses
            )
            response.raise_for_status()

            # Parse JSON response
            geojson_data = response.json()

            if "features" in geojson_data:
                return geojson_data["features"]
            else:
                self.logger.warning(f"No features returned for batch of {len(building_ids)} IDs")
                return []

        except Exception as e:
            self.logger.error(f"Failed to fetch geometries for batch: {e}")
            return []

    def _save_geometries_metadata(
        self, output_dir: Path, requested_ids: list, retrieved_geometries: list
    ) -> None:
        """
        Save metadata about the geometry fetch operation.

        Args:
            output_dir: Directory to save metadata
            requested_ids: List of requested building IDs
            retrieved_geometries: List of successfully retrieved geometries
        """
        # Extract retrieved IDs from geometries
        retrieved_ids = []
        for feature in retrieved_geometries:
            if "properties" in feature and "BBRUUID" in feature["properties"]:
                retrieved_ids.append(feature["properties"]["BBRUUID"])

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source": "geodanmark_wfs_geometries",
            "total_requested": len(requested_ids),
            "total_retrieved": len(retrieved_geometries),
            "success_rate": len(retrieved_geometries) / len(requested_ids) if requested_ids else 0,
            "missing_ids": list(set(requested_ids) - set(retrieved_ids)),
            "wfs_endpoint": self.settings.geodanmark_wfs_url,
            "feature_type": "gdk60:Bygning",
        }

        metadata_path = output_dir / "geometries_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved geometry fetch metadata to {metadata_path}")
        self.logger.info(
            f"Success rate: {metadata['success_rate']:.1%} ({len(retrieved_geometries)}/{len(requested_ids)})"
        )

    def _get_capabilities(self) -> dict:
        """
        Get WFS capabilities document.

        Returns:
            Parsed capabilities information
        """
        self.logger.info("Fetching WFS capabilities")

        params = {"service": "WFS", "version": "1.0.0", "request": "GetCapabilities"}

        # Add authentication parameters if available
        if self.settings.has_datafordeler_credentials:
            params.update(
                {
                    "username": self.settings.datafordeler_username,
                    "password": self.settings.datafordeler_password,
                }
            )

        try:
            response = self.session.get(self.settings.geodanmark_wfs_url, params=params, timeout=30)
            response.raise_for_status()

            # Parse XML capabilities
            root = ET.fromstring(response.content)

            # Extract basic information
            capabilities = {
                "service_title": None,
                "service_abstract": None,
                "feature_types": [],
                "operations": [],
            }

            # Parse service information
            service_elem = root.find(".//{http://www.opengis.net/wfs}Service")
            if service_elem is not None:
                title_elem = service_elem.find(".//{http://www.opengis.net/wfs}Title")
                if title_elem is not None:
                    capabilities["service_title"] = title_elem.text

                abstract_elem = service_elem.find(".//{http://www.opengis.net/wfs}Abstract")
                if abstract_elem is not None:
                    capabilities["service_abstract"] = abstract_elem.text

            # Parse feature types
            for feature_type in root.findall(".//{http://www.opengis.net/wfs}FeatureType"):
                name_elem = feature_type.find(".//{http://www.opengis.net/wfs}Name")
                title_elem = feature_type.find(".//{http://www.opengis.net/wfs}Title")

                if name_elem is not None:
                    ft_info = {
                        "name": name_elem.text,
                        "title": title_elem.text if title_elem is not None else None,
                    }
                    capabilities["feature_types"].append(ft_info)

            self.logger.info(f"Found {len(capabilities['feature_types'])} feature types")
            return capabilities

        except Exception as e:
            self.logger.error(f"Failed to get WFS capabilities: {e}")
            return {"error": str(e)}

    def _fetch_feature_sample(self, feature_type: str, max_features: int) -> dict:
        """
        Fetch a sample of features from a specific feature type.

        Args:
            feature_type: WFS feature type name
            max_features: Maximum number of features to fetch

        Returns:
            GeoJSON-like structure with sample features
        """
        params = {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": feature_type,
            "outputFormat": "application/json",
            "maxFeatures": max_features,
        }

        # Add authentication parameters if available
        if self.settings.has_datafordeler_credentials:
            params.update(
                {
                    "username": self.settings.datafordeler_username,
                    "password": self.settings.datafordeler_password,
                }
            )

        try:
            response = self.session.get(self.settings.geodanmark_wfs_url, params=params, timeout=60)
            response.raise_for_status()

            # Try to parse as JSON first
            try:
                return response.json()
            except json.JSONDecodeError:
                # If JSON parsing fails, try to parse as GML/XML
                self.logger.warning(
                    f"JSON parsing failed for {feature_type}, attempting XML parsing"
                )
                return self._parse_gml_response(response.content, feature_type)

        except Exception as e:
            self.logger.error(f"Failed to fetch features for {feature_type}: {e}")
            return {"error": str(e), "features": []}

    def _parse_gml_response(self, content: bytes, feature_type: str) -> dict:
        """
        Parse GML response when JSON is not available.

        Args:
            content: Response content as bytes
            feature_type: Feature type name for context

        Returns:
            Simplified feature structure
        """
        try:
            root = ET.fromstring(content)

            # Count features
            features = root.findall(".//{http://www.opengis.net/gml}featureMember")
            feature_count = len(features)

            # Extract a few sample attributes from the first feature
            sample_attributes = {}
            if features:
                first_feature = features[0]
                for elem in first_feature.iter():
                    if elem.text and elem.tag.startswith("{"):
                        # Clean up namespace from tag name
                        tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                        if tag_name not in ["featureMember", "boundedBy"]:
                            sample_attributes[tag_name] = elem.text

            return {
                "type": "FeatureCollection",
                "feature_count": feature_count,
                "sample_attributes": sample_attributes,
                "features": [],  # Not parsing full features from GML for now
            }

        except Exception as e:
            self.logger.error(f"Failed to parse GML response for {feature_type}: {e}")
            return {"error": f"GML parsing failed: {e}", "features": []}

    def _save_capabilities(self, output_dir: Path, capabilities: dict) -> None:
        """
        Save WFS capabilities to file.

        Args:
            output_dir: Directory to save capabilities
            capabilities: Capabilities data
        """
        capabilities_path = output_dir / "wfs_capabilities.json"
        with open(capabilities_path, "w", encoding="utf-8") as f:
            json.dump(capabilities, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved WFS capabilities to {capabilities_path}")

    def _save_combined_samples(self, output_dir: Path, samples: dict) -> None:
        """
        Save all samples to a combined file.

        Args:
            output_dir: Directory to save samples
            samples: Combined sample data
        """
        combined_path = output_dir / "geodanmark_samples.json"
        with open(combined_path, "w", encoding="utf-8") as f:
            json.dump(samples, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved combined samples to {combined_path}")

    def _save_metadata(self, output_dir: Path, max_features: int) -> None:
        """
        Save metadata about the WFS fetch.

        Args:
            output_dir: Directory to save metadata
            max_features: Max features parameter used
        """
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source_url": self.settings.geodanmark_wfs_url,
            "max_features": max_features,
            "authenticated": self.settings.has_datafordeler_credentials,
            "pipeline_version": "1.0.0",
        }

        metadata_path = output_dir / "wfs_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved WFS metadata to {metadata_path}")
