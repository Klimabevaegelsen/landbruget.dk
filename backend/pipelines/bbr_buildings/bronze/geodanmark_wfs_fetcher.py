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

        # Set up authentication if credentials are available
        if self.settings.has_datafordeler_credentials:
            self.session.auth = (
                self.settings.datafordeler_username,
                self.settings.datafordeler_password,
            )
            self.logger.info("Using authenticated access to GeoDanmark WFS")
        else:
            self.logger.warning(
                "No Datafordeleren credentials found - using unauthenticated access"
            )

    def fetch_samples(self, output_dir: Path, max_features: int = 1000) -> None:
        """
        Fetch sample data from GeoDanmark WFS.

        Args:
            output_dir: Directory to save the sample data
            max_features: Maximum number of features to fetch per layer
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting GeoDanmark WFS sample fetch to {run_dir}")

        try:
            # Feature types to fetch
            feature_types = [
                "gdk60:Bygning",
                "gdk60:TekniskAnlaegFlade",
                "gdk60:TekniskAnlaegPunkt",
            ]

            # First, get WFS capabilities
            capabilities = self._get_capabilities()
            self._save_capabilities(run_dir, capabilities)

            # Fetch sample data for each feature type
            samples = {}
            for feature_type in feature_types:
                try:
                    self.logger.info(f"Fetching sample data for {feature_type}")
                    sample_data = self._fetch_feature_sample(feature_type, max_features)
                    samples[feature_type] = sample_data

                    # Save individual sample file
                    sample_file = run_dir / f"{feature_type.replace(':', '_')}_sample.json"
                    with open(sample_file, "w", encoding="utf-8") as f:
                        json.dump(sample_data, f, indent=2, ensure_ascii=False)

                    self.logger.info(
                        f"Saved {len(sample_data.get('features', []))} features to {sample_file}"
                    )

                except Exception as e:
                    self.logger.error(f"Failed to fetch sample for {feature_type}: {e}")
                    samples[feature_type] = {"error": str(e)}

            # Save combined samples
            self._save_combined_samples(run_dir, samples)

            # Save metadata
            self._save_metadata(run_dir, max_features)

            self.logger.info(f"Successfully fetched GeoDanmark WFS samples to {run_dir}")

        except Exception as e:
            self.logger.error(f"Failed to fetch GeoDanmark WFS samples: {e}")
            raise

    def _get_capabilities(self) -> dict:
        """
        Get WFS capabilities document.

        Returns:
            Parsed capabilities information
        """
        self.logger.info("Fetching WFS capabilities")

        params = {"service": "WFS", "version": "1.0.0", "request": "GetCapabilities"}

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
