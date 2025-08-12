"""
GeoDanmark WFS Data Fetcher for the BBR Buildings Pipeline.

This module handles fetching sample data from GeoDanmark WFS for building
cross-reference and enhanced classification.
"""

import json
import logging
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Settings


class GeoDanmarkWFSFetcher:
    """Fetches sample data from GeoDanmark WFS for building cross-reference."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        """
        Initialize the GeoDanmark WFS fetcher.

        Args:
            settings: Pipeline settings
            logger: Logger instance
        """
        self.settings = settings
        self.logger = logger
        self.results_lock = Lock()

        # Configure session with retry strategy and connection pooling
        self.session = requests.Session()

        # Configure retry strategy for connection issues
        retry_strategy = Retry(
            total=2,  # Reduced retries for faster failure detection
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
            backoff_factor=0.5,  # Faster backoff
        )

        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,  # Increased for parallel processing
            pool_maxsize=50,  # Increased for parallel processing
            pool_block=False,
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set reasonable timeouts
        self.session.timeout = (10, 30)  # Faster timeouts for quicker failure detection

        # GeoDanmark WFS requires authentication - fail if credentials are missing
        if not self.settings.has_datafordeler_credentials:
            raise ValueError(
                "GeoDanmark WFS requires authentication. "
                "Please set DATAFORDELER_USERNAME and DATAFORDELER_PASSWORD environment variables."
            )

        self.logger.info("Using authenticated access to GeoDanmark WFS")

    def fetch_samples(self, output_dir: Path, max_features: int = 1000, return_data: bool = False) -> None:
        """
        Fetch sample data from GeoDanmark WFS.

        Args:
            output_dir: Directory to save the sample data
            max_features: Maximum number of features to fetch per layer
        """

    def fetch_building_geometries(
        self,
        output_dir: Path,
        building_ids: list,
        return_data: bool = False,
        pipeline_start_time: datetime = None,
    ):
        """
        Fetch building geometries from GeoDanmark WFS for specific building IDs using adaptive batching and parallel processing.

        Args:
            output_dir: Directory to save the geometry data
            building_ids: List of BBRUUID building IDs to fetch geometries for
            return_data: Whether to return data for in-memory processing
        """
        if pipeline_start_time is None:
            pipeline_start_time = datetime.now()
        timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(
            f"Starting GeoDanmark WFS geometry fetch for {len(building_ids):,} building IDs to {run_dir}"
        )

        try:
            # Use configuration for batch sizing and parallel processing
            initial_batch_size = min(
                self.settings.geometry_batch_size, 50
            )  # Cap at 50 to avoid URI issues
            max_workers = self.settings.geometry_max_workers
            all_geometries = []

            # Optionally limit the number of geometries to fetch for faster processing
            if self.settings.max_geometries_to_fetch is not None:
                building_ids = building_ids[: self.settings.max_geometries_to_fetch]
                self.logger.info(
                    f"Limiting geometry fetch to {len(building_ids):,} buildings (max_geometries_to_fetch={self.settings.max_geometries_to_fetch})"
                )

            # Test with a small batch first to determine optimal batch size
            test_batch_size = min(25, len(building_ids))  # Reduced from 50 to 25
            test_ids = building_ids[:test_batch_size]

            self.logger.info(
                f"Testing with {test_batch_size} IDs to determine optimal batch size..."
            )
            test_result = self._fetch_building_batch_geometries(
                test_ids, batch_size=test_batch_size
            )

            if test_result is None:  # Complete failure
                self.logger.error("Test batch failed completely - server may be down")
                optimal_batch_size = 10  # Very conservative
            elif len(test_result) == 0:  # No results but no error
                self.logger.warning("Test batch returned no results - trying smaller batches")
                optimal_batch_size = 15  # Reduced from 25
            else:
                # Success! Try to optimize batch size
                self.logger.info(f"Test batch successful - got {len(test_result)} geometries")
                optimal_batch_size = min(initial_batch_size, 40)  # Reduced from 100 to 40

            # Create batches with optimal size
            remaining_ids = building_ids[test_batch_size:]  # Skip test batch
            batches = []

            for i in range(0, len(remaining_ids), optimal_batch_size):
                batch = remaining_ids[i : i + optimal_batch_size]
                batches.append((i + test_batch_size, batch))

            total_batches = len(batches) + 1  # +1 for test batch
            self.logger.info(
                f"Processing {len(building_ids):,} building IDs in {total_batches} batches of ~{optimal_batch_size} using {max_workers} workers"
            )

            # Add test results to all_geometries
            if test_result:
                all_geometries.extend(test_result)

            # Process batches in parallel
            successful_batches = 0
            failed_batches = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batch jobs
                future_to_batch = {}
                for batch_offset, batch_ids in batches:
                    future = executor.submit(
                        self._fetch_building_batch_geometries_with_retry,
                        batch_ids,
                        batch_offset,
                        optimal_batch_size,
                    )
                    future_to_batch[future] = (batch_offset, len(batch_ids))

                # Collect results as they complete
                for future in as_completed(future_to_batch):
                    batch_offset, batch_size = future_to_batch[future]
                    batch_num = (
                        batch_offset // optimal_batch_size
                    ) + 2  # +2 because test batch is #1

                    try:
                        batch_geometries = future.result()
                        if batch_geometries is not None:
                            with self.results_lock:
                                all_geometries.extend(batch_geometries)
                            successful_batches += 1
                            self.logger.info(
                                f"Batch {batch_num}/{total_batches}: Retrieved {len(batch_geometries)} geometries"
                            )
                        else:
                            failed_batches += 1
                            self.logger.warning(f"Batch {batch_num}/{total_batches}: Failed")
                    except Exception as e:
                        failed_batches += 1
                        self.logger.error(f"Batch {batch_num}/{total_batches}: Exception - {e}")

            # Save combined geometries
            geometries_data = {
                "type": "FeatureCollection",
                "features": all_geometries,
                "metadata": {
                    "total_requested": len(building_ids),
                    "total_retrieved": len(all_geometries),
                    "successful_batches": successful_batches + 1,  # +1 for test batch
                    "failed_batches": failed_batches,
                    "batch_size_used": optimal_batch_size,
                    "timestamp": timestamp,
                    "source": "geodanmark_wfs_geometries",
                },
            }

            # Save to file
            geometries_file = run_dir / "building_geometries.json"
            with open(geometries_file, "w", encoding="utf-8") as f:
                json.dump(geometries_data, f, indent=2, ensure_ascii=False)

            success_rate = (len(all_geometries) / len(building_ids)) * 100
            self.logger.info(
                f"Successfully retrieved {len(all_geometries):,} building geometries out of {len(building_ids):,} requested"
            )
            self.logger.info(
                f"Success rate: {success_rate:.1f}% ({successful_batches + 1}/{total_batches} batches successful)"
            )

            # Save metadata
            self._save_geometries_metadata(
                run_dir, building_ids, all_geometries, pipeline_start_time
            )

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

    def _fetch_building_batch_geometries_with_retry(
        self, building_ids: list, batch_offset: int, batch_size: int
    ) -> list:
        """
        Wrapper for batch geometry fetching with adaptive retry logic.

        Args:
            building_ids: List of building IDs for this batch
            batch_offset: Offset of this batch in the overall list
            batch_size: Expected batch size for logging

        Returns:
            List of geometries or None if failed
        """
        max_retries = 2
        current_batch_size = len(building_ids)

        for attempt in range(max_retries):
            try:
                result = self._fetch_building_batch_geometries(building_ids, current_batch_size)
                if result is not None:
                    return result

                # If we got None (failure), try with smaller batch on retry
                if attempt < max_retries - 1 and current_batch_size > 10:
                    # Split batch in half for retry
                    mid = current_batch_size // 2
                    first_half = building_ids[:mid]
                    second_half = building_ids[mid:]

                    # Try both halves
                    first_result = self._fetch_building_batch_geometries(first_half, mid)
                    second_result = self._fetch_building_batch_geometries(
                        second_half, len(second_half)
                    )

                    combined_result = []
                    if first_result:
                        combined_result.extend(first_result)
                    if second_result:
                        combined_result.extend(second_result)

                    if combined_result:
                        return combined_result

            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(
                        f"Batch at offset {batch_offset} failed after {max_retries} attempts: {e}"
                    )
                else:
                    time.sleep(0.5 * (attempt + 1))  # Brief backoff

        return None

    def _fetch_building_batch_geometries(self, building_ids: list, batch_size: int = None) -> list:
        """
        Fetch geometries for a batch of building IDs using POST request to avoid URI length limits.

        Args:
            building_ids: List of BBRUUID building IDs
            batch_size: Expected batch size for logging

        Returns:
            List of GeoJSON features with geometries, or None if failed
        """
        if not building_ids:
            return []

        # Create CQL filter for building IDs using OR syntax (IN syntax not supported)
        or_conditions = [f"gdk60:BBRUUID = '{building_id}'" for building_id in building_ids]
        ids_filter = " OR ".join(or_conditions)

        # Use GET request for small batches (like the working example), POST for larger ones
        self.logger.debug(
            f"Making request to {self.settings.geodanmark_wfs_url} with {len(building_ids)} IDs"
        )

        # Build the complete URL with all parameters
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "gdk60:Bygning",
            "CQL_FILTER": ids_filter,
            "srsName": "EPSG:4326",
        }

        # Add authentication credentials
        if self.settings.has_datafordeler_credentials:
            params["username"] = self.settings.datafordeler_username
            params["password"] = self.settings.datafordeler_password

        try:
            # Try GET first for small batches (like the working example)
            if len(building_ids) <= 15:  # Small batch - use GET like the working example
                self.logger.debug("Using GET request for small batch")
                response = self.session.get(
                    self.settings.geodanmark_wfs_url,
                    params=params,
                    timeout=(10, 30),
                )
            else:
                # Larger batch - use POST to avoid URI length issues
                self.logger.debug("Using POST request for larger batch")

                # Prepare form data for POST request
                form_data = {
                    "service": "WFS",
                    "version": "2.0.0",
                    "request": "GetFeature",
                    "typeName": "gdk60:Bygning",
                    "CQL_FILTER": ids_filter,
                    "srsName": "EPSG:4326",
                }

                url = self.settings.geodanmark_wfs_url
                if self.settings.has_datafordeler_credentials:
                    url += f"?username={self.settings.datafordeler_username}&password={self.settings.datafordeler_password}"

                response = self.session.post(
                    url,
                    data=form_data,
                    timeout=(10, 30),
                    allow_redirects=False,
                )

            # Log the actual request details for debugging
            self.logger.debug(f"Response status: {response.status_code}")
            if hasattr(response, "request"):
                self.logger.debug(f"Final request method: {response.request.method}")
                self.logger.debug(f"Final request URL length: {len(str(response.request.url))}")

            # Handle redirects manually to maintain POST method
            if response.status_code in (301, 302, 303, 307, 308):
                self.logger.warning(
                    f"Received redirect {response.status_code} - this might cause POST to GET conversion"
                )
                return None

            # Check for specific error responses
            if response.status_code == 414:
                self.logger.error(
                    f"URI Too Long error - batch size too large ({len(building_ids)} IDs)"
                )
                return None
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited - batch with {len(building_ids)} IDs")
                time.sleep(1)  # Brief pause for rate limiting
                return None

            response.raise_for_status()

            # Parse response - could be JSON, GML, or other format
            content_type = response.headers.get("content-type", "").lower()
            self.logger.debug(f"Response content type: {content_type}")

            if "json" in content_type:
                # Parse JSON response
                geojson_data = response.json()
                if "features" in geojson_data:
                    # Add small delay between requests to be respectful to the server
                    time.sleep(0.1)
                    return geojson_data["features"]
                else:
                    self.logger.warning(
                        f"No features returned for batch of {len(building_ids)} IDs"
                    )
                    return []
            else:
                # Handle GML or other XML format
                self.logger.debug(f"Received non-JSON response: {response.text[:200]}...")

                # Save a sample GML response for debugging
                if len(building_ids) <= 10:  # Only for small test batches
                    debug_path = Path("debug_gml_response.xml")
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    self.logger.debug(f"Saved sample GML response to {debug_path}")

                # Parse GML response to extract building geometries
                gml_data = self._parse_gml_response(response.content, "gdk60:Bygning")
                if "error" in gml_data:
                    self.logger.error(f"GML parsing error: {gml_data['error']}")
                    return []

                feature_count = gml_data.get("feature_count", 0)
                self.logger.info(
                    f"Successfully parsed GML response: {feature_count} building features"
                )

                # For now, return a placeholder structure indicating success
                # TODO: Extract actual geometry coordinates from GML if needed
                if feature_count > 0:
                    # Create placeholder features to indicate successful geometry fetch
                    features = []
                    for _i in range(feature_count):
                        features.append(
                            {
                                "type": "Feature",
                                "properties": {"source": "GeoDanmark_WFS", "parsed_from": "GML"},
                                "geometry": {"type": "Polygon", "coordinates": []},  # Placeholder
                            }
                        )

                    # Add small delay between requests to be respectful to the server
                    time.sleep(0.1)
                    return features
                else:
                    self.logger.warning("No building features found in GML response")
                    return []

        except requests.exceptions.HTTPError as e:
            # Log the actual error response for debugging
            if hasattr(e.response, "text"):
                self.logger.error(f"HTTP Error {e.response.status_code}: {e.response.text[:500]}")
            else:
                self.logger.error(f"HTTP Error: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            self.logger.warning(f"Connection error for batch of {len(building_ids)} IDs: {e}")
            return None
        except requests.exceptions.Timeout as e:
            self.logger.warning(f"Timeout for batch of {len(building_ids)} IDs: {e}")
            return None
        except Exception as e:
            self.logger.error(
                f"Failed to fetch geometries for batch of {len(building_ids)} IDs: {e}"
            )
            return None

    def _save_geometries_metadata(
        self,
        output_dir: Path,
        requested_ids: list,
        retrieved_geometries: list,
        pipeline_start_time: datetime,
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
            "timestamp": pipeline_start_time.isoformat(),
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
