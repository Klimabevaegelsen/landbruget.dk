import asyncio
import datetime
import json
import logging
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage
from playwright.async_api import async_playwright

# Load environment variables from .env file
load_dotenv()


class GCSStorage:
    """Google Cloud Storage backend for arbejdstilsynet_inspections files."""

    def __init__(self, bucket_name, prefix="bronze/arbejdstilsynet_inspections"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.is_available = self._check_gcs_available()

    def _check_gcs_available(self):
        """Check if GCS is available (Google Cloud Storage library is installed)."""
        try:
            _ = storage
            return True
        except (ImportError, NameError):
            logging.warning(
                "Google Cloud Storage library not available. Using local storage only."
            )
            return False

    def upload_file(self, local_path, gcs_path=None):
        """Upload a file to GCS bucket."""
        if not self.is_available:
            logging.warning("GCS not available, skipping upload")
            return False

        if gcs_path is None:
            # Use the file structure from local path but with GCS prefix
            relative_path = os.path.relpath(
                local_path, start=os.path.dirname(os.path.dirname(local_path))
            )
            gcs_path = f"{self.prefix}/{relative_path}"

        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            logging.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to upload to GCS: {e}")
            return False


class BronzePipeline:
    def __init__(
        self,
        pipeline_name: str,
        source_url: str | None,
        gcs_bucket: str | None = None,
        log_level: str = "INFO",
    ):
        self.pipeline_name = pipeline_name
        self.source_url = source_url
        self.pipeline_root_dir = Path(__file__).resolve().parent.parent
        self.bronze_data_dir = self.pipeline_root_dir / "bronze" / "data"
        self.gcs_bucket = gcs_bucket
        self.log_level = log_level

        # Set logging level
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        if not self.source_url:
            logging.error(
                "SOURCE_CSV_URL for pipeline %s is not set. Please set it in the .env file.",
                self.pipeline_name,
            )
            raise ValueError("SOURCE_CSV_URL for %s is missing." % self.pipeline_name)

        # Initialize GCS storage if bucket is provided
        self.gcs = None
        if self.gcs_bucket:
            self.gcs = GCSStorage(
                bucket_name=self.gcs_bucket, prefix=f"bronze/{self.pipeline_name}"
            )
            logging.info(f"GCS storage initialized with bucket: {self.gcs_bucket}")

    async def fetch_data_with_playwright(
        self, filters_to_apply=None
    ) -> list[tuple[str, bytes]]:
        """Fetches data using Playwright automation. Returns list of (filter_name, csv_bytes)."""
        if filters_to_apply is None:
            filters_to_apply = [
                {"name": "Anlægsarbejde", "search_term": "Anlægs"},
                {"name": "Landbrug, skovbrug og fiskeri", "search_term": "Landbrug"},
                {"name": "Slagterier", "search_term": "Slagter"},
            ]
        results = []
        async with async_playwright() as playwright:
            is_docker = os.environ.get("DOCKER_ENV") or os.path.exists("/.dockerenv")
            browser_args = []
            if is_docker:
                browser_args = [
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-gpu",
                    "--disable-software-rasterizer",
                ]
            browser_options = {"args": browser_args, "headless": True}
            browser = await playwright.chromium.launch(**browser_options)
            context = await browser.new_context(
                accept_downloads=True, viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()

            page.on("crash", lambda: logging.error("Browser page crashed"))
            page.on("pageerror", lambda error: logging.error(f"Page error: {error}"))

            await page.goto(self.source_url, wait_until="networkidle", timeout=120000)
            await page.wait_for_timeout(5000)

            try:
                await page.wait_for_selector(
                    'iframe[title="Power BI Report Viewer"]',
                    state="visible",
                    timeout=30000,
                )
                powerbi_frame = page.frame_locator(
                    'iframe[title="Power BI Report Viewer"]'
                )
                await page.wait_for_timeout(5000)
                await powerbi_frame.locator(
                    '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[2]/transform/div/div[2]/div/div/visual-modern/div/div'
                ).click(timeout=120000)
                await page.wait_for_timeout(3000)
                await powerbi_frame.locator(
                    '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[2]/transform/div/div[2]/visual-container[1]/transform/div/div[2]/div/div/visual-modern/div'
                ).click(timeout=10000)
                await page.wait_for_timeout(3000)

                filter_selector_xpath = '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[1]/transform/div/div[2]/visual-container[3]/transform/div/div[2]/div/div/visual-modern/div/div/div[2]/div'
                hover_target_xpath = '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[3]/transform/div/div[2]/div/div/visual-modern/div/div/div[2]/div[1]/div[1]/div/div/div/div[8]'
                options_button_xpath = '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[3]/transform/div/visual-container-header/div/div/div/visual-container-options-menu/visual-header-item-container/div/button'

                for filter_info in filters_to_apply:
                    filter_name = filter_info["name"]
                    search_term = filter_info["search_term"]
                    while True:
                        try:
                            await powerbi_frame.locator(filter_selector_xpath).click(
                                timeout=10000
                            )
                            await asyncio.sleep(0.2)
                            await page.keyboard.press("ControlOrMeta+A")
                            await asyncio.sleep(0.2)
                            await page.keyboard.press("Backspace")
                            await page.keyboard.type(search_term, delay=100)
                            await powerbi_frame.locator(
                                "span:text-is('%s')" % filter_name
                            ).click(timeout=5000)
                            break
                        except Exception as e:
                            logging.error(
                                "[Playwright] Could not apply filter '%s': %s",
                                filter_name,
                                e,
                            )
                            continue

                    await page.wait_for_timeout(3000)

                    try:
                        await powerbi_frame.locator(hover_target_xpath).hover()
                        await powerbi_frame.locator(options_button_xpath).click()
                        try:
                            await powerbi_frame.locator('//*[@id="0"]').click(
                                timeout=3000
                            )
                        except:
                            try:
                                await powerbi_frame.locator(
                                    "span:text-is('Export data')"
                                ).click(timeout=3000)
                            except:
                                await powerbi_frame.locator(
                                    "span:text-matches('Export', 'i')"
                                ).first.click(timeout=3000)

                        await page.wait_for_timeout(1000)

                        try:
                            await powerbi_frame.locator(
                                '//div[contains(@class, "export-data-dialog")]//*[contains(text(), "File format") or contains(@aria-label, "format")]//button'
                            ).click(timeout=5000)
                        except:
                            await powerbi_frame.locator(
                                "mat-dialog-content pbi-dropdown button"
                            ).click(timeout=5000)

                        await page.wait_for_timeout(500)
                        try:
                            await powerbi_frame.locator(
                                "div.pbi-dropdown-item:has-text('CSV')"
                            ).click(timeout=5000)
                        except:
                            await (
                                powerbi_frame.locator("pbi-dropdown-item")
                                .nth(1)
                                .click(timeout=5000)
                            )

                        async with page.expect_download(timeout=30000) as download_info:
                            try:
                                await powerbi_frame.locator(
                                    "mat-dialog-actions button:has-text('Export')"
                                ).click(timeout=5000)
                            except:
                                await powerbi_frame.locator(
                                    "mat-dialog-actions button"
                                ).first.click(timeout=5000)

                        download = await download_info.value
                        # Save download to a temp file and return bytes
                        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                            await download.save_as(tmp_file.name)
                            with open(tmp_file.name, "rb") as f:
                                csv_bytes = f.read()
                        results.append((filter_name, csv_bytes))
                        logging.info(
                            "[Playwright] Successfully downloaded CSV for '%s'",
                            filter_name,
                        )
                        # wait for the download to complete
                        await page.wait_for_timeout(5000)

                    except Exception as e:
                        logging.error(
                            "[Playwright] Export failed for '%s': %s", filter_name, e
                        )
                        continue

            finally:
                await browser.close()

        return results

    def save_raw_data(
        self, data: bytes, filter_name: str = None
    ) -> tuple[Path | None, str | None]:
        """Saves raw data to a timestamped directory in the bronze layer."""
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        storage_dir = self.bronze_data_dir / timestamp_str
        try:
            storage_dir.mkdir(parents=True, exist_ok=True)
            if filter_name:
                safe_filter_name = (
                    filter_name.replace(" ", "_").replace(",", "").replace("/", "_")
                )
                data_file_path = storage_dir / ("data_%s.csv" % safe_filter_name)
            else:
                data_file_path = storage_dir / "data.csv"
            with open(data_file_path, "wb") as f:
                f.write(data)
            logging.info(
                "[%s] Raw data saved to: %s", self.pipeline_name, data_file_path
            )
            return data_file_path, timestamp_str
        except OSError as e:
            logging.error(
                "[%s] Error saving raw data to %s: %s",
                self.pipeline_name,
                storage_dir,
                e,
            )
            return None, None

    def create_metadata_file(
        self, fetch_timestamp_dir_name: str, data_file_path: Path
    ) -> None:
        """Appends a metadata entry to metadata.json for the fetched data."""
        metadata_file_path = data_file_path.parent / "metadata.json"

        record_count = 0
        try:
            with open(data_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if lines:
                record_count = max(0, len(lines) - 1)
            logging.info(
                "[%s] Calculated record count: %d from %s",
                self.pipeline_name,
                record_count,
                data_file_path,
            )
        except Exception as e:
            logging.error(
                "[%s] Could not read or count records from %s: %s. Setting record_count to 0.",
                self.pipeline_name,
                data_file_path,
                e,
            )
            record_count = 0

        description = (
            "Raw inspection results from Arbejdstilsynet."
            if self.pipeline_name == "arbejdstilsynet_inspections"
            else f"Raw data for {self.pipeline_name}."
        )

        metadata = {
            "source_url": self.source_url,
            "fetch_timestamp_utc_iso": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
            "fetch_timestamp_dirname": fetch_timestamp_dir_name,
            "description": description,
            "pipeline_name": self.pipeline_name,
            "layer": "bronze",
            "file_format": data_file_path.suffix[1:].lower()
            if data_file_path.suffix
            else "unknown",
            "data_filename": data_file_path.name,
            # Fix: always use relative path from pipeline_root_dir, not /data
            "relative_data_file_path": str(
                data_file_path.relative_to(self.pipeline_root_dir)
            ),
            "record_count": record_count,
        }

        # Load existing metadata list or initialize new one
        metadata_list = []
        if metadata_file_path.exists():
            try:
                with open(metadata_file_path, "r", encoding="utf-8") as f:
                    metadata_list = json.load(f)
                    if not isinstance(metadata_list, list):
                        logging.warning(
                            "[%s] metadata.json is not a list. Overwriting it.",
                            self.pipeline_name,
                        )
                        metadata_list = []
            except Exception as e:
                logging.error(
                    "[%s] Error reading existing metadata.json: %s. Overwriting it.",
                    self.pipeline_name,
                    e,
                )
                metadata_list = []

        metadata_list.append(metadata)

        try:
            with open(metadata_file_path, "w", encoding="utf-8") as f:
                json.dump(metadata_list, f, indent=4)
            logging.info(
                "[%s] Appended metadata entry to: %s",
                self.pipeline_name,
                metadata_file_path,
            )
        except OSError as e:
            logging.error(
                "[%s] Error saving metadata file to %s: %s",
                self.pipeline_name,
                metadata_file_path,
                e,
            )

    def run(self) -> None:
        """Executes the bronze layer pipeline steps using Playwright only."""
        logging.info(
            "Starting bronze layer processing for pipeline: %s", self.pipeline_name
        )
        filters = [
            {"name": "Anlægsarbejde", "search_term": "Anlægs"},
            {"name": "Landbrug, skovbrug og fiskeri", "search_term": "Landbrug"},
            {"name": "Slagterier", "search_term": "Slagter"},
        ]
        results = asyncio.run(self.fetch_data_with_playwright(filters))
        if not results:
            logging.error("No data fetched for any filter. Exiting bronze run.")
            return

        from io import BytesIO

        import pandas as pd

        # Create temp folder for initial raw CSVs
        temp_dir = self.bronze_data_dir / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        dfs = []
        for filter_name, csv_bytes in results:
            try:
                df = pd.read_csv(BytesIO(csv_bytes), encoding="utf-8")
                dfs.append(df)
                safe_filter_name = (
                    filter_name.replace(" ", "_").replace(",", "").replace("/", "_")
                )
                temp_csv_path = temp_dir / f"data_{safe_filter_name}.csv"
                with open(temp_csv_path, "wb") as f:
                    f.write(csv_bytes)
            except Exception as e:
                logging.error(
                    "[Bronze] Could not parse/save CSV for filter %s: %s",
                    filter_name,
                    e,
                )

        if not dfs:
            logging.error("No valid CSVs to merge. Exiting bronze run.")
            return

        merged_df = pd.concat(dfs, ignore_index=True)

        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        storage_dir = self.bronze_data_dir / timestamp_str
        storage_dir.mkdir(parents=True, exist_ok=True)

        merged_file_path = storage_dir / "data_merged.csv"
        merged_df.to_csv(merged_file_path, index=False, encoding="utf-8")
        logging.info(
            "[%s] Merged data saved to: %s", self.pipeline_name, merged_file_path
        )

        # Create metadata
        self.create_metadata_file(timestamp_str, merged_file_path)

        # Upload merged file to GCS if available
        if self.gcs:
            self.gcs.upload_file(local_path=str(merged_file_path))

        # Delete the temp directory and all its contents
        import shutil

        try:
            shutil.rmtree(temp_dir)
            logging.info("Temporary folder '%s' deleted after merging.", temp_dir)
        except Exception as e:
            logging.warning("Could not delete temp folder '%s': %s", temp_dir, e)

        logging.info(
            "[%s] Bronze layer processing completed for merged data.",
            self.pipeline_name,
        )


def main(log_level: str = "INFO", gcs_bucket: str | None = None) -> None:
    # Environment variable loading is done at the top of the script
    # load_dotenv()

    pipeline_name = "arbejdstilsynet_inspections"
    source_url = "https://publicdata.at.dk/reports/powerbi/Data%20på%20nettet/Tilsynsindblikket?rs:embed=true"  # Hardcoded URL

    pipeline = BronzePipeline(
        pipeline_name=pipeline_name,
        source_url=source_url,
        gcs_bucket=gcs_bucket,
        log_level=log_level,
    )
    asyncio.run(pipeline.run())


if __name__ == "__main__":
    main()
