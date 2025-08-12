import asyncio
import json
import logging
import os
import re
import subprocess
import time
import traceback
from datetime import datetime
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


async def fetch(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


def fetch_text(soup, selector):
    if soup.select_one(selector) is None:
        return None
    return soup.select_one(selector).text.strip()


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe storage."""
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    filename = re.sub(r"[^\w\s._-]", "_", filename)
    filename = re.sub(r"\s+", "_", filename)
    filename = filename.strip("._")

    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        if ext:
            filename = name[:180] + "_truncated" + ext
        else:
            filename = filename[:200]

    return filename or "unnamed_file"


class EnhancedDMACompanyDetailScraper:
    def __init__(self, data, output_base_dir="/tmp/dma_data", start_date=None, end_date=None):
        self.data = data
        self.start_date = start_date
        self.end_date = end_date
        self.output_base_dir = Path(output_base_dir)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.output_base_dir / self.timestamp
        self.session_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Enhanced DMA scraper initialized - output dir: {self.session_dir}")

    async def download_pdf(self, session, pdf_url, company_dir, section, index):
        """Download PDF and save to organized directory structure."""
        try:
            logger.info(f"  Downloading PDF: {pdf_url}")
            async with session.get(pdf_url) as response:
                if response.status == 200:
                    content = await response.read()

                    # Create filename
                    filename = f"{section}_{index}.pdf"
                    pdf_path = company_dir / "pdfs" / filename
                    pdf_path.parent.mkdir(exist_ok=True)

                    # Save PDF
                    with open(pdf_path, "wb") as f:
                        f.write(content)

                    logger.info(f"  ✅ Downloaded PDF: {len(content)} bytes -> {pdf_path}")
                    return str(pdf_path)
                else:
                    logger.warning(f"  ❌ Failed to download PDF: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"  ❌ Error downloading PDF {pdf_url}: {e}")
            return None

    async def scrape_details(self, session, url):
        try:
            html = await fetch(session, url)
            soup = BeautifulSoup(html, "html.parser")

            data = {"title": soup.find("div", class_="dma-content-header").find("span").text.strip()}

            for section in [
                "Grunddata",
                "Adresse",
                "Aktiviteter/anlæg og miljøkategorier",
                "Myndighed",
                "IED-oplysninger (Direktivet om industrielle emissioner)",
            ]:
                section_div = soup.find("div", string=section)
                if section_div:
                    section_body = section_div.find_next("div", class_="card-body")
                    for dt, dd in zip(section_body.find_all("dt"), section_body.find_all("dd")):
                        key = dt.text.strip(":")
                        value = dd.text.strip()
                        data[key] = value

            # Filter based on Tilsynsdato if dates are provided
            if self.start_date and self.end_date and "Tilsynsdato" in data:
                try:
                    tilsynsdato = datetime.strptime(data["Tilsynsdato"], "%d-%m-%Y")
                    if not (self.start_date <= tilsynsdato <= self.end_date):
                        return {}
                except ValueError:
                    logger.warning(f"Could not parse Tilsynsdato: {data['Tilsynsdato']}")

            return data
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    async def scrape_table_url(self, session, url, company_dir, section, index):
        try:
            html = await fetch(session, url)
            soup = BeautifulSoup(html, "html.parser")
            cvr_selector = "div:nth-child(2) > div.card-body > dl > dd:nth-child(4)"
            chr_selector = "div:nth-child(2) > div.card-body > dl > dd:nth-child(8)"
            cvr = fetch_text(soup, cvr_selector)
            chr = fetch_text(soup, chr_selector)
            pdf_url_selector = "#hent-0"

            pdf_path = None
            if soup.select_one(pdf_url_selector) is not None:
                pdf_url = "https://dma.mst.dk" + soup.select_one(pdf_url_selector).get("href")
                pdf_path = await self.download_pdf(session, pdf_url, company_dir, section, index)

            return {
                "pdf_url": pdf_url if soup.select_one(pdf_url_selector) else None,
                "pdf_path": pdf_path,
                "cvr": cvr,
                "chr": chr,
            }
        except Exception as e:
            logger.error(f"Error scraping PDF URL from {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def scrape_table(self, session, url, table_id, company_dir):
        try:
            html = await fetch(session, url)
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", id=table_id)

            if not table:
                return []

            headers = [th.text.strip() for th in table.find_all("th")]
            rows = []

            for row in table.find_all("tr")[1:]:  # Skip header row
                cols = row.find_all("td")
                if len(cols) == len(headers):
                    row_data = {}
                    for i, col in enumerate(cols):
                        row_data[headers[i]] = col.text.strip()
                        if col.find("a"):
                            row_data[f"{headers[i]}_url"] = "https://dma.mst.dk" + col.find("a")["href"]
                    rows.append(row_data)

            # Download PDFs for each row
            for index, row in enumerate(rows):
                if "_url" in row:
                    pdf_data = await self.scrape_table_url(
                        session, row["_url"], company_dir, table_id.replace("-tabel", ""), index
                    )
                    if pdf_data:
                        row.update(pdf_data)

            return rows
        except Exception as e:
            logger.error(f"Error scraping table from {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    async def process_miljoeaktoer(self, session, company_item):
        url = company_item["miljoeaktoerUrl"]
        logger.info(f"Processing {url}")
        try:
            data = await self.scrape_details(session, url)
            data["miljoeaktoerUrl"] = url

            # Get CVR for directory naming - use from original company data first
            cvr = company_item.get("cvr_number", "unknown_cvr")
            if not cvr or cvr == "unknown_cvr":
                cvr = data.get("CVR-nr.", "unknown_cvr")
            cvr = sanitize_filename(str(cvr))

            # Create company directory: timestamp/cvr/
            company_dir = self.session_dir / cvr
            company_dir.mkdir(exist_ok=True)

            # Save basic company data
            import json

            with open(company_dir / "company_data.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Scrape Tilsyn, Håndhævelser, and Afgørelser with PDF downloads
            for section, table_id in [
                ("Tilsyn", "tilsyn-tabel"),
                ("Håndhævelser", "haandhaevelse-tabel"),
                ("Afgørelser", "afgoerelser-tabel"),
            ]:
                section_data = await self.scrape_table(session, url, table_id, company_dir)
                data[section] = section_data

                if section_data:
                    logger.info(f"  Found {len(section_data)} records in {section}")
                    # Save section data
                    with open(company_dir / f"{section.lower()}_data.json", "w", encoding="utf-8") as f:
                        json.dump(section_data, f, indent=2, ensure_ascii=False)

            # Save complete data
            with open(company_dir / "complete_data.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            return data
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return None

    async def process_miljoeaktoer_for_company_file_path(self, max_concurrent=3):
        sem = asyncio.Semaphore(max_concurrent)

        # Create progress bar for company processing
        pbar = tqdm(total=len(self.data), desc="Processing companies", unit="company")

        # Track failures
        processed_count = 0
        failure_count = 0

        async with aiohttp.ClientSession() as session:

            async def bounded(item):
                nonlocal processed_count, failure_count
                async with sem:
                    result = await self.process_miljoeaktoer(session, item)
                    processed_count += 1

                    if result is None:
                        failure_count += 1
                        self.consecutive_failures += 1
                        self.total_failures += 1

                        # Check circuit breaker
                        if self.check_circuit_breaker(processed_count, failure_count):
                            pbar.set_description("❌ CIRCUIT BREAKER TRIGGERED - STOPPING")
                            pbar.close()
                            raise Exception("Circuit breaker triggered - too many failures")
                    else:
                        # Reset consecutive failures on success
                        self.consecutive_failures = 0

                    # Update progress bar with failure info
                    pbar.set_postfix(
                        {
                            "Failures": failure_count,
                            "Rate": f"{failure_count / processed_count:.1%}" if processed_count > 0 else "0%",
                            "Consecutive": self.consecutive_failures,
                        }
                    )
                    pbar.update(1)
                    return result

            tasks = [bounded(item) for item in self.data]

            try:
                company_details_data = await asyncio.gather(*tasks)
            except Exception as e:
                logger.error(f"❌ Processing stopped due to circuit breaker: {e}")
                pbar.close()
                raise

        pbar.close()

        # Final failure check
        if failure_count > 0:
            failure_rate = failure_count / len(self.data)
            logger.warning(f"⚠️  Processing completed with {failure_count} failures ({failure_rate:.1%} failure rate)")

            if failure_rate > self.max_failure_rate:
                logger.error(f"❌ Final failure rate {failure_rate:.1%} exceeds threshold {self.max_failure_rate:.1%}")
                raise Exception(f"Pipeline failed: {failure_rate:.1%} failure rate exceeds {self.max_failure_rate:.1%}")

        # Save session summary
        summary = {
            "timestamp": self.timestamp,
            "total_companies": len(self.data),
            "successful_companies": len([d for d in company_details_data if d]),
            "failed_companies": failure_count,
            "failure_rate": failure_count / len(self.data) if len(self.data) > 0 else 0,
            "output_directory": str(self.session_dir),
        }

        with open(self.session_dir / "session_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info(f"Session complete: {summary}")
        return company_details_data


class FullDMAPipeline:
    """Full DMA pipeline with auto-shutdown capability."""

    def __init__(self, output_base_dir="/tmp/dma_data", delay_seconds=1.0):  # 1 second delay
        self.output_base_dir = Path(output_base_dir)
        self.delay_seconds = delay_seconds
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Fixed timestamp for entire pipeline
        self.session_dir = self.output_base_dir / self.timestamp
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Progress tracking
        self.progress_file = self.session_dir / "progress.json"
        self.completion_file = self.session_dir / "COMPLETED.txt"

        # Circuit breaker for failure handling
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10  # Stop after 10 consecutive failures
        self.total_failures = 0
        self.max_failure_rate = 0.2  # Stop if >20% failure rate after 100 companies

        # New configurable parameters
        self.max_companies = None  # Process all companies by default
        self.start_date = None  # No date filtering by default
        self.end_date = None  # No date filtering by default
        self.auto_shutdown_enabled = True  # Auto-shutdown enabled by default

        logger.info(f"Full DMA pipeline initialized - output dir: {self.session_dir}")
        logger.info(f"Server-friendly delay between requests: {delay_seconds}s, max 3 concurrent")
        logger.info(
            f"Circuit breaker: max {self.max_consecutive_failures} consecutive failures, max {self.max_failure_rate * 100}% failure rate"
        )

    def check_circuit_breaker(self, total_processed, total_failures):
        """Check if we should stop due to too many failures."""
        # Check consecutive failures
        if self.consecutive_failures >= self.max_consecutive_failures:
            logger.error(f"❌ CIRCUIT BREAKER: {self.consecutive_failures} consecutive failures. Stopping pipeline.")
            return True

        # Check failure rate after processing at least 100 companies
        if total_processed >= 100:
            failure_rate = total_failures / total_processed
            if failure_rate > self.max_failure_rate:
                logger.error(
                    f"❌ CIRCUIT BREAKER: Failure rate {failure_rate:.2%} exceeds {self.max_failure_rate:.2%}. Stopping pipeline."
                )
                return True

        return False

    async def fetch_companies_from_api(self, session):
        """Fetch all companies from DMA API with server-friendly rate limiting."""
        logger.info("🔍 Fetching companies from DMA API (server-friendly mode)...")

        all_companies = []
        page = 1
        api_failures = 0
        max_api_failures = 5  # Stop API fetching after 5 failures

        # Create progress bar for API fetching
        api_pbar = tqdm(desc="Fetching API pages", unit="page")

        while True:
            try:
                api_pbar.set_description(f"Fetching page {page}")

                # DMA API payload (using correct form data format from working scraper)
                payload = {
                    "page": str(page),
                    "timestamp": str(int(time.time() * 1000)),
                    "searched": "true",
                    "valgtSoegning": "prae_Alle husdyrbrug",
                    "soegningNavn": "",
                    "fritekst": "",
                    "adresse": "",
                    "regionsnr": "",
                    "postNr": "",
                    "afstandIKm": "",
                    "indenforKortudsnit": "false",
                    "kortudsnitTopHoejre": "POINT(915960 6408560)",
                    "kortudsnitBundVenstre": "POINT(404040 6051440)",
                    "virksomhedsIdentifikator": "",
                    "medtagInaktive": "false",
                    "kunRisikovirksomheder": "false",
                    "aktiviteter": [
                        "VL20000112",
                        "VL20000430",
                        "VL00000430",
                        "VL00001000",
                        "VL00000431",
                        "VL00000432",
                        "VL00000433",
                        "VL00001001",
                        "VL00000492",
                        "VL00000493",
                        "VL00000494",
                        "VL00000495",
                        "VL00000491",
                        "VL00000490",
                        "VL00000437",
                        "VL00000496",
                        "VL00000292",
                        "VL00000470",
                        "VL00000497",
                        "VL00000429",
                    ],
                    "kunGodkendelsespligtige": "",
                    "harMCPAnlaeg": "false",
                    "harLCPAnlaeg": "false",
                    "omfattetAfVOC": "false",
                    "myndighedNavn": "",
                    "harTilsyn": "false",
                    "harAfgoerelse": "false",
                    "harHaandhaevelse": "false",
                    "periodeFra": "",
                    "periodeTil": "",
                    "sortering": "NAVN",
                    "sortAscending": "true",
                    "visFritekst": "false",
                    "visMyndighed": "false",
                    "visVirksomhed": "true",
                    "visBeliggenhed": "true",
                    "visOffentliggoerelser": "false",
                    "empty": "false",
                }

                async with session.post("https://dma.mst.dk/soeg/page", data=payload) as response:
                    if response.status != 200:
                        api_failures += 1
                        logger.error(
                            f"API request failed with status {response.status} (failure {api_failures}/{max_api_failures})"
                        )

                        if api_failures >= max_api_failures:
                            logger.error(f"❌ Too many API failures ({api_failures}). Stopping API fetch.")
                            break

                        # Wait longer before retry
                        await asyncio.sleep(self.delay_seconds * 3)
                        continue

                    data = await response.json()
                    companies = data.get("resultater", [])

                    if not companies:
                        api_pbar.set_description(f"No more companies found on page {page}")
                        break

                    # Reset API failures on success
                    api_failures = 0

                    # Process companies to extract miljoeaktoerUrl and CVR (using correct structure)
                    for company in companies:
                        if "miljoeaktoerUrl" in company:
                            # Extract data like the original scraper
                            miljoeaktoer = company["miljoeaktoer"]
                            company_data = {
                                "miljoeaktoerUrl": company["miljoeaktoerUrl"],
                                "myndighedUrl": company["myndighedUrl"],
                                "navn": miljoeaktoer["navn"],
                                "cvr_number": miljoeaktoer["cvr"],
                                "chr": miljoeaktoer["chr"],
                                "pnr": miljoeaktoer["pnr"],
                                "mstNoegle": miljoeaktoer["mstNoegle"],
                                "fuldAdresse": company["fuldAdresse"],
                                "bynavn": miljoeaktoer["bynavn"],
                                "postNummer": miljoeaktoer["postNummer"],
                                "vejnavn": miljoeaktoer["vejnavn"],
                                "husNummer": miljoeaktoer["husNummer"],
                                "hovedaktivitetKode": miljoeaktoer["hovedaktivitetKode"],
                                "hovedaktivitetTekst": miljoeaktoer["hovedaktivitetTekst"],
                                "miljoeaktoerGruppeKode": miljoeaktoer["miljoeaktoerGruppeKode"],
                                "miljoeaktoerGruppeTekst": miljoeaktoer["miljoeaktoerGruppeTekst"],
                                "godkendelsespligtig": miljoeaktoer["godkendelsespligtig"],
                                "risikovirksomhed": miljoeaktoer["risikovirksomhed"],
                                "stedfaestelse": miljoeaktoer["stedfaestelse"],
                                "ansvarligMyndighed": miljoeaktoer["ansvarligeMyndigheder"][0]["navn"]
                                if miljoeaktoer["ansvarligeMyndigheder"]
                                else None,
                                "ansvarligMyndighedKnr": miljoeaktoer["ansvarligeMyndigheder"][0]["knr"]
                                if miljoeaktoer["ansvarligeMyndigheder"]
                                else None,
                            }
                            all_companies.append(company_data)

                            # Check if we've reached the max companies limit
                            if self.max_companies and len(all_companies) >= self.max_companies:
                                api_pbar.set_description(f"Reached max companies limit: {self.max_companies}")
                                break

                    api_pbar.set_postfix({"Total": len(all_companies), "Page": page, "Failures": api_failures})
                    api_pbar.update(1)

                    # Check if we've reached the max companies limit
                    if self.max_companies and len(all_companies) >= self.max_companies:
                        break

                    # Check if we've reached the end
                    if len(companies) < 500:
                        api_pbar.set_description(f"Reached end - page {page} had {len(companies)} companies")
                        break

                    page += 1

                    # Add server-friendly delay between API requests
                    await asyncio.sleep(self.delay_seconds)

            except Exception as e:
                api_failures += 1
                logger.error(f"Error fetching page {page}: {e} (failure {api_failures}/{max_api_failures})")

                if api_failures >= max_api_failures:
                    logger.error(f"❌ Too many API failures ({api_failures}). Stopping API fetch.")
                    break

                # Wait longer before retry
                await asyncio.sleep(self.delay_seconds * 3)

        api_pbar.close()

        if api_failures >= max_api_failures:
            logger.error(f"❌ API fetching failed after {api_failures} failures")
            return []

        logger.info(f"✅ Total companies fetched: {len(all_companies)}")
        return all_companies

    def save_progress(self, current_page, total_pages, processed_companies, total_companies):
        """Save current progress to file."""
        progress = {
            "timestamp": datetime.now().isoformat(),
            "current_page": current_page,
            "total_pages": total_pages,
            "processed_companies": processed_companies,
            "total_companies": total_companies,
            "completion_percentage": (processed_companies / total_companies * 100) if total_companies > 0 else 0,
        }

        with open(self.progress_file, "w") as f:
            json.dump(progress, f, indent=2)

    def mark_completion(self):
        """Mark the pipeline as completed."""
        completion_info = {
            "completed_at": datetime.now().isoformat(),
            "session_dir": str(self.session_dir),
            "total_companies_processed": len(list(self.session_dir.glob("*/company_data.json"))),
            "total_pdfs_downloaded": len(list(self.session_dir.glob("*/pdfs/*.pdf"))),
        }

        with open(self.completion_file, "w") as f:
            f.write("DMA Pipeline Completed Successfully!\n")
            f.write(f"Completed at: {completion_info['completed_at']}\n")
            f.write(f"Companies processed: {completion_info['total_companies_processed']}\n")
            f.write(f"PDFs downloaded: {completion_info['total_pdfs_downloaded']}\n")
            f.write(f"Output directory: {completion_info['session_dir']}\n")

        logger.info(f"🎉 Pipeline completed! {completion_info}")
        return completion_info

    def auto_shutdown_vm(self):
        """Automatically shutdown the VM after completion."""
        if not self.auto_shutdown_enabled:
            logger.info("🔌 Auto-shutdown disabled, VM will remain running")
            return

        logger.info("🔄 Initiating VM auto-shutdown...")

        try:
            # Copy data to GCS bucket first - using standard bronze/dma path structure
            logger.info("📤 Uploading data to GCS...")
            gcs_path = f"gs://landbrugsdata-raw-data/bronze/dma/{self.timestamp}/"
            subprocess.run(["gsutil", "-m", "cp", "-r", str(self.session_dir), gcs_path], check=True)
            logger.info(f"✅ Data uploaded to GCS successfully: {gcs_path}")

            # Shutdown the VM
            logger.info("🔌 Shutting down VM...")
            subprocess.run(["sudo", "shutdown", "-h", "+1"], check=True)
            logger.info("✅ VM shutdown initiated (1 minute delay)")

        except Exception as e:
            logger.error(f"❌ Error during shutdown process: {e}")
            logger.error("VM will NOT be shut down automatically")

    async def run_full_pipeline(self):
        """Run the complete DMA pipeline."""
        logger.info("🚀 Starting FULL DMA Pipeline (Server-Friendly Mode)...")

        try:
            async with aiohttp.ClientSession() as session:
                # Step 1: Fetch all companies
                companies = await self.fetch_companies_from_api(session)

                if not companies:
                    logger.error("❌ No companies found! Exiting...")
                    return

                # Step 2: Process each company with detailed scraping
                logger.info(f"🔄 Processing {len(companies)} companies with detailed scraping...")

                scraper = EnhancedDMACompanyDetailScraper(
                    data=companies,
                    output_base_dir=str(self.session_dir.parent),
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

                # Override session_dir and timestamp to use our fixed pipeline timestamp
                scraper.session_dir = self.session_dir
                scraper.timestamp = self.timestamp

                # Process all companies with max 3 concurrent requests
                await scraper.process_miljoeaktoer_for_company_file_path(max_concurrent=3)

                # Step 3: Mark completion
                completion_info = self.mark_completion()

                # Step 4: Auto-shutdown VM
                self.auto_shutdown_vm()

                return completion_info

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Enhanced DMA Detail Scraper")
    parser.add_argument("--full-pipeline", action="store_true", help="Run the full DMA pipeline")
    parser.add_argument("--max-companies", type=int, help="Maximum number of companies to process")
    parser.add_argument("--start-date", type=str, help="Start date for filtering (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for filtering (YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="/tmp/dma_data", help="Output directory")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds")
    parser.add_argument("--no-auto-shutdown", action="store_true", help="Disable automatic VM shutdown")

    args = parser.parse_args()

    if args.full_pipeline:
        logger.info("🚀 Running FULL DMA Pipeline...")

        # Parse dates if provided
        start_date = None
        end_date = None
        if args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
                logger.info(f"📅 Start date filter: {start_date.strftime('%Y-%m-%d')}")
            except ValueError:
                logger.error(f"❌ Invalid start date format: {args.start_date}. Use YYYY-MM-DD")
                exit(1)

        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
                logger.info(f"📅 End date filter: {end_date.strftime('%Y-%m-%d')}")
            except ValueError:
                logger.error(f"❌ Invalid end date format: {args.end_date}. Use YYYY-MM-DD")
                exit(1)

        # Create and configure the full pipeline
        pipeline = FullDMAPipeline(output_base_dir=args.output_dir, delay_seconds=args.delay)

        # Set max companies if specified
        if args.max_companies:
            pipeline.max_companies = args.max_companies
            logger.info(f"🔢 Maximum companies to process: {args.max_companies}")

        # Set date filters if specified
        if start_date or end_date:
            pipeline.start_date = start_date
            pipeline.end_date = end_date

        # Disable auto-shutdown if requested
        if args.no_auto_shutdown:
            pipeline.auto_shutdown_enabled = False
            logger.info("🔌 Auto-shutdown disabled")

        # Run the pipeline
        asyncio.run(pipeline.run_full_pipeline())

    else:
        logger.info("ℹ️  To run the full pipeline, use: python enhanced_dma_detail_scraper.py --full-pipeline")
        logger.info("ℹ️  Available options:")
        logger.info("   --max-companies N     : Process only N companies")
        logger.info("   --start-date YYYY-MM-DD : Filter by start date")
        logger.info("   --end-date YYYY-MM-DD   : Filter by end date")
        logger.info("   --output-dir PATH       : Set output directory")
        logger.info("   --delay SECONDS         : Set delay between requests")
        logger.info("   --no-auto-shutdown      : Disable VM auto-shutdown")
        logger.info("ℹ️  This will:")
        logger.info("   - Fetch companies from DMA API")
        logger.info("   - Download PDFs and company data")
        logger.info("   - Upload results to GCS")
        logger.info("   - Auto-shutdown the VM when complete (unless disabled)")
