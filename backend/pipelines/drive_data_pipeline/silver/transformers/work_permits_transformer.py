"""
Work Permits transformer for processing agricultural work permits from PDF documents.

This transformer specializes in extracting agricultural work permit statistics
from Danish "Landbrugsvisum" (Agricultural Visa) PDFs which contain pivot tables
with company CVR numbers, nationalities, years, and permit counts.
"""

import re
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ...utils.logging import get_logger
from ...utils.storage import DriveStorageManager
from ..transformers.base import BaseTransformer, TransformResult

logger = get_logger()


class WorkPermitsTransformer(BaseTransformer):
    """Transformer for extracting agricultural work permits data from PDFs."""

    def __init__(self) -> None:
        """Initialize the work permits transformer."""
        super().__init__()
        self.name = "WorkPermitsTransformer"
        logger.info("Initialized WorkPermitsTransformer for visa/work permits processing")

        # Known Danish country names from the permit documents (EXACT from working script)
        self.country_names = [
            "Ukraine",
            "Indien",
            "Brasilien",
            "Vietnam",
            "Uganda",
            "Bangladesh",
            "Filippinerne",
            "Tanzania",
            "Thailand",
            "Kirgizstan",
            "Usbekistan",
            "Hviderusland",
            "Sydafrika",
            "Kina",
            "Ghana",
            "Marokko",
            "Polen",
            "Rumænien",
            "Tyrkiet",
            "Etiopien",
            "Kenya",
            "Pakistan",
            "Albanien",
            "Nordmakedonien",
        ]

        # Expected years in the pivot table
        self.expected_years = ["2019", "2020", "2021", "2022", "2023"]

    def can_handle(self, file_path: Path, file_metadata: dict[str, Any]) -> bool:
        """
        Check if this transformer can handle the given file.

        Args:
            file_path: Path to the file
            file_metadata: Metadata about the file

        Returns:
            True if this transformer can handle the file
        """
        logger.info(f"WorkPermitsTransformer.can_handle() checking file: {file_path}")

        # Check if it's a PDF file that might contain work permits data
        if not file_path.suffix.lower() == ".pdf":
            logger.info(f"WorkPermitsTransformer: Not a PDF file, skipping: {file_path.suffix}")
            return False

        # Check filename patterns that suggest work permits data
        filename_lower = file_path.name.lower()
        work_permit_indicators = [
            "landbrugsvisum",
            "work_permit",
            "arbejdstilladelse",
            "visa",
            "permit",
            "statistik",
        ]

        matches = [indicator for indicator in work_permit_indicators if indicator in filename_lower]
        can_handle_result = any(indicator in filename_lower for indicator in work_permit_indicators)

        logger.info(
            f"WorkPermitsTransformer: filename='{filename_lower}', "
            f"matches={matches}, can_handle={can_handle_result}"
        )

        return can_handle_result

    def transform(self, file_path: Path, metadata: Any, output_dir: Path) -> TransformResult:
        """
        Transform work permits PDF into structured data.

        Args:
            file_path: Path to the PDF file
            metadata: File metadata
            output_dir: Directory to save processed output

        Returns:
            TransformResult object
        """
        # Import TransformResult here to avoid circular imports
        from .base import TransformResult

        if not pdfplumber:
            logger.error("pdfplumber not available - cannot process PDF")
            return TransformResult(success=False, error="pdfplumber not available")

        logger.info(f"Processing work permits PDF: {file_path}")

        try:
            # Extract raw text from PDF
            full_text = self._extract_text_from_pdf(file_path)
            logger.info(f"Extracted {len(full_text)} characters from PDF")

            # Debug: Show first 500 characters of extracted text
            logger.debug(f"First 500 chars: {full_text[:500]}")

            # Parse work permits data from text
            records = self._parse_work_permits_data(full_text)
            logger.info(f"Found {len(records)} raw records from parsing")

            # Clean and validate the data
            cleaned_records = self._clean_work_permits_data(records)

            logger.info(f"Extracted {len(cleaned_records)} work permit records")

            if not cleaned_records:
                return TransformResult(success=False, error="No work permit data extracted")

            # Convert to DataFrame and save as Parquet
            df = pd.DataFrame(cleaned_records)

            # Create output filename
            output_filename = f"work_permits_{file_path.stem}.parquet"
            output_path = output_dir / output_filename

            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)

            # Save as Parquet file
            df.to_parquet(output_path, index=False)
            logger.info(f"Saved work permits data to: {output_path}")

            return TransformResult(
                success=True,
                output_path=output_path,
                row_count=len(cleaned_records),
                schema=self.get_output_schema(),
                metadata={
                    "transformer": "WorkPermitsTransformer",
                    "companies_count": df["company_id"].nunique(),
                    "nationalities_count": df["nationality"].nunique(),
                    "years_covered": sorted(df["year"].unique().tolist()),
                    "total_permits": df["first_permits_count"].sum(),
                },
            )

        except Exception as e:
            logger.error(f"Error processing work permits PDF {file_path}: {e}")
            return TransformResult(success=False, error=str(e))

    def _extract_text_from_pdf(
        self, pdf_path: Path, storage_manager: DriveStorageManager | None = None
    ) -> str:
        """Extract raw text from PDF using pdfplumber."""
        logger.debug("Extracting text from PDF...")

        # Handle both absolute and relative paths
        if pdf_path.is_absolute():
            # Use absolute path directly
            actual_path = pdf_path
        else:
            # For relative paths, try to construct absolute path
            # This assumes we're running from the project root
            from pathlib import Path

            project_root = Path.cwd()
            actual_path = project_root / "data_local" / pdf_path

        full_text = ""
        with pdfplumber.open(actual_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    full_text += f"--- PAGE {page_num} ---\n"
                    full_text += page_text + "\n"

        logger.debug(f"Extracted {len(full_text)} characters from PDF")
        return full_text

    def _parse_work_permits_data(self, text: str) -> list[dict[str, Any]]:
        """
        Parse work permits data from extracted text.

        The data is structured as a pivot table with:
        - CVR numbers as row headers
        - Nationalities as sub-rows
        - Years (2019-2023) as columns
        - Permit counts in cells
        - Special "Ukendt CVR" (Unknown CVR) section
        """
        logger.debug("Parsing work permits data from text...")

        lines = text.split("\n")
        visa_records = []

        # State tracking across pages
        current_cvr = None
        current_page = None

        cvr_count = 0
        country_matches = 0

        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            # Track page numbers
            page_match = re.search(r"--- PAGE (\d+) ---", line)
            if page_match:
                current_page = int(page_match.group(1))
                logger.debug(f"Processing page {current_page}")
                continue

            # Look for CVR numbers (8 digits) or "Ukendt CVR"
            if "ukendt cvr" in line.lower():
                current_cvr = "UNKNOWN"
                cvr_count += 1
                logger.debug("Found Unknown CVR section")
                continue

            cvr_match = re.search(r"\b(\d{8})\b", line)
            if cvr_match:
                potential_cvr = cvr_match.group(1)
                # Verify it's actually a CVR (not a permit count or other number)
                if line.find(potential_cvr) < 50:  # CVR should be near start of line
                    current_cvr = potential_cvr
                    cvr_count += 1
                    if cvr_count <= 5:  # Log first few for debugging
                        logger.debug(f"Found CVR: {current_cvr}")
                    continue

            # Skip if we don't have a current CVR
            if not current_cvr:
                continue

            # Look for table headers with years
            if any(year in line for year in self.expected_years):
                if "CVR-nummer" in line or "Nationalitet" in line:
                    logger.debug(f"Found table header on page {current_page}: {line}")
                    continue

            # Look for nationality rows with data
            for country in self.country_names:
                if country in line:
                    # Extract the part of the line after the country name
                    country_pos = line.find(country)
                    data_part = line[country_pos + len(country) :].strip()

                    # Extract numbers that could be permit counts
                    numbers = re.findall(r"\b(\d+)\b", data_part)
                    country_matches += 1

                    if numbers:
                        logger.debug(f"Found {country} in CVR {current_cvr}: numbers {numbers}")

                        # Filter out obvious totals (usually the largest number or at the end)
                        permit_counts = []
                        for num_str in numbers:
                            num = int(num_str)
                            # Reasonable permit count range (not CVR numbers, not huge totals)
                            if 1 <= num <= 1000:
                                permit_counts.append(num)

                        # Try to map numbers to years based on position
                        # This is tricky without exact column positions, so we'll be conservative
                        if permit_counts:
                            # For now, let's create records for the actual numbers we find
                            # We'll need to infer years or use a more sophisticated approach
                            for i, count in enumerate(
                                permit_counts[:-1]
                            ):  # Skip the last (likely total)
                                if i < len(self.expected_years):
                                    year = int(self.expected_years[i])
                                    visa_records.append(
                                        {
                                            "company_id": current_cvr,
                                            "nationality": country,
                                            "year": year,
                                            "first_permits_count": count,
                                            "source_line": line,
                                            "line_number": line_num,
                                            "page_number": current_page,
                                            "source_file": "work_permits_pdf",
                                            "extracted_at": pd.Timestamp.now(),
                                        }
                                    )
                                    logger.debug(
                                        f"Added record: {current_cvr} - {country} - {year}: "
                                        f"{count} permits"
                                    )

                    break  # Found country match, don't check other countries for this line

        logger.info(f"Extracted {len(visa_records)} permit records from pivot table")
        logger.info(
            f"Found {cvr_count} CVR entries and {country_matches} country matches during parsing"
        )
        return visa_records

    def _clean_work_permits_data(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Clean and validate the work permits data."""
        if not records:
            return []

        logger.debug("Cleaning work permits data...")

        # Convert to DataFrame for easier processing
        df = pd.DataFrame(records)

        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=["company_id", "nationality", "year", "first_permits_count"])
        logger.debug(f"Removed {initial_count - len(df)} duplicates")

        # Group by company, nationality, year and sum permits (in case of multiple entries)
        df_clean = (
            df.groupby(["company_id", "nationality", "year"])
            .agg({"first_permits_count": "sum", "source_file": "first", "extracted_at": "first"})
            .reset_index()
        )

        # Add timestamps for database compatibility
        df_clean["created_at"] = pd.Timestamp.now()
        df_clean["updated_at"] = pd.Timestamp.now()

        # Convert back to records
        cleaned_records = df_clean.to_dict("records")

        logger.info(
            f"Cleaned data: {len(cleaned_records)} unique company-nationality-year combinations"
        )
        return cleaned_records

    def get_output_schema(self) -> dict[str, str]:
        """Get the expected output schema for work permits data."""
        return {
            "company_id": "VARCHAR",
            "year": "INTEGER",
            "nationality": "VARCHAR",
            "first_permits_count": "INTEGER",
            "source_file": "VARCHAR",
            "extracted_at": "TIMESTAMP",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        }
