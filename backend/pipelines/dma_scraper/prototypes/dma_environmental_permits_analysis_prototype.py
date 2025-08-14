#!/usr/bin/env python3
"""
🧪 PROTOTYPE: DMA Environmental Permits Analysis using Gemini 2.5 Flash

⚠️  STATUS: PROTOTYPE - NOT INTEGRATED INTO PRODUCTION PIPELINE
This is an experimental analysis tool for extracting structured data from DMA environmental
permit PDFs using Google's Gemini 2.5 Flash multimodal AI.

DESCRIPTION:
This script processes afgørelser (decisions/rulings) PDFs scraped by the DMA pipeline to:
- Identify environmental permits vs. other regulatory documents
- Extract structured data on energy consumption, animal production, emissions, etc.
- Group facilities intelligently using AI-powered address consolidation
- Apply temporal logic to handle permit renewals and updates

FEATURES:
✅ Cost-optimized: Only sends first 2 pages for permit classification
✅ Parallel processing: 5-10x faster document processing
✅ Smart address grouping: AI-powered facility consolidation
✅ Temporal aggregation: Handles permit renewals correctly
✅ Multi-folder discovery: Finds documents across different structures

REQUIREMENTS:
- PyPDF2 for cost-optimized PDF page extraction
- Google Cloud Application Default Credentials (ADC) configured
- Access to Vertex AI Gemini 2.5 Flash

USAGE:
This prototype is designed to work with data from the DMA scraper pipeline.
Modify the test_companies list in main() to analyze specific CVRs.

INTEGRATION STATUS:
❌ Not integrated into production pipeline - standalone prototype
❌ No automated scheduling or triggers
❌ Manual execution required

TODO FOR PRODUCTION:
- [ ] Integrate with DMA pipeline scheduling
- [ ] Add configuration management
- [ ] Implement batch processing for all companies
- [ ] Add data validation and quality checks
- [ ] Create output schema documentation
- [ ] Add monitoring and alerting
"""

import json
import logging
import os
import subprocess
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import vertexai
from dotenv import load_dotenv
from vertexai.generative_models import GenerativeModel, Part

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# PDF processing
try:
    import PyPDF2

    PDF_AVAILABLE = True
    logger.info("🔧 PyPDF2 available - will extract first 2 pages for cost optimization")
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("⚠️ PyPDF2 not available - will send full PDFs (higher costs). Install with: pip install PyPDF2")


@dataclass
class DocumentInfo:
    """Information about a DMA document"""

    cvr: str
    company_name: str
    document_type: str
    document_path: str
    file_size: Optional[int] = None


class DMAPermitAnalyzer:
    """Analyzer for DMA environmental permit PDFs using Gemini 2.5 Flash"""

    def __init__(self):
        """Initialize the analyzer with Vertex AI Gemini using ADC"""

        # Initialize Vertex AI with ADC
        project_id = self._get_project_id()
        vertexai.init(project=project_id, location="europe-west1")

        self.model = GenerativeModel("gemini-2.5-flash")

        logger.info(f"🤖 Initialized DMA Permit Analyzer with Vertex AI Gemini 2.5 Flash (Project: {project_id})")

    def _get_project_id(self) -> str:
        """Get the current project ID from gcloud config"""
        try:
            result = subprocess.run(
                ["gcloud", "config", "get-value", "project"], capture_output=True, text=True, check=True
            )
            project_id = result.stdout.strip()
            if not project_id:
                raise ValueError("No project ID found in gcloud config")
            return project_id
        except Exception as e:
            logger.error(f"Failed to get project ID: {e}")
            # Fallback to landbrugsdata-1 if that's your project
            return "landbrugsdata-1"

    def discover_all_afgoerelser_pdfs(self, cvr: str, base_gcs_path: str) -> List[str]:
        """
        Discover all afgørelser PDF files for a given CVR across multiple possible folder structures
        """
        all_afgoerelser_files = []

        try:
            logger.info(f"🔍 Discovering afgørelser PDFs for CVR {cvr} (checking multiple folder structures)")

            # Method 1: Try direct CVR folder
            direct_path = f"{base_gcs_path}/{cvr}/pdfs/"
            try:
                result = subprocess.run(["gsutil", "ls", direct_path], capture_output=True, text=True, check=True)
                if result.returncode == 0:
                    files = result.stdout.strip().split("\n")
                    direct_afgoerelser = [f for f in files if "afgoerelser_" in f and f.endswith(".pdf")]
                    all_afgoerelser_files.extend(direct_afgoerelser)
                    logger.info(f"📁 Direct path {direct_path}: {len(direct_afgoerelser)} files")
            except subprocess.CalledProcessError:
                logger.info(f"📁 Direct path {direct_path}: not found")

            # Method 2: Search for CVR in subdirectories (handle multiple folders with same CVR)
            try:
                # List all subdirectories in base path
                result = subprocess.run(
                    ["gsutil", "ls", f"{base_gcs_path}/"], capture_output=True, text=True, check=True
                )

                if result.returncode == 0:
                    subdirectories = [
                        line.strip() for line in result.stdout.strip().split("\n") if line.strip().endswith("/")
                    ]

                    # Check each subdirectory for CVR folders
                    for subdir in subdirectories:
                        if cvr in subdir:  # This subdirectory might contain our CVR
                            try:
                                # Look for pdfs folder in this subdirectory
                                potential_path = (
                                    f"{subdir}{cvr}/pdfs/" if not subdir.endswith(f"{cvr}/") else f"{subdir}pdfs/"
                                )

                                subdir_result = subprocess.run(
                                    ["gsutil", "ls", potential_path], capture_output=True, text=True, check=True
                                )

                                if subdir_result.returncode == 0:
                                    files = subdir_result.stdout.strip().split("\n")
                                    subdir_afgoerelser = [
                                        f for f in files if "afgoerelser_" in f and f.endswith(".pdf")
                                    ]
                                    # Avoid duplicates
                                    new_files = [f for f in subdir_afgoerelser if f not in all_afgoerelser_files]
                                    all_afgoerelser_files.extend(new_files)
                                    if new_files:
                                        logger.info(f"📁 Alternative path {potential_path}: {len(new_files)} new files")

                            except subprocess.CalledProcessError:
                                continue  # This subdirectory doesn't have the expected structure

            except subprocess.CalledProcessError:
                logger.warning(f"Could not list subdirectories in {base_gcs_path}")

            # Remove duplicates and sort
            all_afgoerelser_files = sorted(list(set(all_afgoerelser_files)))

            logger.info(f"📄 Total afgørelser PDFs found for CVR {cvr}: {len(all_afgoerelser_files)}")

            return all_afgoerelser_files

        except Exception as e:
            logger.error(f"Error discovering PDFs for CVR {cvr}: {e}")
            # Fallback to just the first file in direct path
            return [f"{base_gcs_path}/{cvr}/pdfs/afgoerelser_0.pdf"]

    def extract_first_pages_pdf(self, pdf_path: str, num_pages: int = 2) -> bytes:
        """
        Extract first N pages from PDF for cost-optimized analysis
        Returns the first pages as a new PDF in bytes
        """
        if not PDF_AVAILABLE:
            # Fallback: return full PDF if PyPDF2 not available
            logger.warning(f"📄 PyPDF2 not available - sending full PDF for {pdf_path.split('/')[-1]} (higher cost)")
            with open(pdf_path, "rb") as f:
                return f.read()

        try:
            logger.debug(f"📄 Extracting first {num_pages} pages from {pdf_path.split('/')[-1]} for cost optimization")

            with open(pdf_path, "rb") as file:
                reader = PyPDF2.PdfReader(file)
                writer = PyPDF2.PdfWriter()

                # Add first N pages (or all pages if fewer than N)
                pages_to_extract = min(num_pages, len(reader.pages))

                for i in range(pages_to_extract):
                    writer.add_page(reader.pages[i])

                # Write to bytes
                output_stream = tempfile.NamedTemporaryFile()
                writer.write(output_stream)
                output_stream.seek(0)

                first_pages_bytes = output_stream.read()
                output_stream.close()

                # Log savings
                original_size = os.path.getsize(pdf_path)
                new_size = len(first_pages_bytes)
                savings_pct = (1 - new_size / original_size) * 100

                logger.debug(
                    f"💰 Cost optimization: {pages_to_extract} pages ({new_size:,} bytes) "
                    f"vs full PDF ({original_size:,} bytes) - {savings_pct:.1f}% reduction"
                )

                return first_pages_bytes

        except Exception as e:
            logger.warning(f"⚠️ Error extracting pages from {pdf_path.split('/')[-1]}: {e}. Using full PDF.")
            # Fallback to full PDF if extraction fails
            with open(pdf_path, "rb") as f:
                return f.read()

    def download_pdf_temporarily(self, gcs_path: str) -> str:
        """Download a PDF from GCS to a temporary file"""
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            temp_path = temp_file.name
            temp_file.close()

            # Download using gsutil
            result = subprocess.run(["gsutil", "cp", gcs_path, temp_path], capture_output=True, text=True, check=True)

            if result.returncode == 0:
                logger.info(f"📥 Downloaded {gcs_path} to {temp_path}")
                return temp_path
            else:
                logger.error(f"Failed to download {gcs_path}: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")
            return None

    def analyze_first_pages_for_permit_check(self, pdf_path: str, doc_info: DocumentInfo) -> Dict:
        """
        Analyze first 2 pages to check if this is an environmental permit (COST OPTIMIZED)
        """
        try:
            logger.info(f"🔍 Checking first 2 pages for environmental permit: {doc_info.cvr}")

            # Extract only first 2 pages for cost optimization
            first_pages_data = self.extract_first_pages_pdf(pdf_path, num_pages=2)

            # Create PDF part for Vertex AI (only first 2 pages)
            pdf_part = Part.from_data(data=first_pages_data, mime_type="application/pdf")

            permit_check_prompt = """# ROLLE OG OPGAVE
Du er ekspert i danske miljømyndigheds dokumenter. Din opgave er at analysere de 
første 2 sider af dette PDF-dokument for at afgøre om det er en miljøtilladelse, 
miljøafgørelse eller relateret miljødokument.

# HVAD AT LEDE EFTER
Se efter følgende nøgleord og koncepter på de første sider:
- Miljøtilladelse, miljøafgørelse, miljøgodkendelse, afgørelse
- Accept af anmeldelse, husdyrgodkendelse, § 10
- Husdyr, kvæg, svin, fjerkræ, mink, fasaner
- Dyreenheder, DE, årssøer, staldpladser
- Ammoniak, NH3, emission
- Gødning, gylle, ajle
- Biogas, biogasanlæg
- Transport, kørselsaktivitet
- Energiforbrug, el, olie, gas, varme

# SVAR FORMAT
Returner kun JSON i følgende format:

VIGTIGT: Sæt "is_environmental_permit" til TRUE for BÅDE miljøtilladelser OG 
miljøafgørelser - begge indeholder værdifulde miljødata.

```json
{
  "is_environmental_permit": true/false,
  "confidence_score": 0.0-1.0,
  "document_type": "beskrivelse af dokumenttype",
  "key_indicators": ["liste", "af", "fund"],
  "contains_animal_production": true/false,
  "contains_energy_data": true/false,
  "contains_emission_data": true/false,
  "worth_full_analysis": true/false,
  "brief_summary": "kort beskrivelse af dokumentets indhold",
  "facility_address": "fuld adresse af anlægget/bedriften eller null",
  "document_date": "udstedelsesdato i format YYYY-MM-DD eller null",
  "permit_number": "tilladelsesnummer eller sagsnummer eller null",
  "valid_period": "gyldighedsperiode eller null"
}
```
"""

            response = self.model.generate_content([permit_check_prompt, pdf_part])

            # Parse response
            response_text = response.text.strip()

            # Clean JSON from markdown
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()

            analysis = json.loads(response_text)

            logger.info(f"✅ Permit check complete - Is permit: {analysis.get('is_environmental_permit', False)}")

            return {
                "status": "permit_check_complete",
                "document_info": {
                    "cvr": doc_info.cvr,
                    "company_name": doc_info.company_name,
                    "document_type": doc_info.document_type,
                    "document_path": doc_info.document_path,
                    "file_size": doc_info.file_size,
                },
                "analysis": analysis,
                "analyzed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in permit check: {e}")
            return {
                "status": "error",
                "error": str(e),
                "document_info": {
                    "cvr": doc_info.cvr,
                    "company_name": doc_info.company_name,
                    "document_type": doc_info.document_type,
                    "document_path": doc_info.document_path,
                    "file_size": doc_info.file_size,
                },
            }

    def analyze_full_document_for_data_extraction(self, pdf_path: str, doc_info: DocumentInfo) -> Dict:
        """
        Analyze full document to extract detailed environmental permit data
        """
        try:
            logger.info(f"📊 Extracting detailed data from environmental permit: {doc_info.cvr}")

            # Create PDF part for Vertex AI
            pdf_part = Part.from_data(data=open(pdf_path, "rb").read(), mime_type="application/pdf")

            extraction_prompt = """# ROLLE OG OPGAVE
Du er ekspert i danske miljøtilladelser og skal ekstraktere strukturerede data fra dette miljøtilladelsesdokument.

# DATA AT EKSTRAKTERE

## 1. ENERGI
- Elektricitet (kWh/år, MWh/år)
- Olie (liter/år, m³/år)  
- Gas (m³/år, GJ/år)
- Varme/fjernvarme (GJ/år, MWh/år)

## 2. HUSDYR (efter art og aldersgruppe)
- Kvæg: køer, kvier, kalve, tyre
- Svin: årssøer, smågrise, slagtesvin, orner
- Fjerkræ: høner, slagtekyllinger, andet fjerkræ
- Andre: får, geder, heste, mink, osv.
Angiv antal dyr og dyreenheder (DE)

## 3. AMMONIAKEMISSION
- NH3 emission i kg/år eller tons/år
- Emissionsfaktorer per dyretype

## 4. GØDNING TIL BIOGAS
- Aftalt aflevering af gylle/gødning til biogasanlæg
- Mængde i tons/år eller m³/år
- Navn på biogasanlæg hvis nævnt

## 5. TRANSPORT
- Forventet antal kørsler/år
- Transportaktivitet relateret til produktion

# SVAR FORMAT
Returner kun JSON i følgende præcise format:

```json
{
  "energy": {
    "electricity_kwh_per_year": nummer_eller_null,
    "oil_liters_per_year": nummer_eller_null,
    "gas_m3_per_year": nummer_eller_null,
    "heat_gj_per_year": nummer_eller_null,
    "energy_notes": "eventuelle noter om energiforbrug"
  },
  "animals": {
    "cattle": {
      "dairy_cows": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null},
      "young_cattle": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null},
      "calves": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null}
    },
    "pigs": {
      "sows": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null},
      "piglets": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null},
      "finisher_pigs": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null}
    },
    "poultry": {
      "laying_hens": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null},
      "broilers": {"count": nummer_eller_null, "animal_units_de": nummer_eller_null}
    },
    "other_animals": {},
    "total_animal_units_de": nummer_eller_null,
    "animal_notes": "eventuelle noter om dyr"
  },
  "ammonia_emission": {
    "nh3_kg_per_year": nummer_eller_null,
    "emission_factors": {},
    "ammonia_notes": "eventuelle noter om ammoniak"
  },
  "biogas_manure": {
    "delivery_tons_per_year": nummer_eller_null,
    "delivery_m3_per_year": nummer_eller_null,
    "biogas_plant_name": "navn_eller_null",
    "biogas_notes": "eventuelle noter om biogas"
  },
  "transport": {
    "expected_trips_per_year": nummer_eller_null,
    "transport_activity": "beskrivelse_eller_null",
    "transport_notes": "eventuelle noter om transport"
  },
  "permit_details": {
    "permit_number": "nummer_eller_null",
    "issue_date": "dato_eller_null",
    "valid_until": "dato_eller_null",
    "issuing_authority": "myndighed_eller_null"
  },
  "extraction_confidence": {
    "overall_confidence": 0.0_til_1.0,
    "energy_confidence": 0.0_til_1.0,
    "animals_confidence": 0.0_til_1.0,
    "ammonia_confidence": 0.0_til_1.0,
    "biogas_confidence": 0.0_til_1.0,
    "transport_confidence": 0.0_til_1.0
  }
}
```

# VIGTIGE NOTER
- Brug null hvis data ikke findes
- Vær præcis med enheder (kWh vs MWh, liter vs m³)
- Dyreenheder (DE) er vigtige for sammenligning
- Sæt confidence score lavt hvis usikker"""

            response = self.model.generate_content([extraction_prompt, pdf_part])

            # Parse response
            response_text = response.text.strip()

            # Clean JSON from markdown
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()

            extraction_data = json.loads(response_text)

            logger.info(f"✅ Full extraction complete for {doc_info.cvr}")

            return {
                "status": "extraction_complete",
                "document_info": {
                    "cvr": doc_info.cvr,
                    "company_name": doc_info.company_name,
                    "document_type": doc_info.document_type,
                    "document_path": doc_info.document_path,
                    "file_size": doc_info.file_size,
                },
                "extraction_data": extraction_data,
                "analyzed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in full extraction: {e}")
            return {
                "status": "error",
                "error": str(e),
                "document_info": {
                    "cvr": doc_info.cvr,
                    "company_name": doc_info.company_name,
                    "document_type": doc_info.document_type,
                    "document_path": doc_info.document_path,
                    "file_size": doc_info.file_size,
                },
            }

    def parallel_permit_checks(
        self, pdf_paths: List[str], doc_info_template: DocumentInfo, max_workers: int = 5
    ) -> List[Dict]:
        """
        Perform parallel permit checks on multiple PDFs
        """
        results = []

        def check_single_permit(pdf_path: str) -> Dict:
            """Check a single PDF for environmental permit"""
            doc_info = DocumentInfo(
                cvr=doc_info_template.cvr,
                company_name=doc_info_template.company_name,
                document_type=doc_info_template.document_type,
                document_path=pdf_path,
            )

            # Download PDF temporarily
            temp_pdf_path = self.download_pdf_temporarily(pdf_path)
            if not temp_pdf_path:
                return {"pdf_path": pdf_path, "status": "error", "error": "Failed to download PDF"}

            try:
                # Check if it's a permit (COST OPTIMIZED - only first 2 pages)
                permit_check = self.analyze_first_pages_for_permit_check(temp_pdf_path, doc_info)
                return {"pdf_path": pdf_path, "permit_check": permit_check}
            except Exception as e:
                logger.error(f"Error in parallel permit check for {pdf_path}: {e}")
                return {"pdf_path": pdf_path, "status": "error", "error": str(e)}
            finally:
                # Cleanup temporary file
                try:
                    os.unlink(temp_pdf_path)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temporary file: {e}")

        # Execute permit checks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pdf = {executor.submit(check_single_permit, pdf_path): pdf_path for pdf_path in pdf_paths}

            for future in as_completed(future_to_pdf):
                pdf_path = future_to_pdf[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"✅ Parallel permit check complete for {pdf_path.split('/')[-1]}")
                except Exception as e:
                    logger.error(f"❌ Parallel permit check failed for {pdf_path}: {e}")
                    results.append({"pdf_path": pdf_path, "status": "error", "error": str(e)})

        return results

    def smart_group_addresses(self, addresses: List[str]) -> Dict[str, List[str]]:
        """
        Use Gemini Flash to intelligently group similar addresses that represent the same facility
        """
        if len(addresses) <= 1:
            return {addr: [addr] for addr in addresses}

        try:
            logger.info(f"🏠 Using Gemini to intelligently group {len(addresses)} addresses...")

            address_grouping_prompt = f"""# OPGAVE
Du skal intelligent gruppere disse adresser, der stammer fra danske miljøtilladelser. Mange af adresserne refererer til samme fysiske lokalitet, men er skrevet med forskellige formater, detaljer eller stavemåder.

# ADRESSER AT GRUPPERE
{chr(10).join([f"{i + 1}. {addr}" for i, addr in enumerate(addresses)])}

# GRUPPERINGSREGLER
1. **Samme vej + postnummer** = samme facilitet (selvom husnumre varierer lidt)
2. **Forskellige skrivemåder** af samme sted = samme facilitet
3. **Ekstra detaljer** (landsby, kommune, ejendomsnavn) ændrer ikke grundadressen
4. **Husnummerområder** (f.eks. "38-40" vs "40A") kan være samme facilitet
5. **Manglende/ekstra informationer** skal ikke splitte relaterede adresser

# EKSEMPLER PÅ GRUPPERING
- "Grønkærvej 26, 7660 Bækmarksbro" OG "Grønkærvej 26, Flynder, 7660 Bækmarksbro" = SAMME GRUPPE
- "Bøvlingvej 38-40" OG "Bøvlingvej 40A" (samme postnummer) = MULIGVIS SAMME GRUPPE
- Forskellige veje eller postnumre = FORSKELLIGE GRUPPER

# SVAR FORMAT
Returner kun JSON med grupper, hvor hver gruppe har en "canonical_address" (den mest komplette/præcise) og "addresses" (alle varianter):

```json
{{
  "groups": [
    {{
      "canonical_address": "den bedste/mest komplette adresse for denne gruppe",
      "addresses": ["adresse1", "adresse2", "adresse3"],
      "confidence": 0.0_til_1.0,
      "reasoning": "kort forklaring på hvorfor disse hører sammen"
    }},
    {{
      "canonical_address": "anden gruppes bedste adresse",
      "addresses": ["adresse4", "adresse5"],
      "confidence": 0.0_til_1.0,
      "reasoning": "kort forklaring"
    }}
  ]
}}
```

Vær konservativ - det er bedre at have for mange grupper end at fejlagtigt samle forskellige faciliteter."""

            response = self.model.generate_content([address_grouping_prompt])
            response_text = response.text.strip()

            # Clean JSON from markdown
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()

            grouping_result = json.loads(response_text)

            # Convert to our expected format
            address_groups = {}
            for group in grouping_result.get("groups", []):
                canonical = group.get("canonical_address", "")
                group_addresses = group.get("addresses", [])
                confidence = group.get("confidence", 0.5)
                reasoning = group.get("reasoning", "")

                logger.info(
                    f"📍 Address group: '{canonical}' ({len(group_addresses)} variants, confidence: {confidence:.2f})"
                )
                logger.debug(f"    Reasoning: {reasoning}")
                logger.debug(f"    Variants: {group_addresses}")

                address_groups[canonical] = group_addresses

            return address_groups

        except Exception as e:
            logger.error(f"Error in smart address grouping: {e}")
            # Fallback to individual grouping
            return {addr: [addr] for addr in addresses}

    def group_documents_by_facility(self, permit_results: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group documents by facility address with SMART ADDRESS GROUPING (extracted during permit check) and sort by date (newest first)
        """
        # Step 1: Extract all valid addresses and build document mapping
        valid_documents = []
        all_addresses = []
        unknown_address_counter = 1

        for result in permit_results:
            permit_check = result.get("permit_check", {})
            if permit_check.get("status") != "permit_check_complete":
                logger.warning(f"Skipping document due to failed permit check: {result.get('pdf_path', 'unknown')}")
                continue

            analysis = permit_check.get("analysis", {})
            if not analysis.get("is_environmental_permit", False):
                logger.info(f"Skipping non-environmental permit: {result.get('pdf_path', 'unknown')}")
                continue

            # Extract facility information (address was fetched during first permit check call)
            facility_address = analysis.get("facility_address", "").strip()
            document_date = analysis.get("document_date")
            permit_number = analysis.get("permit_number", "")

            # Handle missing/empty addresses
            if not facility_address or facility_address in ["null", "None", ""]:
                # Create unique grouping for documents without addresses
                pdf_name = result.get("pdf_path", "").split("/")[-1]
                facility_address = f"Unknown Address #{unknown_address_counter} ({pdf_name})"
                unknown_address_counter += 1
                logger.warning(f"No facility address found in {pdf_name}, using fallback: {facility_address}")

            # Add metadata for better grouping
            result["parsed_date"] = self._parse_date(document_date)
            result["permit_number"] = permit_number
            result["original_facility_address"] = facility_address  # Store original address

            valid_documents.append(result)
            if facility_address not in all_addresses:
                all_addresses.append(facility_address)

        # Step 2: Use Gemini to intelligently group similar addresses
        logger.info(f"🏠 Applying smart address grouping to {len(all_addresses)} unique addresses...")
        address_groups = self.smart_group_addresses(all_addresses)

        # Step 3: Create mapping from original address to canonical address
        address_to_canonical = {}
        for canonical_address, address_variants in address_groups.items():
            for variant in address_variants:
                address_to_canonical[variant] = canonical_address

        # Step 4: Group documents by canonical addresses
        facility_groups = defaultdict(list)

        for result in valid_documents:
            original_address = result["original_facility_address"]
            canonical_address = address_to_canonical.get(original_address, original_address)

            # Store the canonical address for later use
            result["canonical_facility_address"] = canonical_address

            facility_groups[canonical_address].append(result)
            logger.debug(
                f"Grouped document {result.get('pdf_path', '').split('/')[-1]} under canonical facility: {canonical_address}"
            )

        # Step 5: Sort each facility group by date (newest first) and log results
        for canonical_address, documents in facility_groups.items():
            documents.sort(key=lambda x: x.get("parsed_date", datetime.min), reverse=True)

            # Show original address variants for this group
            original_variants = list(set([doc["original_facility_address"] for doc in documents]))
            variant_info = f" (grouped from: {', '.join(original_variants)})" if len(original_variants) > 1 else ""

            newest_date = documents[0].get("parsed_date", datetime.min)
            date_str = newest_date.strftime("%Y-%m-%d") if newest_date != datetime.min else "unknown date"

            logger.info(
                f"🏭 Canonical facility '{canonical_address}': {len(documents)} documents (newest: {date_str}){variant_info}"
            )

        return dict(facility_groups)

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object"""
        if not date_str or date_str == "null":
            return datetime.min

        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%d-%m-%Y")
            except ValueError:
                return datetime.min

    def analyze_facility_documents(
        self, facility_address: str, documents: List[Dict], doc_info_template: DocumentInfo
    ) -> Dict:
        """
        Analyze all documents for a single facility with proper temporal logic
        """
        logger.info(f"🏭 Analyzing {len(documents)} documents for facility: {facility_address}")

        # Download all PDFs for this facility
        pdf_contents = []
        document_metadata = []

        for doc in documents:
            pdf_path = doc["pdf_path"]
            temp_pdf_path = self.download_pdf_temporarily(pdf_path)
            if temp_pdf_path:
                try:
                    with open(temp_pdf_path, "rb") as f:
                        pdf_contents.append(f.read())

                    # Extract metadata
                    permit_check = doc.get("permit_check", {})
                    analysis = permit_check.get("analysis", {})
                    document_metadata.append(
                        {
                            "file_name": pdf_path.split("/")[-1],
                            "document_date": analysis.get("document_date"),
                            "permit_number": analysis.get("permit_number"),
                            "document_type": analysis.get("document_type"),
                            "brief_summary": analysis.get("brief_summary"),
                        }
                    )

                    os.unlink(temp_pdf_path)
                except Exception as e:
                    logger.error(f"Error processing {pdf_path}: {e}")

        if not pdf_contents:
            return {
                "status": "error",
                "error": "No valid PDFs found for facility",
                "facility_address": facility_address,
            }

        # Create aggregate analysis prompt
        aggregate_prompt = f"""# ROLLE OG OPGAVE
Du er ekspert i danske miljøtilladelser og skal analysere ALLE dokumenter for en specifik bedrift/facilitet for at give et samlet overblik.

# FACILITET
Adresse: {facility_address}
Antal dokumenter: {len(documents)}

# DOKUMENTER (sorteret efter dato, nyeste først)
"""

        for i, metadata in enumerate(document_metadata):
            aggregate_prompt += f"""
Dokument {i + 1}: {metadata["file_name"]}
- Dato: {metadata["document_date"]}
- Type: {metadata["document_type"]}
- Tilladelsesnr: {metadata["permit_number"]}
- Beskrivelse: {metadata["brief_summary"]}
"""

        aggregate_prompt += """
# VIGTIGE REGLER FOR AGGREGERING
1. **NYESTE TILLADELSE VINDER**: Hvis flere dokumenter dækker samme aktivitet, brug data fra det nyeste dokument
2. **KUMULATIVE DATA**: Hvis dokumenter dækker forskellige aktiviteter/områder på samme facilitet, addér tallene
3. **ADRESSE KONTROL**: Bekræft at alle dokumenter vedrører samme fysiske facilitet
4. **TIDSMÆSSIG LOGIK**: Ældre tilladelser kan være erstattet af nyere - vær opmærksom på dette

# DATA AT EKSTRAKTERE (SAMLET FOR HELE FACILITETEN)
- Energi (elektricitet, olie, gas, varme) - fra nyeste relevante tilladelse
- Husdyr (arter og antal) - samlet kapacitet eller fra nyeste tilladelse hvis erstatning
- Ammoniakemission (NH3) - samlet eller fra nyeste beregning
- Gødning til biogas - samlet aflevering
- Transport - samlet aktivitet

# SVAR FORMAT
Returner kun JSON i følgende præcise format:

```json
{{
  "facility_address": "{facility_address}",
  "documents_analyzed": {len(documents)},
  "temporal_logic_applied": "beskrivelse af hvordan tidsmæssig prioritering blev håndteret",
  "energy": {{
    "electricity_kwh_per_year": nummer_eller_null,
    "oil_liters_per_year": nummer_eller_null,
    "gas_m3_per_year": nummer_eller_null,
    "heat_gj_per_year": nummer_eller_null,
    "energy_notes": "noter om energiforbrug og hvilke dokumenter det kommer fra"
  }},
  "animals": {{
    "cattle": {{
      "dairy_cows": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}},
      "young_cattle": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}},
      "calves": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}}
    }},
    "pigs": {{
      "sows": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}},
      "piglets": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}},
      "finisher_pigs": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}}
    }},
    "poultry": {{
      "laying_hens": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}},
      "broilers": {{"count": nummer_eller_null, "animal_units_de": nummer_eller_null}}
    }},
    "other_animals": {{}},
    "total_animal_units_de": nummer_eller_null,
    "animal_notes": "noter om dyr og hvilke dokumenter det kommer fra"
  }},
  "ammonia_emission": {{
    "nh3_kg_per_year": nummer_eller_null,
    "emission_factors": {{}},
    "ammonia_notes": "noter om ammoniak og hvilke dokumenter det kommer fra"
  }},
  "biogas_manure": {{
    "delivery_tons_per_year": nummer_eller_null,
    "delivery_m3_per_year": nummer_eller_null,
    "biogas_plant_name": "navn_eller_null",
    "biogas_notes": "noter om biogas og hvilke dokumenter det kommer fra"
  }},
  "transport": {{
    "expected_trips_per_year": nummer_eller_null,
    "transport_activity": "beskrivelse_eller_null",
    "transport_notes": "noter om transport og hvilke dokumenter det kommer fra"
  }},
  "permit_details": {{
    "primary_permit_number": "hovedtilladelsesnummer",
    "primary_issue_date": "dato for hovedtilladelse",
    "valid_until": "gyldighed",
    "issuing_authority": "myndighed",
    "all_permit_numbers": ["liste", "af", "alle", "tilladelsesnumre"]
  }},
  "extraction_confidence": {{
    "overall_confidence": 0.0_til_1.0,
    "energy_confidence": 0.0_til_1.0,
    "animals_confidence": 0.0_til_1.0,
    "ammonia_confidence": 0.0_til_1.0,
    "biogas_confidence": 0.0_til_1.0,
    "transport_confidence": 0.0_til_1.0
  }}
}}
```"""

        try:
            # Send all PDFs to Gemini at once
            pdf_parts = []
            for i, pdf_content in enumerate(pdf_contents):
                pdf_part = Part.from_data(data=pdf_content, mime_type="application/pdf")
                pdf_parts.append(pdf_part)

            # Combine prompt and all PDFs
            content_parts = [aggregate_prompt] + pdf_parts

            response = self.model.generate_content(content_parts)

            # Parse response
            response_text = response.text.strip()

            # Clean JSON from markdown
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()

            extraction_data = json.loads(response_text)

            logger.info(f"✅ Facility analysis complete for {facility_address}")

            return {
                "status": "facility_analysis_complete",
                "facility_address": facility_address,
                "documents_processed": len(documents),
                "extraction_data": extraction_data,
                "analyzed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in facility analysis for {facility_address}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "facility_address": facility_address,
                "documents_processed": len(documents),
            }

    def analyze_dma_permits_for_company(self, cvr: str, company_name: str, base_gcs_path: str) -> Dict:
        """
        Analyze ALL afgørelser PDFs for a company with parallel processing and facility grouping
        """
        logger.info(f"🚀 Starting enhanced DMA permit analysis for {company_name} (CVR: {cvr})")

        # Discover all afgørelser PDFs for this CVR
        pdf_paths = self.discover_all_afgoerelser_pdfs(cvr, base_gcs_path)

        if not pdf_paths:
            return {
                "cvr": cvr,
                "company_name": company_name,
                "total_documents": 0,
                "facilities": {},
                "summary": "No afgørelser documents found",
            }

        # Create document info template
        doc_info_template = DocumentInfo(
            cvr=cvr, company_name=company_name, document_type="afgoerelser", document_path=""
        )

        # Step 1: Parallel permit checks (5-10x faster!) - extracts addresses early
        logger.info(
            f"⚡ Running parallel permit checks on {len(pdf_paths)} documents (extracting addresses for grouping)..."
        )
        permit_results = self.parallel_permit_checks(pdf_paths, doc_info_template, max_workers=5)

        # Log permit check results
        valid_permits = sum(
            1
            for r in permit_results
            if r.get("permit_check", {}).get("analysis", {}).get("is_environmental_permit", False)
        )
        logger.info(f"✅ Permit checks complete: {valid_permits}/{len(permit_results)} confirmed environmental permits")

        # Step 2: Group documents by facility address with SMART ADDRESS GROUPING (using addresses extracted during permit checks)
        logger.info(f"🏭 Intelligently grouping {valid_permits} environmental permits by facility address...")
        facility_groups = self.group_documents_by_facility(permit_results)

        if not facility_groups:
            return {
                "cvr": cvr,
                "company_name": company_name,
                "total_documents": len(pdf_paths),
                "facilities": {},
                "summary": "No environmental permits found in documents",
            }

        # Step 3: Analyze each facility with aggregate document analysis
        facility_results = {}
        total_permits = sum(len(docs) for docs in facility_groups.values())

        logger.info(f"🎯 Found {len(facility_groups)} facilities with {total_permits} environmental permits")

        for facility_address, documents in facility_groups.items():
            logger.info(f"🏭 Processing facility: {facility_address} ({len(documents)} documents)")

            # Aggregate analysis for this facility
            facility_result = self.analyze_facility_documents(facility_address, documents, doc_info_template)
            facility_results[facility_address] = facility_result

            # Log key findings
            if facility_result.get("status") == "facility_analysis_complete":
                extraction_data = facility_result.get("extraction_data", {})
                confidence = extraction_data.get("extraction_confidence", {}).get("overall_confidence", 0)
                nh3_emission = extraction_data.get("ammonia_emission", {}).get("nh3_kg_per_year")

                logger.info(f"    ✅ Facility analysis complete (confidence: {confidence:.2f})")
                if nh3_emission:
                    logger.info(f"    📊 NH3 emission: {nh3_emission} kg/år")

        # Compile company summary
        company_results = {
            "cvr": cvr,
            "company_name": company_name,
            "total_documents": len(pdf_paths),
            "total_facilities": len(facility_groups),
            "total_environmental_permits": total_permits,
            "facilities": facility_results,
            "analysis_method": "parallel_permit_checks_with_facility_grouping",
            "analyzed_at": datetime.now().isoformat(),
        }

        logger.info(f"✅ Company analysis complete: {len(facility_groups)} facilities, {total_permits} permits")

        return company_results


def main():
    """🧪 PROTOTYPE: Main analysis function - test with fertilizer producers"""
    logger.info("🧪 DMA Environmental Permit Analysis - PROTOTYPE")
    logger.info("⚠️  This is a prototype script - not integrated into production pipeline")
    logger.info("=" * 70)

    # Test with first 2 companies to validate enhanced approach
    test_companies = [
        {"cvr": "39675706", "company_name": "Rosenfeldt v/Nicolai Oxholm Tillisch", "rank": 2},
        {"cvr": "43377531", "company_name": "Smedsgaard Agro ApS", "rank": 6},
    ]

    analyzer = DMAPermitAnalyzer()
    results = []

    base_gcs_path = "gs://landbrugsdata-raw-data/bronze/dma/20250705_054247/20250705_054247"

    for company in test_companies:
        cvr = company["cvr"]
        name = company["company_name"]
        rank = company["rank"]

        logger.info(f"\n📄 Analyzing Rank #{rank}: {name} (CVR: {cvr})")

        try:
            result = analyzer.analyze_dma_permits_for_company(cvr, name, base_gcs_path)
            results.append(result)

            # Log summary of findings with new facility-based structure
            total_docs = result.get("total_documents", 0)
            total_facilities = result.get("total_facilities", 0)
            total_permits = result.get("total_environmental_permits", 0)

            # Log facility-level findings
            for facility_address, facility_result in result.get("facilities", {}).items():
                if facility_result.get("status") == "facility_analysis_complete":
                    extraction_data = facility_result.get("extraction_data", {})
                    confidence = extraction_data.get("extraction_confidence", {}).get("overall_confidence", 0)
                    nh3_emission = extraction_data.get("ammonia_emission", {}).get("nh3_kg_per_year")

                    if nh3_emission:
                        logger.info(
                            f"    🏭 {facility_address}: NH3 emission: {nh3_emission} kg/år (confidence: {confidence:.2f})"
                        )

            logger.info(
                f"✅ Company summary: {total_facilities} facilities, {total_permits}/{total_docs} environmental permits found"
            )

        except Exception as e:
            logger.error(f"Error analyzing {name}: {e}")
            results.append({"cvr": cvr, "company_name": name, "total_documents": 0, "error": str(e), "documents": []})

    # Save results (prototype output directory)
    output_dir = Path("prototypes_output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "dma_environmental_permits_analysis.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logger.info(f"\n💾 Analysis results saved to: {output_file}")
    logger.info("✅ DMA Environmental Permit Analysis Complete!")

    # Summary with new facility-based structure
    total_companies = len(results)
    total_documents = sum(r.get("total_documents", 0) for r in results)
    total_facilities = sum(r.get("total_facilities", 0) for r in results)
    total_permits = sum(r.get("total_environmental_permits", 0) for r in results)
    successful_facilities = 0

    # Count successful facility analyses
    for result in results:
        for facility_result in result.get("facilities", {}).values():
            if facility_result.get("status") == "facility_analysis_complete":
                successful_facilities += 1

    logger.info("\n📊 ENHANCED ANALYSIS SUMMARY:")
    logger.info(f"    Companies analyzed: {total_companies}")
    logger.info(f"    Total documents processed: {total_documents}")
    logger.info(f"    Total facilities identified: {total_facilities}")
    logger.info(f"    Environmental permits found: {total_permits}")
    logger.info(f"    Successful facility analyses: {successful_facilities}")
    if total_documents > 0:
        logger.info(f"    Permit success rate: {total_permits / total_documents * 100:.1f}%")
    if total_facilities > 0:
        logger.info(f"    Facility analysis success rate: {successful_facilities / total_facilities * 100:.1f}%")


if __name__ == "__main__":
    main()
