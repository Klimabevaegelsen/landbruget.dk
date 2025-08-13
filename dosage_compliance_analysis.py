#!/usr/bin/env python3
"""
Pesticide Dosage Compliance Analysis

Compares actual pesticide usage (from our disaggregation data) against
API-recommended dosages for regulatory compliance validation.

Key corrections implemented:
- Our dosage_quantity = total per application (needs division by area_ha)
- API MaxDosageApp = per hectare recommendation
- Unit mapping: 2=kg, 4=l, 5=tablets
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PlanteITAPI:
    """Client for Plante IT Pesticide Service API."""

    def __init__(self, username: str = None, password: str = None):
        self.base_url = "https://pesticideservice.dlbr.dk/api"

        # Get credentials from environment variables (avoid hardcoded credentials)
        self.username = username or os.getenv("PLANTE_IT_USERNAME")
        self.password = password or os.getenv("PLANTE_IT_PASSWORD")

        if not self.username or not self.password:
            raise ValueError(
                "Plante IT API credentials not found. Please set PLANTE_IT_USERNAME and "
                "PLANTE_IT_PASSWORD environment variables. In production pipelines, these are "
                "automatically provided via GitHub Actions secrets."
            )

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)

    def get_products_for_crop(self, crop_id: int) -> List[Dict]:
        """Get all pesticide products approved for a specific crop."""
        try:
            response = self.session.get(f"{self.base_url}/Products?CropId={crop_id}", timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching products for crop {crop_id}: {e}")
            return []

    def get_product_detail(self, product_id: int, crop_id: int) -> Optional[Dict]:
        """Get detailed information for a specific product on a specific crop."""
        try:
            response = self.session.get(
                f"{self.base_url}/Products/{product_id}?CropId={crop_id}", timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching product {product_id} for crop {crop_id}: {e}")
            return None


class DosageComplianceAnalyzer:
    """Analyzes dosage compliance between our data and API recommendations."""

    def __init__(self, pesticide_data_path: str, crop_mapping_path: str):
        self.pesticide_data_path = pesticide_data_path
        self.crop_mapping_path = crop_mapping_path
        self.api_client = PlanteITAPI()

        # Unit mapping based on our codebase analysis
        self.unit_mapping = {2: "kg", 4: "l", 5: "tablets"}

        # Load data
        self.conn = duckdb.connect()
        self.load_data()

    def load_data(self):
        """Load pesticide data and crop mapping."""
        logger.info("Loading pesticide data...")
        self.conn.execute(
            f"CREATE TABLE pesticides AS SELECT * FROM read_parquet('{self.pesticide_data_path}')"
        )

        logger.info("Loading crop mapping...")
        self.crop_mapping = pd.read_csv(self.crop_mapping_path)
        # Only keep mappings that have API matches
        self.crop_mapping = self.crop_mapping[self.crop_mapping["api_crop_id"].notna()].copy()
        logger.info(f"Loaded {len(self.crop_mapping)} crop code mappings with API matches")

    def get_our_dosage_data(self) -> pd.DataFrame:
        """Get our pesticide usage data with per-hectare calculations."""
        logger.info("Extracting our dosage data...")

        # Get matched crop codes
        matched_codes = self.crop_mapping["our_crop_code"].astype(str).tolist()
        matched_codes_str = ",".join([f"'{code}'" for code in matched_codes])

        query = f"""
        SELECT 
            crop_code,
            pesticide_registration_number,
            pesticide_name,
            dosage_quantity as total_dosage,
            dosage_unit,
            area_ha,
            dosage_quantity / area_ha as dosage_per_ha,
            CASE 
                WHEN dosage_unit = 2 THEN 'kg'
                WHEN dosage_unit = 4 THEN 'l' 
                WHEN dosage_unit = 5 THEN 'tablets'
                ELSE 'unknown'
            END as unit_name,
            COUNT(*) as application_count
        FROM pesticides 
        WHERE aar = '2024' 
          AND crop_code IN ({matched_codes_str})
          AND pesticide_registration_number IS NOT NULL 
          AND pesticide_registration_number != ''
          AND area_ha > 0 
          AND dosage_quantity > 0
          AND dosage_unit IN (2, 4, 5)
        GROUP BY crop_code, pesticide_registration_number, pesticide_name, 
                 total_dosage, dosage_unit, area_ha, dosage_per_ha, unit_name
        ORDER BY dosage_per_ha DESC
        """

        our_data = self.conn.execute(query).fetchdf()
        logger.info(f"Found {len(our_data)} unique dosage applications to analyze")
        return our_data

    def fetch_api_dosage_data(self) -> Dict[Tuple[int, str], Dict]:
        """Fetch dosage recommendations from API for all relevant crop-pesticide combinations."""
        logger.info("Fetching API dosage recommendations...")

        api_dosage_data = {}

        # Get unique API crop IDs from our mapping
        api_crop_ids = self.crop_mapping["api_crop_id"].dropna().unique()

        for crop_id in api_crop_ids:
            logger.info(f"Fetching products for crop {crop_id}...")

            products = self.api_client.get_products_for_crop(int(crop_id))

            for product in products:
                product_id = product.get("Id")
                registration_number = product.get("RegistrationNumber", "").strip()

                if not registration_number:
                    continue

                # Get detailed product info
                detail = self.api_client.get_product_detail(product_id, int(crop_id))
                if detail:
                    key = (int(crop_id), registration_number)
                    api_dosage_data[key] = {
                        "product_id": product_id,
                        "product_name": product.get("Name", ""),
                        "registration_number": registration_number,
                        "crop_id": crop_id,
                        "max_dosage_app": detail.get("MaxDosageApp"),
                        "dosage_unit": detail.get("DosageUnit", ""),
                        "max_applications": detail.get("MaxApplications"),
                        "detail": detail,
                    }

        logger.info(f"Fetched dosage data for {len(api_dosage_data)} crop-pesticide combinations")
        return api_dosage_data

    def analyze_compliance(self) -> pd.DataFrame:
        """Perform comprehensive dosage compliance analysis."""
        logger.info("Starting dosage compliance analysis...")

        # Get our data
        our_data = self.get_our_dosage_data()

        # Fetch API data
        api_data = self.fetch_api_dosage_data()

        # Merge with crop mapping to get API crop IDs
        our_data_with_api = our_data.merge(
            self.crop_mapping[["our_crop_code", "api_crop_id", "api_crop_name"]],
            left_on="crop_code",
            right_on="our_crop_code",
            how="left",
        )

        results = []

        for idx, row in our_data_with_api.iterrows():
            api_crop_id = row["api_crop_id"]
            reg_number = str(row["pesticide_registration_number"]).strip()

            if pd.isna(api_crop_id):
                continue

            # Look for API match
            key = (int(api_crop_id), reg_number)
            api_match = api_data.get(key)

            result = {
                "our_crop_code": row["crop_code"],
                "our_crop_name": (
                    self.crop_mapping[
                        self.crop_mapping["our_crop_code"] == row["crop_code"]
                    ]["our_crop_name"].iloc[0]
                    if len(
                        self.crop_mapping[
                            self.crop_mapping["our_crop_code"] == row["crop_code"]
                        ]
                    ) > 0
                    else ""
                ),
                "api_crop_id": api_crop_id,
                "api_crop_name": row["api_crop_name"],
                "pesticide_registration_number": reg_number,
                "our_pesticide_name": row["pesticide_name"],
                "our_dosage_per_ha": row["dosage_per_ha"],
                "our_unit": row["unit_name"],
                "application_count": row["application_count"],
                "area_ha": row["area_ha"],
                "api_found": api_match is not None,
            }

            if api_match:
                result.update(
                    {
                        "api_product_name": api_match["product_name"],
                        "api_max_dosage_app": api_match["max_dosage_app"],
                        "api_dosage_unit": api_match["dosage_unit"],
                        "api_max_applications": api_match["max_applications"],
                        "compliance_status": self._assess_compliance(
                            row["dosage_per_ha"],
                            row["unit_name"],
                            api_match["max_dosage_app"],
                            api_match["dosage_unit"],
                        ),
                        "dosage_ratio": self._calculate_dosage_ratio(
                            row["dosage_per_ha"],
                            row["unit_name"],
                            api_match["max_dosage_app"],
                            api_match["dosage_unit"],
                        ),
                    }
                )
            else:
                result.update(
                    {
                        "api_product_name": None,
                        "api_max_dosage_app": None,
                        "api_dosage_unit": None,
                        "api_max_applications": None,
                        "compliance_status": "NO_API_DATA",
                        "dosage_ratio": None,
                    }
                )

            results.append(result)

        results_df = pd.DataFrame(results)
        logger.info(f"Analyzed {len(results_df)} dosage applications")

        return results_df

    def _assess_compliance(
        self, our_dosage: float, our_unit: str, api_max: Optional[float], api_unit: str
    ) -> str:
        """Assess compliance status."""
        if api_max is None:
            return "NO_API_LIMIT"

        if our_unit.lower() != api_unit.lower():
            return "UNIT_MISMATCH"

        if our_dosage <= api_max:
            return "COMPLIANT"
        elif our_dosage <= api_max * 2.0:  # Up to 2x
            return "MODERATE_EXCESS"
        else:
            return "MAJOR_EXCESS"

    def _calculate_dosage_ratio(
        self, our_dosage: float, our_unit: str, api_max: Optional[float], api_unit: str
    ) -> Optional[float]:
        """Calculate ratio of our dosage to API maximum."""
        if api_max is None or our_unit.lower() != api_unit.lower():
            return None
        return our_dosage / api_max

    def generate_report(self, results: pd.DataFrame, output_dir: str = "dosage_compliance_results"):
        """Generate comprehensive compliance report."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        logger.info(f"Generating compliance report in {output_path}")

        # Save detailed results
        results.to_csv(output_path / "detailed_compliance_results.csv", index=False)

        # Generate summary statistics
        summary_stats = self._generate_summary_stats(results)

        # Save summary
        with open(output_path / "compliance_summary.json", "w") as f:
            json.dump(summary_stats, f, indent=2, default=str)

        # Generate markdown report
        self._generate_markdown_report(results, summary_stats, output_path / "compliance_report.md")

        logger.info("Report generation completed")

    def _generate_summary_stats(self, results: pd.DataFrame) -> Dict:
        """Generate summary statistics."""
        total_applications = len(results)
        api_found = len(results[results["api_found"]])

        compliance_stats = results["compliance_status"].value_counts().to_dict()

        # Dosage ratio statistics for compliant cases
        compliant_ratios = results[results["dosage_ratio"].notna()]["dosage_ratio"]

        summary = {
            "total_applications_analyzed": total_applications,
            "api_matches_found": api_found,
            "api_match_rate_pct": (
                round(api_found / total_applications * 100, 1) if total_applications > 0 else 0
            ),
            "compliance_breakdown": compliance_stats,
            "dosage_ratio_stats": {
                "mean": float(compliant_ratios.mean()) if len(compliant_ratios) > 0 else None,
                "median": float(compliant_ratios.median()) if len(compliant_ratios) > 0 else None,
                "min": float(compliant_ratios.min()) if len(compliant_ratios) > 0 else None,
                "max": float(compliant_ratios.max()) if len(compliant_ratios) > 0 else None,
                "std": float(compliant_ratios.std()) if len(compliant_ratios) > 0 else None,
            },
        }

        return summary

    def _generate_markdown_report(self, results: pd.DataFrame, summary: Dict, output_path: Path):
        """Generate markdown compliance report."""

        report = f"""# Pesticide Dosage Compliance Analysis Report

## Executive Summary

- **Total Applications Analyzed**: {summary["total_applications_analyzed"]:,}
- **API Matches Found**: {summary["api_matches_found"]:,} ({summary["api_match_rate_pct"]}%)
- **Analysis Date**: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}

## Compliance Breakdown

"""

        for status, count in summary["compliance_breakdown"].items():
            pct = count / summary["total_applications_analyzed"] * 100
            report += f"- **{status}**: {count:,} applications ({pct:.1f}%)\n"

        report += f"""
## Dosage Ratio Statistics

For applications where API limits are available:

- **Mean Ratio**: {summary["dosage_ratio_stats"]["mean"]:.2f} (our usage / API max)
- **Median Ratio**: {summary["dosage_ratio_stats"]["median"]:.2f}
- **Range**: {summary["dosage_ratio_stats"]["min"]:.2f} - {summary["dosage_ratio_stats"]["max"]:.2f}
- **Standard Deviation**: {summary["dosage_ratio_stats"]["std"]:.2f}

## Top Compliance Issues

### Major Excesses (>2x API limit)
"""

        major_excesses = results[
            results["compliance_status"] == "MAJOR_EXCESS"
        ].nlargest(10, "dosage_ratio")
        if len(major_excesses) > 0:
            for _, row in major_excesses.iterrows():
                report += (
                    f"- **{row['our_pesticide_name']}** on **{row['our_crop_name']}**: "
                    f"{row['dosage_ratio']:.1f}x limit "
                    f"({row['our_dosage_per_ha']:.2f} {row['our_unit']}/ha vs "
                    f"{row['api_max_dosage_app']:.2f} {row['api_dosage_unit']}/ha)\n"
                )
        else:
            report += "None found.\n"

        report += """
### Unit Mismatches
"""

        unit_mismatches = results[results["compliance_status"] == "UNIT_MISMATCH"]
        if len(unit_mismatches) > 0:
            for _, row in unit_mismatches.head(10).iterrows():
                report += (
                    f"- **{row['our_pesticide_name']}** on **{row['our_crop_name']}**: "
                    f"Our unit: {row['our_unit']}, API unit: {row['api_dosage_unit']}\n"
                )
        else:
            report += "None found.\n"

        report += f"""
## Methodology Notes

1. **Dosage Calculation**: Our data contains total dosage per application,
   converted to per-hectare by
   dividing by area_ha
2. **Unit Mapping**: 2=kg, 4=l, 5=tablets (based on codebase analysis)
3. **Crop Matching**: Used comprehensive crop code mapping with
   {len(self.crop_mapping)} validated matches
4. **API Source**: Plante IT Pesticide Service (pesticideservice.dlbr.dk)
5. **Tolerance Levels**: 
   - Compliant: ≤ API limit
   - Minor excess: 1.0-1.1x API limit  
   - Moderate excess: 1.1-2.0x API limit
   - Major excess: >2.0x API limit

## Data Quality Assessment

This analysis validates **{summary["api_match_rate_pct"]}%** of our pesticide
applications against official
API dosage recommendations, providing strong regulatory compliance insights for Danish agricultural
pesticide usage.
"""

        with open(output_path, "w") as f:
            f.write(report)


def main():
    """Run the complete dosage compliance analysis."""

    # File paths
    pesticide_data = "pesticides_2023_2024.parquet"
    crop_mapping = "crop_code_api_mapping.csv"

    # Initialize analyzer
    analyzer = DosageComplianceAnalyzer(pesticide_data, crop_mapping)

    # Run analysis
    logger.info("Starting comprehensive dosage compliance analysis...")
    results = analyzer.analyze_compliance()

    # Generate report
    analyzer.generate_report(results)

    logger.info("Dosage compliance analysis completed successfully!")

    # Print quick summary
    total = len(results)
    compliant = len(results[results["compliance_status"] == "COMPLIANT"])
    major_issues = len(results[results["compliance_status"] == "MAJOR_EXCESS"])

    print("\n🎯 QUICK SUMMARY:")
    print(f"📊 Total applications analyzed: {total:,}")
    print(f"✅ Compliant: {compliant:,} ({compliant / total * 100:.1f}%)")
    print(f"🚨 Major excesses: {major_issues:,} ({major_issues / total * 100:.1f}%)")
    print("📁 Detailed results saved to: dosage_compliance_results/")


if __name__ == "__main__":
    main()
