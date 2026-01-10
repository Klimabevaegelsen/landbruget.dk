# Climate Tool Integration Example

## Complete Pipeline Flow

This document shows how all climate tool components work together.

```
┌─────────────────────────────────────────────────────────────────┐
│                     Climate Tool Architecture                    │
└─────────────────────────────────────────────────────────────────┘

  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
  │ Data Loader  │────────▶│ Calculator   │────────▶│Output Writer │
  │              │         │              │         │              │
  │ Load from:   │         │ Calculate:   │         │ Write to:    │
  │ - CHR        │         │ - Cattle     │         │ - GCS Gold   │
  │ - FVM        │         │ - Fields     │         │ - Supabase   │
  │ - Fertilizer │         │ - Energy     │         │              │
  └──────────────┘         └──────────────┘         └──────────────┘
```

## Complete Example: Single Farm

```python
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter

# Initialize components
loader = ClimateDataLoader()
calculator = FarmClimateCalculator(loader)
writer = ClimateOutputWriter()

# Calculate emissions for one farm
cvr = "12345678"
year = 2024

report = calculator.calculate_emissions(cvr=cvr, year=year)

# Print summary
print(f"\nClimate Report for CVR {report.cvr}, Year {report.year}")
print(f"Total CO2e: {report.total_co2e_kg:,.0f} kg")
print(f"Data Completeness: {report.data_completeness:.1%}")
print(f"\nCategories:")
for cat in report.categories:
    print(f"  {cat.name}: {cat.co2e_kg:,.0f} kg CO2e ({cat.data_quality})")
    for source_name, source_co2e in cat.sub_sources.items():
        print(f"    - {source_name}: {source_co2e:,.0f} kg CO2e")

# Write to GCS
output_path = writer.write_to_gold_layer([report])
print(f"\n✅ Wrote report to: {output_path}")

# Optional: Sync to Supabase
if writer.sync_to_supabase([report]):
    print("✅ Synced to Supabase")
```

## Complete Example: Multiple Farms (Batch Processing)

```python
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter
from datetime import datetime

def process_batch(cvr_list: list[str], year: int, batch_name: str = None):
    """
    Process emissions for a batch of farms.

    Args:
        cvr_list: List of CVR numbers to process
        year: Year to calculate emissions for
        batch_name: Optional name for the batch (for logging)

    Returns:
        List of EmissionReport objects
    """
    # Initialize components
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)
    writer = ClimateOutputWriter()

    batch_id = batch_name or datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n{'='*60}")
    print(f"Processing Batch: {batch_id}")
    print(f"Farms: {len(cvr_list)}, Year: {year}")
    print(f"{'='*60}\n")

    # Process each farm
    reports = []
    failed = []

    for i, cvr in enumerate(cvr_list, 1):
        try:
            print(f"[{i}/{len(cvr_list)}] Processing CVR {cvr}...", end=" ")
            report = calculator.calculate_emissions(cvr=cvr, year=year)
            reports.append(report)
            print(f"✅ {report.total_co2e_kg:,.0f} kg CO2e")
        except Exception as e:
            failed.append((cvr, str(e)))
            print(f"❌ Failed: {e}")

    # Write results
    if reports:
        print(f"\n{'='*60}")
        print(f"Writing {len(reports)} reports to GCS...")
        print(f"{'='*60}\n")

        # Custom run metadata
        run_metadata = {
            "batch_id": batch_id,
            "batch_size": len(cvr_list),
            "successful": len(reports),
            "failed": len(failed),
            "failed_cvrs": [cvr for cvr, _ in failed] if failed else [],
        }

        output_path = writer.write_to_gold_layer(
            reports,
            run_metadata=run_metadata
        )

        print(f"✅ Wrote {len(reports)} reports to:")
        print(f"   {output_path}")

        # Optional: Sync to Supabase
        if writer.sync_to_supabase(reports):
            print(f"✅ Synced to Supabase")

    # Summary
    print(f"\n{'='*60}")
    print(f"Batch Summary")
    print(f"{'='*60}")
    print(f"Total farms:       {len(cvr_list)}")
    print(f"Successful:        {len(reports)}")
    print(f"Failed:            {len(failed)}")
    if reports:
        total_emissions = sum(r.total_co2e_kg for r in reports)
        avg_emissions = total_emissions / len(reports)
        avg_completeness = sum(r.data_completeness for r in reports) / len(reports)
        print(f"Total emissions:   {total_emissions:,.0f} kg CO2e")
        print(f"Average emissions: {avg_emissions:,.0f} kg CO2e")
        print(f"Avg completeness:  {avg_completeness:.1%}")

    if failed:
        print(f"\nFailed CVRs:")
        for cvr, error in failed:
            print(f"  - {cvr}: {error}")

    return reports, failed


# Example: Process 100 farms
if __name__ == "__main__":
    # Load CVR list from somewhere (e.g., database, CSV, etc.)
    cvr_list = [
        "12345678",
        "87654321",
        "11111111",
        # ... more CVRs
    ]

    # Process all farms for 2024
    reports, failed = process_batch(cvr_list, year=2024, batch_name="production_2024")
```

## Complete Example: Incremental Updates

```python
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter

def process_new_farms_only(year: int, existing_cvrs: set[str] = None):
    """
    Process only farms that haven't been calculated yet.

    Args:
        year: Year to calculate emissions for
        existing_cvrs: Set of CVRs already processed (optional)

    Returns:
        List of new EmissionReport objects
    """
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)
    writer = ClimateOutputWriter()

    # Get all farms with data for the year
    # (In practice, this would query your farm registry)
    all_cvrs = get_all_farms_with_data(year)

    # Filter out already processed farms
    if existing_cvrs:
        new_cvrs = [cvr for cvr in all_cvrs if cvr not in existing_cvrs]
        print(f"Found {len(new_cvrs)} new farms to process (total: {len(all_cvrs)})")
    else:
        new_cvrs = all_cvrs
        print(f"Processing all {len(new_cvrs)} farms")

    # Process new farms
    reports = []
    for cvr in new_cvrs:
        try:
            report = calculator.calculate_emissions(cvr=cvr, year=year)
            reports.append(report)
        except Exception as e:
            print(f"Failed to process {cvr}: {e}")

    # Write incremental update
    if reports:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = writer.write_to_gold_layer(
            reports,
            timestamp=f"{timestamp}_incremental",
            run_metadata={
                "update_type": "incremental",
                "new_farms": len(reports),
            }
        )
        print(f"✅ Wrote {len(reports)} new reports to {output_path}")

    return reports


def get_all_farms_with_data(year: int) -> list[str]:
    """Get all farms that have data available for the year."""
    # Implementation depends on your data sources
    # This is a placeholder
    return []
```

## Complete Example: Data Quality Report

```python
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter
import pandas as pd

def generate_quality_report(reports: list) -> pd.DataFrame:
    """
    Generate a data quality report from emission reports.

    Args:
        reports: List of EmissionReport objects

    Returns:
        DataFrame with quality metrics
    """
    records = []

    for report in reports:
        # Overall quality
        record = {
            'cvr': report.cvr,
            'year': report.year,
            'total_co2e_kg': report.total_co2e_kg,
            'data_completeness': report.data_completeness,
            'categories_count': len(report.categories),
        }

        # Category quality breakdown
        for cat in report.categories:
            record[f'{cat.name}_quality'] = cat.data_quality
            record[f'{cat.name}_co2e_kg'] = cat.co2e_kg

        records.append(record)

    df = pd.DataFrame(records)

    # Summary statistics
    print("\nData Quality Summary")
    print("=" * 60)
    print(f"Total farms:           {len(df)}")
    print(f"Avg completeness:      {df['data_completeness'].mean():.1%}")
    print(f"Complete data:         {len(df[df['data_completeness'] >= 0.9])} farms")
    print(f"Partial data:          {len(df[df['data_completeness'] < 0.9])} farms")
    print(f"\nCategory Quality:")

    for col in df.columns:
        if col.endswith('_quality'):
            cat_name = col.replace('_quality', '')
            quality_counts = df[col].value_counts()
            print(f"  {cat_name}:")
            for quality, count in quality_counts.items():
                print(f"    {quality}: {count} farms")

    return df


# Example usage
if __name__ == "__main__":
    # Calculate emissions
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)

    cvrs = ["12345678", "87654321", "11111111"]
    reports = [calculator.calculate_emissions(cvr=cvr, year=2024) for cvr in cvrs]

    # Generate quality report
    quality_df = generate_quality_report(reports)

    # Save quality report
    quality_df.to_csv("climate_quality_report.csv", index=False)
    print("\n✅ Quality report saved to climate_quality_report.csv")

    # Write emission reports
    writer = ClimateOutputWriter()
    output_path = writer.write_to_gold_layer(reports)
    print(f"✅ Emission reports written to {output_path}")
```

## Complete Example: Reading Previous Reports

```python
from output_writer import ClimateOutputWriter

def analyze_historical_reports():
    """Analyze historical emission reports."""
    writer = ClimateOutputWriter()

    # List all available reports
    all_reports = writer.list_available_reports()
    print(f"Found {len(all_reports)} historical reports\n")

    # Analyze each report
    for report_path in all_reports[:5]:  # Show first 5
        metadata = writer.read_report_metadata(report_path)

        if metadata:
            timestamp = metadata['timestamp']
            count = metadata['report_count']
            total_emissions = metadata['statistics']['total_emissions_kg_co2e']
            avg_completeness = metadata['statistics']['avg_data_completeness']

            print(f"Report: {timestamp}")
            print(f"  Farms:       {count}")
            print(f"  Emissions:   {total_emissions:,.0f} kg CO2e")
            print(f"  Completeness: {avg_completeness:.1%}")
            print()


# Example: Compare reports over time
def compare_reports(pattern_1: str, pattern_2: str):
    """Compare two sets of reports."""
    writer = ClimateOutputWriter()

    reports_1 = writer.list_available_reports(pattern=pattern_1)
    reports_2 = writer.list_available_reports(pattern=pattern_2)

    if reports_1 and reports_2:
        meta_1 = writer.read_report_metadata(reports_1[0])
        meta_2 = writer.read_report_metadata(reports_2[0])

        print(f"Comparison: {pattern_1} vs {pattern_2}")
        print("=" * 60)

        if meta_1 and meta_2:
            farms_1 = meta_1['report_count']
            farms_2 = meta_2['report_count']
            emissions_1 = meta_1['statistics']['total_emissions_kg_co2e']
            emissions_2 = meta_2['statistics']['total_emissions_kg_co2e']

            print(f"Farms:     {farms_1} → {farms_2} ({farms_2 - farms_1:+d})")
            print(f"Emissions: {emissions_1:,.0f} → {emissions_2:,.0f} kg CO2e")

            if emissions_1 > 0:
                change_pct = ((emissions_2 - emissions_1) / emissions_1) * 100
                print(f"Change:    {change_pct:+.1f}%")


if __name__ == "__main__":
    # Analyze historical reports
    analyze_historical_reports()

    # Compare January 2024 vs December 2024
    compare_reports("202401*", "202412*")
```

## Environment Setup

```bash
# Set GCS bucket (required)
export GCS_BUCKET="landbrugsdata-raw-data"

# Set Supabase credentials (optional, for sync)
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-service-role-key"
```

## Error Handling Best Practices

```python
import logging
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_process_farm(cvr: str, year: int, calculator, writer):
    """Process a single farm with comprehensive error handling."""
    try:
        # Calculate emissions
        report = calculator.calculate_emissions(cvr=cvr, year=year)

        # Validate report
        if report.total_co2e_kg <= 0:
            logger.warning(f"CVR {cvr}: Zero or negative emissions calculated")

        if report.data_completeness < 0.5:
            logger.warning(f"CVR {cvr}: Low data completeness ({report.data_completeness:.1%})")

        # Write report
        writer.write_to_gold_layer([report])

        return report, None

    except ValueError as e:
        logger.error(f"CVR {cvr}: Invalid data - {e}")
        return None, f"Invalid data: {e}"

    except KeyError as e:
        logger.error(f"CVR {cvr}: Missing required field - {e}")
        return None, f"Missing field: {e}"

    except Exception as e:
        logger.exception(f"CVR {cvr}: Unexpected error - {e}")
        return None, f"Unexpected error: {e}"


# Example usage with error tracking
if __name__ == "__main__":
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)
    writer = ClimateOutputWriter()

    cvrs = ["12345678", "87654321", "11111111"]

    results = []
    errors = []

    for cvr in cvrs:
        report, error = safe_process_farm(cvr, 2024, calculator, writer)
        if report:
            results.append(report)
        else:
            errors.append((cvr, error))

    # Summary
    logger.info(f"Processed {len(results)} farms successfully")
    if errors:
        logger.warning(f"Failed to process {len(errors)} farms:")
        for cvr, error in errors:
            logger.warning(f"  - {cvr}: {error}")
```

## Related Documentation

- [Climate Calculator README](CLIMATE_CALCULATOR_README.md)
- [Data Loader README](DATA_LOADER_README.md)
- [Output Writer README](OUTPUT_WRITER_README.md)
