"""
Test suite for ClimateOutputWriter

This module tests the output writer functionality including:
- Writing to GCS gold layer
- DataFrame conversion
- Metadata generation
- Report listing
"""

from climate_calculator import EmissionCategory, EmissionReport
from output_writer import ClimateOutputWriter


def create_mock_report(cvr: str = "12345678", year: int = 2024) -> EmissionReport:
    """
    Create a mock EmissionReport for testing.

    Args:
        cvr: Company CVR number
        year: Calculation year

    Returns:
        Mock EmissionReport object
    """
    return EmissionReport(
        cvr=cvr,
        year=year,
        total_co2e_kg=150000.0,
        categories=[
            EmissionCategory(
                name="cattle",
                co2e_kg=80000.0,
                data_quality="complete",
                sub_sources={
                    "enteric_methane": 60000.0,
                    "manure_storage": 20000.0,
                },
            ),
            EmissionCategory(
                name="fields",
                co2e_kg=70000.0,
                data_quality="complete",
                sub_sources={
                    "n2o_fertilizer": 40000.0,
                    "carbon_balance": 20000.0,
                    "nitrate_leaching": 10000.0,
                },
            ),
        ],
        intensity_metrics={
            "co2e_per_kg_milk": 1.2,
            "co2e_per_ha": 2500.0,
        },
        data_completeness=0.85,
    )


class TestClimateOutputWriter:
    """Test suite for ClimateOutputWriter class."""

    def test_initialization(self):
        """Test ClimateOutputWriter initialization."""
        writer = ClimateOutputWriter()
        assert writer.bucket is not None
        assert writer.gcs is not None

    def test_reports_to_records(self):
        """Test conversion of EmissionReport to list of dicts."""
        writer = ClimateOutputWriter()
        report = create_mock_report()
        records = writer._reports_to_records([report])

        # Check records structure
        assert len(records) == 1
        assert "cvr" in records[0]
        assert "year" in records[0]
        assert "total_co2e_kg" in records[0]
        assert "data_completeness" in records[0]

        # Check CVR format (8 digits with leading zeros)
        assert records[0]["cvr"] == "12345678"
        assert len(records[0]["cvr"]) == 8

        # Check values
        assert records[0]["year"] == 2024
        assert records[0]["total_co2e_kg"] == 150000.0
        assert records[0]["data_completeness"] == 0.85

    def test_reports_to_records_cvr_padding(self):
        """Test CVR number padding to 8 digits."""
        writer = ClimateOutputWriter()
        report = create_mock_report(cvr="1234567")  # 7 digits
        records = writer._reports_to_records([report])

        # Should be padded to 8 digits
        assert records[0]["cvr"] == "01234567"
        assert len(records[0]["cvr"]) == 8

    def test_categories_to_records(self):
        """Test conversion of EmissionCategory to list of dicts."""
        writer = ClimateOutputWriter()
        report = create_mock_report()
        records = writer._categories_to_records([report])

        # Check records structure
        assert len(records) == 2  # 2 categories (cattle, fields)
        assert all("cvr" in r for r in records)
        assert all("year" in r for r in records)
        assert all("category_name" in r for r in records)
        assert all("co2e_kg" in r for r in records)
        assert all("data_quality" in r for r in records)
        assert all("sub_sources" in r for r in records)

        # Check category data
        cattle_row = next(r for r in records if r["category_name"] == "cattle")
        assert cattle_row["co2e_kg"] == 80000.0
        assert cattle_row["data_quality"] == "complete"
        assert "enteric_methane" in cattle_row["sub_sources"]

    def test_build_metadata(self):
        """Test metadata generation."""
        writer = ClimateOutputWriter()
        reports = [
            create_mock_report(cvr="12345678", year=2024),
            create_mock_report(cvr="87654321", year=2024),
        ]
        timestamp = "20240101_120000"

        metadata = writer._build_metadata(reports, timestamp)

        # Check metadata structure
        assert metadata["timestamp"] == timestamp
        assert metadata["report_count"] == 2
        assert len(metadata["cvr_list"]) == 2
        assert "12345678" in metadata["cvr_list"]
        assert "87654321" in metadata["cvr_list"]

        # Check statistics
        assert "statistics" in metadata
        assert metadata["statistics"]["total_emissions_kg_co2e"] == 300000.0
        assert metadata["statistics"]["avg_emissions_kg_co2e"] == 150000.0

    def test_build_metadata_with_custom_metadata(self):
        """Test metadata generation with custom run metadata."""
        writer = ClimateOutputWriter()
        report = create_mock_report()
        timestamp = "20240101_120000"

        custom_metadata = {
            "pipeline_version": "1.0.0",
            "run_by": "test_suite",
        }

        metadata = writer._build_metadata([report], timestamp, custom_metadata)

        # Check custom metadata is included
        assert "run_metadata" in metadata
        assert metadata["run_metadata"]["pipeline_version"] == "1.0.0"
        assert metadata["run_metadata"]["run_by"] == "test_suite"

    def test_write_to_gold_layer_validation(self):
        """Test validation when writing to gold layer."""
        writer = ClimateOutputWriter()

        # Test with empty reports
        result = writer.write_to_gold_layer([])
        assert result == ""

    def test_multiple_reports_different_years(self):
        """Test handling multiple reports with different years."""
        writer = ClimateOutputWriter()
        reports = [
            create_mock_report(cvr="12345678", year=2023),
            create_mock_report(cvr="12345678", year=2024),
        ]

        records = writer._reports_to_records(reports)
        assert len(records) == 2
        assert {r["year"] for r in records} == {2023, 2024}

        # Check metadata year range
        metadata = writer._build_metadata(reports, "20240101_120000")
        assert metadata["year_range"]["min"] == 2023
        assert metadata["year_range"]["max"] == 2024


def test_mock_report_creation():
    """Test the mock report creation helper."""
    report = create_mock_report()

    assert report.cvr == "12345678"
    assert report.year == 2024
    assert report.total_co2e_kg == 150000.0
    assert len(report.categories) == 2
    assert report.data_completeness == 0.85


def test_emission_category_structure():
    """Test EmissionCategory structure in mock report."""
    report = create_mock_report()

    # Check cattle category
    cattle = report.get_category("cattle")
    assert cattle is not None
    assert cattle.name == "cattle"
    assert cattle.co2e_kg == 80000.0
    assert cattle.data_quality == "complete"
    assert "enteric_methane" in cattle.sub_sources
    assert cattle.sub_sources["enteric_methane"] == 60000.0

    # Check fields category
    fields = report.get_category("fields")
    assert fields is not None
    assert fields.name == "fields"
    assert fields.co2e_kg == 70000.0


if __name__ == "__main__":
    # Run basic tests without pytest
    print("Running basic output_writer tests...")

    # Test 1: Initialization
    print("\n1. Testing initialization...")
    writer = ClimateOutputWriter()
    print("✅ Writer initialized")

    # Test 2: Mock report creation
    print("\n2. Testing mock report creation...")
    report = create_mock_report()
    print(f"✅ Created mock report: CVR {report.cvr}, {report.total_co2e_kg:,.0f} kg CO2e")

    # Test 3: Records conversion
    print("\n3. Testing records conversion...")
    emissions_records = writer._reports_to_records([report])
    print(f"✅ Emissions records: {len(emissions_records)} records")

    categories_records = writer._categories_to_records([report])
    print(f"✅ Categories records: {len(categories_records)} records")

    # Test 4: Metadata generation
    print("\n4. Testing metadata generation...")
    metadata = writer._build_metadata([report], "20240101_120000")
    print(f"✅ Metadata generated: {metadata['report_count']} reports")
    print(f"   Total emissions: {metadata['statistics']['total_emissions_kg_co2e']:,.0f} kg CO2e")

    # Test 5: Multiple reports
    print("\n5. Testing multiple reports...")
    reports = [
        create_mock_report(cvr="12345678", year=2024),
        create_mock_report(cvr="87654321", year=2024),
    ]
    multi_records = writer._reports_to_records(reports)
    print(f"✅ Multiple reports records: {len(multi_records)} records")

    print("\n" + "=" * 50)
    print("✅ All basic tests passed!")
    print("=" * 50)
    print("\nTo run full pytest suite:")
    print("  cd backend/pipelines/climate")
    print("  pytest test_output_writer.py -v")
