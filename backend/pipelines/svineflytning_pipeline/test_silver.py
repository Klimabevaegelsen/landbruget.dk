#!/usr/bin/env python3
"""
Test script for Svineflytning silver processing.

This script tests the silver layer implementation with a small sample of data
to validate the transformation logic before running on the full dataset.
"""

import json
import logging
import tempfile
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_test_data():
    """Create a small sample of test data that matches the bronze data structure."""
    test_data = [
        {
            "timestamp": "2025-01-15T10:00:00.000000",
            "start_date": "2025-01-14",
            "end_date": "2025-01-15",
            "response": {
                "GLRCHRWSInfoOutbound": {
                    "KlientId": "LandbrugsData",
                    "BrugerNavn": "42731978",
                    "SessionId": "1",
                    "TrackID": "1",
                    "ReturSvar": "OK",
                    "GLRCHRSvarMeddelelser": None,
                    "SvartidMS": 0,
                },
                "Response": {
                    "SvineflytningListe": {
                        "Svineflytning": [
                            {
                                "Oprindelse": "INDL",
                                "Handling": "ret",
                                "Id": 12345678,
                                "FlytteTidspunkt": {
                                    "SvineflytDato": "2025-01-14",
                                    "SvineflytTidspunkt": 800,
                                    "SvineflytRaekkefoelge": 1,
                                },
                                "Afsender": {
                                    "Landekode": "DK",
                                    "ChrNummer": 12345,
                                    "Ejendom": {
                                        "Adresse": "Test Farm Road 123",
                                        "ByNavn": "TestBy",
                                        "PostNummer": 1234,
                                        "PostDistrikt": "Test District",
                                        "KommuneNummer": 123,
                                        "KommuneNavn": "Test Municipality",
                                        "DatoOpret": "2020-01-01",
                                        "DatoOpdatering": "2024-01-01",
                                    },
                                    "BesaetningsNummer": 12345,
                                    "UdlandsEjendom": None,
                                },
                                "Modtager": {
                                    "Landekode": "DK",
                                    "ChrNummer": 67890,
                                    "Ejendom": {
                                        "Adresse": "Receiver Farm Street 456",
                                        "ByNavn": "ReceiverBy",
                                        "PostNummer": 5678,
                                        "PostDistrikt": "Receiver District",
                                        "KommuneNummer": 456,
                                        "KommuneNavn": "Receiver Municipality",
                                        "DatoOpret": "2019-06-15",
                                        "DatoOpdatering": "2023-12-01",
                                    },
                                    "BesaetningsNummer": 67890,
                                    "UdlandsEjendom": None,
                                },
                                "AntalDyr": {
                                    "AntalDyrIAlt": 25,
                                    "AntalSoer": 5,
                                    "AntalSlagtesvin": 20,
                                    "Antal190LitersContainere": None,
                                    "Antal240LitersContainere": None,
                                },
                                "Koeretoej": {
                                    "Forvogn": {"Landekode": "DK", "RegNr": "AB12345"},
                                    "Haenger": {"RegNr": "TR67890"},
                                },
                                "Omlaesser": None,
                                "TracesDokument": None,
                                "Sundhedscertifikat": None,
                                "IndberetterLogon": "test_user:test-session-id",
                                "IndberetningForetaget": "2025-01-15T09:30:00",
                            },
                            {
                                "Oprindelse": "INDL",
                                "Handling": "slet",
                                "Id": 12345679,
                                "FlytteTidspunkt": None,
                                "Afsender": None,
                                "Modtager": None,
                                "AntalDyr": None,
                                "Koeretoej": None,
                                "Omlaesser": None,
                                "TracesDokument": None,
                                "Sundhedscertifikat": None,
                                "IndberetterLogon": None,
                                "IndberetningForetaget": None,
                            },
                        ]
                    }
                },
            },
        }
    ]

    return test_data


def test_silver_processing():
    """Test the silver processing with sample data."""
    logger.info("Starting silver processing test")

    try:
        # Import silver processing
        from silver.transform import SvineflytningSilverProcessor

        # Create test data
        test_data = create_test_data()
        logger.info(f"Created test data with {len(test_data)} chunks")

        # Create temporary file for test data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp_file:
            json.dump(test_data, tmp_file, indent=2)
            tmp_file_path = tmp_file.name

        logger.info(f"Saved test data to: {tmp_file_path}")

        # Create temporary output directory
        with tempfile.TemporaryDirectory() as tmp_output_dir:
            logger.info(f"Using output directory: {tmp_output_dir}")

            # Initialize processor
            processor = SvineflytningSilverProcessor(output_dir=tmp_output_dir)

            # Process the test data
            result = processor.process_bronze_data(
                bronze_data_path=tmp_file_path, export_timestamp="test_20250115_100000"
            )

            # Check results
            if result["success"]:
                logger.info("✅ Silver processing test PASSED")
                logger.info(f"📊 Processed {result['processed_movements']} movements")
                logger.info(f"💾 Output: {result['output_path']}")

                # Check if output files exist
                output_path = Path(result["output_path"])
                expected_files = ["movements.parquet", "properties.parquet", "vehicles.parquet"]

                for filename in expected_files:
                    file_path = output_path / filename
                    if file_path.exists():
                        logger.info(f"✅ Found expected file: {filename}")
                    else:
                        logger.error(f"❌ Missing expected file: {filename}")

                # Try to read one of the files to validate structure
                try:
                    import duckdb

                    conn = duckdb.connect()
                    movements_path = output_path / "movements.parquet"
                    if movements_path.exists():
                        df = conn.execute(f"SELECT * FROM '{movements_path}' LIMIT 5").fetchdf()
                        logger.info(f"✅ Successfully read movements table: {len(df)} rows, {len(df.columns)} columns")
                        logger.info(f"Columns: {list(df.columns)}")

                except Exception as e:
                    logger.warning(f"Could not validate parquet files: {e}")

            else:
                logger.error(f"❌ Silver processing test FAILED: {result['error']}")
                return False

        # Clean up temporary file
        Path(tmp_file_path).unlink()

        return True

    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_silver_processing()
    if success:
        print("🎉 All tests passed!")
        exit(0)
    else:
        print("💥 Tests failed!")
        exit(1)
