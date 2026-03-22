"""Silver transform tests for svineflytning pipeline.

Tests the DuckDB SQL transformations that convert raw_movements
into silver_movements, silver_properties, and silver_vehicles.
"""

import duckdb
import pytest


@pytest.fixture()
def silver_conn(sample_bronze_data):
    """DuckDB connection with raw_movements populated and silver tables created.

    Replicates the exact SQL from SvineflytningSilverProcessor without
    importing the class (avoids cloud storage dependencies).
    """
    conn = duckdb.connect(":memory:")

    # Create raw_movements schema (same as silver/transform.py)
    conn.execute("""
        CREATE TABLE raw_movements (
            Id BIGINT, Oprindelse VARCHAR, Handling VARCHAR,
            FlytteTidspunkt_SvineflytDato DATE,
            FlytteTidspunkt_SvineflytTidspunkt INTEGER,
            FlytteTidspunkt_SvineflytRaekkefoelge INTEGER,
            Afsender_Landekode VARCHAR, Afsender_ChrNummer BIGINT,
            Afsender_BesaetningsNummer BIGINT,
            Afsender_Ejendom_Adresse VARCHAR, Afsender_Ejendom_ByNavn VARCHAR,
            Afsender_Ejendom_PostNummer INTEGER,
            Afsender_Ejendom_PostDistrikt VARCHAR,
            Afsender_Ejendom_KommuneNummer INTEGER,
            Afsender_Ejendom_KommuneNavn VARCHAR,
            Afsender_Ejendom_DatoOpret DATE,
            Afsender_Ejendom_DatoOpdatering DATE,
            Afsender_UdlandsEjendom VARCHAR,
            Modtager_Landekode VARCHAR, Modtager_ChrNummer BIGINT,
            Modtager_BesaetningsNummer BIGINT,
            Modtager_Ejendom_Adresse VARCHAR, Modtager_Ejendom_ByNavn VARCHAR,
            Modtager_Ejendom_PostNummer INTEGER,
            Modtager_Ejendom_PostDistrikt VARCHAR,
            Modtager_Ejendom_KommuneNummer INTEGER,
            Modtager_Ejendom_KommuneNavn VARCHAR,
            Modtager_Ejendom_DatoOpret DATE,
            Modtager_Ejendom_DatoOpdatering DATE,
            Modtager_UdlandsEjendom VARCHAR,
            AntalDyr_AntalDyrIAlt INTEGER, AntalDyr_AntalSoer INTEGER,
            AntalDyr_AntalSlagtesvin INTEGER,
            AntalDyr_Antal190LitersContainere INTEGER,
            AntalDyr_Antal240LitersContainere INTEGER,
            Koeretoej_Forvogn_Landekode VARCHAR,
            Koeretoej_Forvogn_RegNr VARCHAR,
            Koeretoej_Haenger_Landekode VARCHAR,
            Koeretoej_Haenger_RegNr VARCHAR,
            Omlaesser VARCHAR, TracesDokument VARCHAR,
            Sundhedscertifikat VARCHAR,
            IndberetterLogon VARCHAR, IndberetningForetaget TIMESTAMP,
            _chunk_timestamp VARCHAR, _chunk_start_date DATE,
            _chunk_end_date DATE
        )
    """)

    # Insert test movements from fixture
    for chunk in sample_bronze_data:
        response = chunk.get("response", {}).get("Response", {})
        svineflytning_liste = response.get("SvineflytningListe", {})
        movements = svineflytning_liste.get("Svineflytning", [])

        for movement in movements:
            flyttetidspunkt = movement.get("FlytteTidspunkt") or {}
            afsender = movement.get("Afsender") or {}
            afsender_ejendom = afsender.get("Ejendom") or {}
            modtager = movement.get("Modtager") or {}
            modtager_ejendom = modtager.get("Ejendom") or {}
            antal_dyr = movement.get("AntalDyr") or {}
            koeretoej = movement.get("Koeretoej") or {}
            forvogn = koeretoej.get("Forvogn") or {}
            haenger = koeretoej.get("Haenger") or {}

            conn.execute(
                "INSERT INTO raw_movements VALUES (" + ", ".join(["?"] * 47) + ")",
                [
                    movement.get("Id"),
                    movement.get("Oprindelse"),
                    movement.get("Handling"),
                    flyttetidspunkt.get("SvineflytDato"),
                    flyttetidspunkt.get("SvineflytTidspunkt"),
                    flyttetidspunkt.get("SvineflytRaekkefoelge"),
                    afsender.get("Landekode"),
                    afsender.get("ChrNummer"),
                    afsender.get("BesaetningsNummer"),
                    afsender_ejendom.get("Adresse"),
                    afsender_ejendom.get("ByNavn"),
                    afsender_ejendom.get("PostNummer"),
                    afsender_ejendom.get("PostDistrikt"),
                    afsender_ejendom.get("KommuneNummer"),
                    afsender_ejendom.get("KommuneNavn"),
                    afsender_ejendom.get("DatoOpret"),
                    afsender_ejendom.get("DatoOpdatering"),
                    afsender.get("UdlandsEjendom"),
                    modtager.get("Landekode"),
                    modtager.get("ChrNummer"),
                    modtager.get("BesaetningsNummer"),
                    modtager_ejendom.get("Adresse"),
                    modtager_ejendom.get("ByNavn"),
                    modtager_ejendom.get("PostNummer"),
                    modtager_ejendom.get("PostDistrikt"),
                    modtager_ejendom.get("KommuneNummer"),
                    modtager_ejendom.get("KommuneNavn"),
                    modtager_ejendom.get("DatoOpret"),
                    modtager_ejendom.get("DatoOpdatering"),
                    modtager.get("UdlandsEjendom"),
                    antal_dyr.get("AntalDyrIAlt"),
                    antal_dyr.get("AntalSoer"),
                    antal_dyr.get("AntalSlagtesvin"),
                    antal_dyr.get("Antal190LitersContainere"),
                    antal_dyr.get("Antal240LitersContainere"),
                    forvogn.get("Landekode"),
                    forvogn.get("RegNr"),
                    haenger.get("Landekode") if haenger else None,
                    haenger.get("RegNr") if haenger else None,
                    movement.get("Omlaesser"),
                    movement.get("TracesDokument"),
                    movement.get("Sundhedscertifikat"),
                    movement.get("IndberetterLogon"),
                    movement.get("IndberetningForetaget"),
                    movement.get("_chunk_timestamp"),
                    movement.get("_chunk_start_date"),
                    movement.get("_chunk_end_date"),
                ],
            )

    export_timestamp = "20260322_120000"

    # Create silver_movements (exact SQL from transform.py)
    conn.execute(f"""
        CREATE OR REPLACE TABLE silver_movements AS
        SELECT
            Id as movement_id, Oprindelse as origin_system, Handling as action_type,
            FlytteTidspunkt_SvineflytDato as movement_date,
            FlytteTidspunkt_SvineflytTidspunkt as movement_time,
            FlytteTidspunkt_SvineflytRaekkefoelge as movement_sequence,
            Afsender_Landekode as sender_country_code,
            Afsender_ChrNummer as sender_chr_number,
            Afsender_BesaetningsNummer as sender_herd_number,
            Afsender_Ejendom_Adresse as sender_address,
            Afsender_Ejendom_ByNavn as sender_city_name,
            Afsender_Ejendom_PostNummer as sender_postal_code,
            Afsender_Ejendom_PostDistrikt as sender_postal_district,
            Afsender_Ejendom_KommuneNummer as sender_municipality_code,
            Afsender_Ejendom_KommuneNavn as sender_municipality_name,
            Afsender_Ejendom_DatoOpret as sender_property_created,
            Afsender_Ejendom_DatoOpdatering as sender_property_updated,
            Afsender_UdlandsEjendom as sender_foreign_property,
            Modtager_Landekode as receiver_country_code,
            Modtager_ChrNummer as receiver_chr_number,
            Modtager_BesaetningsNummer as receiver_herd_number,
            Modtager_Ejendom_Adresse as receiver_address,
            Modtager_Ejendom_ByNavn as receiver_city_name,
            Modtager_Ejendom_PostNummer as receiver_postal_code,
            Modtager_Ejendom_PostDistrikt as receiver_postal_district,
            Modtager_Ejendom_KommuneNummer as receiver_municipality_code,
            Modtager_Ejendom_KommuneNavn as receiver_municipality_name,
            Modtager_Ejendom_DatoOpret as receiver_property_created,
            Modtager_Ejendom_DatoOpdatering as receiver_property_updated,
            Modtager_UdlandsEjendom as receiver_foreign_property,
            AntalDyr_AntalDyrIAlt as total_animals,
            AntalDyr_AntalSoer as sow_count,
            AntalDyr_AntalSlagtesvin as slaughter_pig_count,
            AntalDyr_Antal190LitersContainere as containers_190l,
            AntalDyr_Antal240LitersContainere as containers_240l,
            Koeretoej_Forvogn_Landekode as vehicle_country_code,
            Koeretoej_Forvogn_RegNr as vehicle_registration,
            Koeretoej_Haenger_Landekode as trailer_country_code,
            Koeretoej_Haenger_RegNr as trailer_registration,
            Omlaesser as transshipment_info,
            TracesDokument as traces_document,
            Sundhedscertifikat as health_certificate,
            IndberetterLogon as reporter_login,
            IndberetningForetaget as report_timestamp,
            '{export_timestamp}' as processed_timestamp,
            _chunk_timestamp as source_chunk_timestamp,
            _chunk_start_date as source_period_start,
            _chunk_end_date as source_period_end,
            CASE WHEN Handling = 'slet' THEN true ELSE false END as is_deleted,
            CASE WHEN Id IS NULL THEN true ELSE false END as is_invalid,
            CASE WHEN AntalDyr_AntalDyrIAlt IS NULL OR AntalDyr_AntalDyrIAlt <= 0
                THEN true ELSE false END as missing_animal_count
        FROM raw_movements
        WHERE Id IS NOT NULL
    """)

    # Create silver_properties
    conn.execute(f"""
        CREATE OR REPLACE TABLE silver_properties AS
        WITH sender_properties AS (
            SELECT DISTINCT
                Afsender_ChrNummer as chr_number,
                Afsender_BesaetningsNummer as herd_number,
                'sender' as property_role,
                Afsender_Ejendom_Adresse as address,
                Afsender_Ejendom_ByNavn as city_name,
                Afsender_Ejendom_PostNummer as postal_code,
                Afsender_Ejendom_PostDistrikt as postal_district,
                Afsender_Ejendom_KommuneNummer as municipality_code,
                Afsender_Ejendom_KommuneNavn as municipality_name,
                Afsender_Ejendom_DatoOpret as date_created,
                Afsender_Ejendom_DatoOpdatering as date_updated,
                Afsender_UdlandsEjendom as foreign_property
            FROM raw_movements WHERE Afsender_ChrNummer IS NOT NULL
        ),
        receiver_properties AS (
            SELECT DISTINCT
                Modtager_ChrNummer as chr_number,
                Modtager_BesaetningsNummer as herd_number,
                'receiver' as property_role,
                Modtager_Ejendom_Adresse as address,
                Modtager_Ejendom_ByNavn as city_name,
                Modtager_Ejendom_PostNummer as postal_code,
                Modtager_Ejendom_PostDistrikt as postal_district,
                Modtager_Ejendom_KommuneNummer as municipality_code,
                Modtager_Ejendom_KommuneNavn as municipality_name,
                Modtager_Ejendom_DatoOpret as date_created,
                Modtager_Ejendom_DatoOpdatering as date_updated,
                Modtager_UdlandsEjendom as foreign_property
            FROM raw_movements WHERE Modtager_ChrNummer IS NOT NULL
        ),
        all_properties AS (
            SELECT * FROM sender_properties
            UNION ALL
            SELECT * FROM receiver_properties
        )
        SELECT chr_number, herd_number, address, city_name, postal_code,
               postal_district, municipality_code, municipality_name,
               date_created, date_updated, foreign_property,
               '{export_timestamp}' as processed_timestamp,
               COUNT(*) as occurrence_count,
               ARRAY_AGG(DISTINCT property_role) as roles
        FROM all_properties WHERE chr_number IS NOT NULL
        GROUP BY chr_number, herd_number, address, city_name, postal_code,
                 postal_district, municipality_code, municipality_name,
                 date_created, date_updated, foreign_property
    """)

    # Create silver_vehicles
    conn.execute(f"""
        CREATE OR REPLACE TABLE silver_vehicles AS
        SELECT DISTINCT
            Koeretoej_Forvogn_RegNr as vehicle_registration,
            Koeretoej_Forvogn_Landekode as vehicle_country_code,
            Koeretoej_Haenger_RegNr as trailer_registration,
            Koeretoej_Haenger_Landekode as trailer_country_code,
            '{export_timestamp}' as processed_timestamp,
            COUNT(*) as usage_count,
            MIN(FlytteTidspunkt_SvineflytDato) as first_movement_date,
            MAX(FlytteTidspunkt_SvineflytDato) as last_movement_date
        FROM raw_movements
        WHERE Koeretoej_Forvogn_RegNr IS NOT NULL
        GROUP BY Koeretoej_Forvogn_RegNr, Koeretoej_Forvogn_Landekode,
                 Koeretoej_Haenger_RegNr, Koeretoej_Haenger_Landekode
    """)

    yield conn
    conn.close()


class TestSilverMovementsTransform:
    """Test the silver_movements table produced by the transform."""

    def test_filters_null_id_records(self, silver_conn):
        """Records with null Id are filtered out."""
        count = silver_conn.execute(
            "SELECT COUNT(*) FROM silver_movements WHERE movement_id IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_valid_movement_preserved(self, silver_conn):
        """The valid movement (Id=100001) should be in silver."""
        row = silver_conn.execute(
            "SELECT * FROM silver_movements WHERE movement_id = 100001"
        ).fetchone()
        assert row is not None

    def test_deleted_movement_preserved_with_flag(self, silver_conn):
        """Deleted movement (Id=100002) should be in silver with is_deleted=true."""
        row = silver_conn.execute(
            "SELECT is_deleted FROM silver_movements WHERE movement_id = 100002"
        ).fetchone()
        assert row is not None
        assert row[0] is True

    def test_is_deleted_false_for_normal_movement(self, silver_conn):
        row = silver_conn.execute(
            "SELECT is_deleted FROM silver_movements WHERE movement_id = 100001"
        ).fetchone()
        assert row[0] is False

    def test_missing_animal_count_flag(self, silver_conn):
        """Deleted movement with null animals should have missing_animal_count=true."""
        row = silver_conn.execute(
            "SELECT missing_animal_count FROM silver_movements WHERE movement_id = 100002"
        ).fetchone()
        assert row[0] is True

    def test_animal_count_present_for_valid(self, silver_conn):
        row = silver_conn.execute(
            "SELECT total_animals, missing_animal_count FROM silver_movements WHERE movement_id = 100001"
        ).fetchone()
        assert row[0] == 150
        assert row[1] is False

    def test_sender_chr_mapped_correctly(self, silver_conn):
        row = silver_conn.execute(
            "SELECT sender_chr_number FROM silver_movements WHERE movement_id = 100001"
        ).fetchone()
        assert row[0] == 123456

    def test_receiver_chr_mapped_correctly(self, silver_conn):
        row = silver_conn.execute(
            "SELECT receiver_chr_number FROM silver_movements WHERE movement_id = 100001"
        ).fetchone()
        assert row[0] == 654321

    def test_processed_timestamp_set(self, silver_conn):
        row = silver_conn.execute(
            "SELECT processed_timestamp FROM silver_movements LIMIT 1"
        ).fetchone()
        assert row[0] == "20260322_120000"

    def test_column_count(self, silver_conn):
        result = silver_conn.execute("SELECT * FROM silver_movements LIMIT 0").description
        assert len(result) == 51  # 48 data + 3 quality flags


class TestSilverPropertiesTransform:
    """Test the silver_properties table."""

    def test_properties_extracted_from_movements(self, silver_conn):
        """Properties should be extracted from sender and receiver data."""
        count = silver_conn.execute("SELECT COUNT(*) FROM silver_properties").fetchone()[0]
        assert count > 0

    def test_sender_property_present(self, silver_conn):
        """Sender CHR 123456 should appear in properties."""
        row = silver_conn.execute(
            "SELECT * FROM silver_properties WHERE chr_number = 123456"
        ).fetchone()
        assert row is not None

    def test_receiver_property_present(self, silver_conn):
        """Receiver CHR 654321 should appear in properties."""
        row = silver_conn.execute(
            "SELECT * FROM silver_properties WHERE chr_number = 654321"
        ).fetchone()
        assert row is not None

    def test_null_chr_excluded(self, silver_conn):
        """Properties with null CHR should be excluded."""
        count = silver_conn.execute(
            "SELECT COUNT(*) FROM silver_properties WHERE chr_number IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_occurrence_count_positive(self, silver_conn):
        row = silver_conn.execute(
            "SELECT occurrence_count FROM silver_properties WHERE chr_number = 123456"
        ).fetchone()
        assert row[0] > 0


class TestSilverVehiclesTransform:
    """Test the silver_vehicles table."""

    def test_vehicles_extracted(self, silver_conn):
        count = silver_conn.execute("SELECT COUNT(*) FROM silver_vehicles").fetchone()[0]
        assert count > 0

    def test_vehicle_registration_present(self, silver_conn):
        row = silver_conn.execute(
            "SELECT * FROM silver_vehicles WHERE vehicle_registration = 'AB12345'"
        ).fetchone()
        assert row is not None

    def test_null_registration_excluded(self, silver_conn):
        count = silver_conn.execute(
            "SELECT COUNT(*) FROM silver_vehicles WHERE vehicle_registration IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_usage_count_tracked(self, silver_conn):
        row = silver_conn.execute(
            "SELECT usage_count FROM silver_vehicles WHERE vehicle_registration = 'AB12345'"
        ).fetchone()
        assert row[0] >= 1

    def test_movement_date_range_tracked(self, silver_conn):
        row = silver_conn.execute(
            "SELECT first_movement_date, last_movement_date FROM silver_vehicles WHERE vehicle_registration = 'AB12345'"
        ).fetchone()
        assert row[0] is not None
        assert row[1] is not None
        assert row[0] <= row[1]
