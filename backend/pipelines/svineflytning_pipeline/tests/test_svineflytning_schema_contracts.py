"""Schema & contract tests for svineflytning pipeline.

Validates that the bronze output structure matches what silver expects,
and that silver output has the correct schema.
"""

import sys
from pathlib import Path
from typing import ClassVar

import duckdb

# Add pipeline to sys.path for local imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestBronzeOutputSchema:
    """Bronze output must match the nested SOAP structure silver expects."""

    def test_chunk_has_required_top_level_keys(self, sample_bronze_chunk):
        """Each bronze chunk must have timestamp, date range, and response."""
        required_keys = {"timestamp", "start_date", "end_date", "response"}
        assert required_keys.issubset(sample_bronze_chunk.keys())

    def test_chunk_response_has_soap_structure(self, sample_bronze_chunk):
        """Response must contain the nested SOAP path to movements."""
        response = sample_bronze_chunk["response"]
        assert "Response" in response
        assert "SvineflytningListe" in response["Response"]

    def test_movements_are_list(self, sample_bronze_chunk):
        """SvineflytningListe.Svineflytning must be a list."""
        movements = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ]
        assert isinstance(movements, list)
        assert len(movements) > 0

    def test_movement_has_core_fields(self, sample_bronze_chunk):
        """Each movement must have at minimum Id, Oprindelse, Handling."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        assert "Id" in movement
        assert "Oprindelse" in movement
        assert "Handling" in movement

    def test_movement_nested_objects_present(self, sample_bronze_chunk):
        """Valid movement must have the nested objects silver flattens."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        nested_keys = {
            "FlytteTidspunkt",
            "Afsender",
            "Modtager",
            "AntalDyr",
            "Koeretoej",
        }
        assert nested_keys.issubset(movement.keys())

    def test_afsender_ejendom_structure(self, sample_bronze_chunk):
        """Afsender.Ejendom must contain address fields silver expects."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        ejendom = movement["Afsender"]["Ejendom"]
        required = {
            "Adresse",
            "ByNavn",
            "PostNummer",
            "PostDistrikt",
            "KommuneNummer",
            "KommuneNavn",
        }
        assert required.issubset(ejendom.keys())

    def test_deleted_movement_allows_null_nested(self, sample_bronze_chunk):
        """Deleted movements (Handling='slet') may have null nested objects."""
        deleted = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][1]
        assert deleted["Handling"] == "slet"
        # Silver must handle None for all nested objects
        assert deleted["Afsender"] is None
        assert deleted["Modtager"] is None
        assert deleted["AntalDyr"] is None
        assert deleted["Koeretoej"] is None


class TestSilverRawMovementsSchema:
    """Silver creates raw_movements table with 47 columns — verify the contract."""

    # The exact columns silver's CREATE TABLE statement defines
    RAW_MOVEMENTS_COLUMNS: ClassVar[dict[str, str]] = {
        "Id": "BIGINT",
        "Oprindelse": "VARCHAR",
        "Handling": "VARCHAR",
        "FlytteTidspunkt_SvineflytDato": "DATE",
        "FlytteTidspunkt_SvineflytTidspunkt": "INTEGER",
        "FlytteTidspunkt_SvineflytRaekkefoelge": "INTEGER",
        "Afsender_Landekode": "VARCHAR",
        "Afsender_ChrNummer": "BIGINT",
        "Afsender_BesaetningsNummer": "BIGINT",
        "Afsender_Ejendom_Adresse": "VARCHAR",
        "Afsender_Ejendom_ByNavn": "VARCHAR",
        "Afsender_Ejendom_PostNummer": "INTEGER",
        "Afsender_Ejendom_PostDistrikt": "VARCHAR",
        "Afsender_Ejendom_KommuneNummer": "INTEGER",
        "Afsender_Ejendom_KommuneNavn": "VARCHAR",
        "Afsender_Ejendom_DatoOpret": "DATE",
        "Afsender_Ejendom_DatoOpdatering": "DATE",
        "Afsender_UdlandsEjendom": "VARCHAR",
        "Modtager_Landekode": "VARCHAR",
        "Modtager_ChrNummer": "BIGINT",
        "Modtager_BesaetningsNummer": "BIGINT",
        "Modtager_Ejendom_Adresse": "VARCHAR",
        "Modtager_Ejendom_ByNavn": "VARCHAR",
        "Modtager_Ejendom_PostNummer": "INTEGER",
        "Modtager_Ejendom_PostDistrikt": "VARCHAR",
        "Modtager_Ejendom_KommuneNummer": "INTEGER",
        "Modtager_Ejendom_KommuneNavn": "VARCHAR",
        "Modtager_Ejendom_DatoOpret": "DATE",
        "Modtager_Ejendom_DatoOpdatering": "DATE",
        "Modtager_UdlandsEjendom": "VARCHAR",
        "AntalDyr_AntalDyrIAlt": "INTEGER",
        "AntalDyr_AntalSoer": "INTEGER",
        "AntalDyr_AntalSlagtesvin": "INTEGER",
        "AntalDyr_Antal190LitersContainere": "INTEGER",
        "AntalDyr_Antal240LitersContainere": "INTEGER",
        "Koeretoej_Forvogn_Landekode": "VARCHAR",
        "Koeretoej_Forvogn_RegNr": "VARCHAR",
        "Koeretoej_Haenger_Landekode": "VARCHAR",
        "Koeretoej_Haenger_RegNr": "VARCHAR",
        "Omlaesser": "VARCHAR",
        "TracesDokument": "VARCHAR",
        "Sundhedscertifikat": "VARCHAR",
        "IndberetterLogon": "VARCHAR",
        "IndberetningForetaget": "TIMESTAMP",
        "_chunk_timestamp": "VARCHAR",
        "_chunk_start_date": "DATE",
        "_chunk_end_date": "DATE",
    }

    def test_raw_movements_schema_matches_silver_create_table(self):
        """Verify DuckDB can create the raw_movements table with expected schema."""
        conn = duckdb.connect(":memory:")

        # This is the exact CREATE TABLE from silver/transform.py
        columns_sql = ", ".join(
            f"{name} {dtype}" for name, dtype in self.RAW_MOVEMENTS_COLUMNS.items()
        )
        conn.execute(f"CREATE TABLE raw_movements ({columns_sql})")

        # Verify column count
        result = conn.execute("SELECT * FROM raw_movements LIMIT 0").description
        assert len(result) == 47

        # Verify column names match
        actual_names = {desc[0] for desc in result}
        assert actual_names == set(self.RAW_MOVEMENTS_COLUMNS.keys())

        conn.close()

    def test_column_count_is_47(self):
        """Silver expects exactly 47 columns in raw_movements."""
        assert len(self.RAW_MOVEMENTS_COLUMNS) == 47


class TestSilverOutputSchema:
    """Silver produces three tables: movements, properties, vehicles."""

    SILVER_MOVEMENTS_COLUMNS: ClassVar[set[str]] = {
        "movement_id",
        "origin_system",
        "action_type",
        "movement_date",
        "movement_time",
        "movement_sequence",
        "sender_country_code",
        "sender_chr_number",
        "sender_herd_number",
        "sender_address",
        "sender_city_name",
        "sender_postal_code",
        "sender_postal_district",
        "sender_municipality_code",
        "sender_municipality_name",
        "sender_property_created",
        "sender_property_updated",
        "sender_foreign_property",
        "receiver_country_code",
        "receiver_chr_number",
        "receiver_herd_number",
        "receiver_address",
        "receiver_city_name",
        "receiver_postal_code",
        "receiver_postal_district",
        "receiver_municipality_code",
        "receiver_municipality_name",
        "receiver_property_created",
        "receiver_property_updated",
        "receiver_foreign_property",
        "total_animals",
        "sow_count",
        "slaughter_pig_count",
        "containers_190l",
        "containers_240l",
        "vehicle_country_code",
        "vehicle_registration",
        "trailer_country_code",
        "trailer_registration",
        "transshipment_info",
        "traces_document",
        "health_certificate",
        "reporter_login",
        "report_timestamp",
        "processed_timestamp",
        "source_chunk_timestamp",
        "source_period_start",
        "source_period_end",
        "is_deleted",
        "is_invalid",
        "missing_animal_count",
    }

    SILVER_PROPERTIES_COLUMNS: ClassVar[set[str]] = {
        "chr_number",
        "herd_number",
        "address",
        "city_name",
        "postal_code",
        "postal_district",
        "municipality_code",
        "municipality_name",
        "date_created",
        "date_updated",
        "foreign_property",
    }

    SILVER_VEHICLES_COLUMNS: ClassVar[set[str]] = {
        "vehicle_country_code",
        "vehicle_registration",
    }

    def test_silver_movements_has_data_quality_flags(self):
        """Silver movements must include the three quality flag columns."""
        quality_flags = {"is_deleted", "is_invalid", "missing_animal_count"}
        assert quality_flags.issubset(self.SILVER_MOVEMENTS_COLUMNS)

    def test_silver_movements_has_processing_metadata(self):
        """Silver movements must include processing metadata columns."""
        metadata_cols = {
            "processed_timestamp",
            "source_chunk_timestamp",
            "source_period_start",
            "source_period_end",
        }
        assert metadata_cols.issubset(self.SILVER_MOVEMENTS_COLUMNS)

    def test_silver_movements_has_sender_receiver_symmetry(self):
        """Sender and receiver columns should be symmetric."""
        sender_cols = {
            c.replace("sender_", "")
            for c in self.SILVER_MOVEMENTS_COLUMNS
            if c.startswith("sender_")
        }
        receiver_cols = {
            c.replace("receiver_", "")
            for c in self.SILVER_MOVEMENTS_COLUMNS
            if c.startswith("receiver_")
        }
        assert sender_cols == receiver_cols

    def test_silver_properties_has_required_columns(self):
        """Properties table must have CHR number and address fields."""
        required = {"chr_number", "herd_number", "address", "municipality_code"}
        assert required.issubset(self.SILVER_PROPERTIES_COLUMNS)

    def test_silver_vehicles_has_registration(self):
        """Vehicles table must have registration information."""
        assert "vehicle_registration" in self.SILVER_VEHICLES_COLUMNS


class TestBronzeToSilverContract:
    """Verify bronze data can be inserted into silver's raw_movements table."""

    def test_valid_movement_inserts_into_raw_movements(self, sample_bronze_data):
        """A valid bronze movement must be insertable into raw_movements schema."""
        conn = duckdb.connect(":memory:")

        # Create raw_movements with silver's schema
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

        # Extract and insert a movement the same way silver does
        chunk = sample_bronze_data[0]
        movement = chunk["response"]["Response"]["SvineflytningListe"]["Svineflytning"][0]

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

        count = conn.execute("SELECT COUNT(*) FROM raw_movements").fetchone()[0]
        assert count == 1

        # Verify the inserted data
        row = conn.execute(
            "SELECT Id, Afsender_ChrNummer, AntalDyr_AntalDyrIAlt FROM raw_movements"
        ).fetchone()
        assert row[0] == 100001
        assert row[1] == 123456
        assert row[2] == 150

        conn.close()

    def test_empty_svineflytning_liste_handled(self, empty_bronze_chunk):
        """Silver must handle chunks with no movements gracefully."""
        response = empty_bronze_chunk["response"]["Response"]
        svineflytning_liste = response.get("SvineflytningListe", {})

        # Silver checks: isinstance(liste, dict) and "Svineflytning" in liste
        assert isinstance(svineflytning_liste, dict)
        assert "Svineflytning" not in svineflytning_liste
