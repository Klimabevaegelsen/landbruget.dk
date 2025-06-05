import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import ibis
import ibis.expr.datatypes as dt
import pandas as pd

# Import config
# Import export module
from . import config, export

# Note: String sanitization is now handled using native Ibis functions:
# col.cast(dt.string).strip().nullif("")

# Helper function to simplify casting and sanitizing in Ibis
# Note: This UDF approach might have limitations depending on the backend capabilities.
# If performance issues arise, alternative SQL-based sanitization might be needed.
# @ibis.udf.scalar.python # REMOVED UDF
# def sanitize_and_cast_string(col: ibis.expr.types.StringValue) -> ibis.expr.types.StringValue:
#     """Sanitizes a string column in Ibis."""
#     # This is a placeholder; direct string manipulation in Ibis/SQL is preferred
#     # For complex sanitization, execution to pandas might still be necessary
#     # but _sanitize_string likely involves basic SQL functions (trim, replace)
#     # Example: return col.strip().nullif('')
#     # We'll keep the pandas sanitization step for now as _sanitize_string implementation isn't shown
#     # return col # Placeholder


def create_properties_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the properties table (excluding owner/user IDs) with geometry."""
    logging.info("Starting creation of properties table (excluding owner/user IDs).")

    if ejendom_oplys_raw is None:
        logging.warning("Cannot create properties table: ejendom_oplys_raw is None.")
        return None

    try:
        # --- STEP 1: Define Ibis Expression for Extraction and Initial Transformation ---

        # Reference the base table
        t = ejendom_oplys_raw

        # Select and cast other fields
        properties_selected = t.select(
            chr_number=t.Response.EjendomsOplysninger.ChrNummer.try_cast(dt.int64),
            address=t.Response.EjendomsOplysninger.Ejendom.Adresse.try_cast(dt.string),
            city_name=t.Response.EjendomsOplysninger.Ejendom.ByNavn.try_cast(dt.string),
            postal_code=t.Response.EjendomsOplysninger.Ejendom.PostNummer.try_cast(dt.int32),
            postal_district=t.Response.EjendomsOplysninger.Ejendom.PostDistrikt.try_cast(dt.string),
            municipality_code=t.Response.EjendomsOplysninger.Ejendom.KommuneNummer.try_cast(dt.int32),
            municipality_name=t.Response.EjendomsOplysninger.Ejendom.KommuneNavn.try_cast(dt.string),
            food_region_number=t.Response.EjendomsOplysninger.FVST.FoedevareRegionsNummer.try_cast(dt.int32),
            food_region_name=t.Response.EjendomsOplysninger.FVST.FoedevareRegionsNavn.try_cast(dt.string),
            vet_dept_name=t.Response.EjendomsOplysninger.FVST.VeterinaerAfdelingsNavn.try_cast(dt.string),
            vet_section_name=t.Response.EjendomsOplysninger.FVST.VeterinaerSektionsNavn.try_cast(dt.string),
            geo_coord_x_source=t.Response.EjendomsOplysninger.StaldKoordinater.StaldKoordinatX.try_cast(dt.float64),
            geo_coord_y_source=t.Response.EjendomsOplysninger.StaldKoordinater.StaldKoordinatY.try_cast(dt.float64),
            date_created_str=t.Response.EjendomsOplysninger.Ejendom.DatoOpret.try_cast(dt.string),
            date_updated_str=t.Response.EjendomsOplysninger.Ejendom.DatoOpdatering.try_cast(dt.string),
        ).distinct()

        # Filter after selection
        properties_intermediate = properties_selected.filter(properties_selected.chr_number.notnull())

        # --- STEP 2: Deduplication in Ibis ---
        properties_deduped = properties_intermediate

        logging.info("Ibis expression defined for properties table (excluding owner/user).")

        # --- STEP 3: Execute intermediate result ---
        logging.info("Executing intermediate Ibis expression to Pandas for geometry and date conversion...")
        try:
            df_intermediate = properties_deduped.execute()
            if df_intermediate.empty:
                logging.warning("Intermediate DataFrame for properties is empty after Ibis processing.")
                return None
            logging.info(f"Intermediate DataFrame shape for properties: {df_intermediate.shape}")
        except Exception as e:
            logging.error(
                f"Failed to execute intermediate Ibis expression for properties: {e}",
                exc_info=True,
            )
            return None

        # --- STEP 4: Pandas-based Cleaning (Keep minimal) ---
        # Apply sanitization using native string functions
        str_cols_to_sanitize = [
            "address",
            "city_name",
            "postal_district",
            "municipality_name",
            "food_region_name",
            "vet_dept_name",
            "vet_section_name",
        ]
        for col in str_cols_to_sanitize:
            if col in df_intermediate.columns:
                df_intermediate[col] = df_intermediate[col].astype(str).str.strip().replace("", pd.NA)

        # Convert date strings to date objects
        date_cols = {
            "date_created_str": "date_created",
            "date_updated_str": "date_updated",
        }
        for str_col, final_col in date_cols.items():
            if str_col in df_intermediate.columns:
                df_intermediate[final_col] = pd.to_datetime(df_intermediate[str_col], errors="coerce").dt.date
                df_intermediate = df_intermediate.drop(columns=[str_col])

        logging.info(f"Properties count after Pandas cleaning: {len(df_intermediate)}")

        # --- STEP 5: Create Geometry using GeoPandas ---
        logging.info("Creating geometry for properties...")

        df_filtered = df_intermediate[
            df_intermediate["geo_coord_x_source"].notna() & df_intermediate["geo_coord_y_source"].notna()
        ].copy()

        if df_filtered.empty:
            logging.warning("No valid coordinates found for properties geometry.")
            return None

        try:
            gdf = gpd.GeoDataFrame(
                df_filtered,
                geometry=gpd.points_from_xy(
                    df_filtered.geo_coord_x_source,
                    df_filtered.geo_coord_y_source,
                    crs=config.SOURCE_CRS,  # Set initial CRS
                ),
            )
            # Add CRS source column AFTER filtering and GDF creation
            gdf["geo_crs_source"] = config.SOURCE_CRS

            logging.info(f"Created initial GeoDataFrame for properties. Shape: {gdf.shape}")

            # --- STEP 6: Transform CRS ---
            logging.info(f"Transforming properties geometry to target CRS: {config.TARGET_CRS}...")
            gdf = gdf.to_crs(config.TARGET_CRS)
            logging.info(f"Transformed properties GeoDataFrame. Shape: {gdf.shape}")

        except Exception as e:
            logging.error(
                f"Failed during properties GeoDataFrame creation/transformation: {e}",
                exc_info=True,
            )
            return None

        # --- STEP 7: Define final column order and Save to GeoParquet ---
        final_cols_order = [
            "chr_number",
            "address",
            "city_name",
            "postal_code",
            "postal_district",
            "municipality_code",
            "municipality_name",
            "food_region_number",
            "food_region_name",
            "vet_dept_name",
            "vet_section_name",
            "geometry",
            "geo_coord_x_source",
            "geo_coord_y_source",
            "geo_crs_source",
            "date_created",
            "date_updated",
        ]
        # Rename 'geometry' column if needed, though GeoDataFrame usually handles it.
        # If gdf.geometry.name is not 'geometry', uncomment below:
        # gdf = gdf.rename_geometry('geo_geometry') # Or whatever final_cols_order expects

        # Ensure all desired columns exist before selection/reindexing
        # Adjust final_cols_order if 'geometry' is the standard name now
        if "geometry" in gdf.columns and "geo_geometry" not in gdf.columns:
            final_cols_order = [col if col != "geo_geometry" else "geometry" for col in final_cols_order]

        available_cols = gdf.columns
        cols_to_select = [col for col in final_cols_order if col in available_cols]

        # Reindex to ensure final column order and select subset
        gdf_final = gdf.reindex(columns=cols_to_select)

        output_path = silver_dir / "properties.geoparquet"
        try:
            logging.info(f"Saving final properties GeoDataFrame... Output path: {output_path}")
            logging.info("GeoDataFrame info before save:")
            logging.info(f"Shape: {gdf_final.shape}")
            logging.info(f"CRS: {gdf_final.crs}")
            logging.info(f"Memory usage: {gdf_final.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            logging.info(f"Columns: {gdf_final.columns.tolist()}")

            # Verify geometry column
            if "geometry" not in gdf_final.columns:
                raise ValueError("GeoDataFrame is missing the 'geometry' column")
            if not all(gdf_final.geometry.is_valid):
                invalid_geoms = gdf_final[~gdf_final.geometry.is_valid]
                logging.warning(f"Found {len(invalid_geoms)} invalid geometries. Attempting to fix...")
                gdf_final.geometry = gdf_final.geometry.buffer(0)  # Simple fix attempt

            saved_path = export.save_table(output_path, gdf_final, is_geo=True)
            if saved_path is None:
                logging.error("Failed to save properties table - no path returned")
                return None
            logging.info(f"Successfully saved properties table to {saved_path}")
            return None  # Successfully saved, no need to return a table

        except Exception as e:
            logging.error(f"Failed during properties GeoParquet save: {e}", exc_info=True)
            return None

    except Exception as e:
        logging.error(f"Failed to create properties table: {e}", exc_info=True)
        return None


def create_property_owners_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the property_owners table including attributes from ejendom details."""
    if ejendom_oplys_raw is None:
        logging.warning("Skipping property_owners table creation: ejendom_oplys_raw is None.")
        return None

    logging.info("Starting creation of property_owners table with attributes.")

    try:
        base_table_name = ejendom_oplys_raw.op().name

        # Unnest the Besaetning list and extract owner identifiers + attributes
        prop_owners = con.sql(f"""
            WITH unnested_herds AS (
                SELECT
                    Response.EjendomsOplysninger.ChrNummer AS chr_number_raw,
                    UNNEST(Response.EjendomsOplysninger.Besaetninger.Besaetning) AS herd
                FROM {base_table_name}
            )
            SELECT DISTINCT
                chr_number_raw,
                herd.Ejer.CvrNummer AS owner_cvr_raw,
                herd.Ejer.CprNummer AS owner_cpr_raw,
                herd.Ejer.Navn AS owner_name_raw,
                herd.Ejer.Adresse AS owner_address_raw,
                herd.Ejer.PostNummer AS owner_postal_code_raw,
                herd.Ejer.PostDistrikt AS owner_postal_district_raw,
                herd.Ejer.ByNavn AS owner_city_raw,
                herd.Ejer.KommuneNummer AS owner_municipality_code_raw,
                herd.Ejer.KommuneNavn AS owner_municipality_name_raw,
                herd.Ejer.Land AS owner_country_raw,
                herd.Ejer.TelefonNummer AS owner_phone_raw,
                herd.Ejer.MobilNummer AS owner_mobile_raw,
                herd.Ejer.Email AS owner_email_raw,
                herd.Ejer.Adressebeskyttelse AS owner_address_protection_raw,
                herd.Ejer.Reklamebeskyttelse AS owner_advertising_protection_raw
            FROM unnested_herds
            WHERE chr_number_raw IS NOT NULL
              AND herd.Ejer IS NOT NULL
              AND (
                  (herd.Ejer.CvrNummer IS NOT NULL OR herd.Ejer.CprNummer IS NOT NULL)
                  OR
                  (herd.Ejer.Navn IS NOT NULL AND herd.Ejer.Adresse IS NOT NULL AND herd.Ejer.PostNummer IS NOT NULL)
               )
        """)

        # Clean and cast columns
        prop_owners = prop_owners.mutate(
            chr_number=ibis.coalesce(
                prop_owners.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            owner_cvr=prop_owners.owner_cvr_raw.cast(dt.string).strip().nullif(""),
            owner_cpr=prop_owners.owner_cpr_raw.cast(dt.string).strip().nullif(""),
            owner_name=prop_owners.owner_name_raw.cast(dt.string).strip().nullif(""),
            owner_address=prop_owners.owner_address_raw.cast(dt.string).strip().nullif(""),
            owner_postal_code=prop_owners.owner_postal_code_raw.cast(dt.string).strip().nullif(""),
            owner_postal_district=prop_owners.owner_postal_district_raw.cast(dt.string).strip().nullif(""),
            owner_city=prop_owners.owner_city_raw.cast(dt.string).strip().nullif(""),
            owner_municipality_code=ibis.coalesce(
                prop_owners.owner_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            owner_municipality_name=prop_owners.owner_municipality_name_raw.cast(dt.string).strip().nullif(""),
            owner_country=prop_owners.owner_country_raw.cast(dt.string).strip().nullif(""),
            owner_phone=prop_owners.owner_phone_raw.cast(dt.string).strip().nullif(""),
            owner_mobile=prop_owners.owner_mobile_raw.cast(dt.string).strip().nullif(""),
            owner_email=prop_owners.owner_email_raw.cast(dt.string).strip().nullif(""),
            owner_address_protection=prop_owners.owner_address_protection_raw.cast(dt.string).strip().nullif(""),
            owner_advertising_protection=prop_owners.owner_advertising_protection_raw.cast(dt.string)
            .strip()
            .nullif(""),
        )

        # Filter out rows with null identifiers after cleaning
        prop_owners = prop_owners.filter(prop_owners.chr_number.notnull())

        # Define final columns order
        final_cols = [
            "chr_number",
            "owner_cvr",
            "owner_cpr",
            "owner_name",
            "owner_address",
            "owner_postal_code",
            "owner_postal_district",
            "owner_city",
            "owner_municipality_code",
            "owner_municipality_name",
            "owner_country",
            "owner_phone",
            "owner_mobile",
            "owner_email",
            "owner_address_protection",
            "owner_advertising_protection",
        ]
        prop_owners_final = prop_owners.select(*[col for col in final_cols if col in prop_owners.columns])

        # Save to parquet
        output_path = silver_dir / "property_owners.parquet"
        rows = prop_owners_final.count().execute()
        if rows == 0:
            logging.warning("Property owners table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving property_owners table with attributes ({rows} rows).")
        saved_path = export.save_table(output_path, prop_owners_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save property_owners table - no path returned")
            return None
        logging.info(f"Saved property_owners table to {saved_path}")

        return prop_owners_final

    except Exception as e:
        logging.error(f"Failed to create property_owners table: {e}", exc_info=True)
        return None


def create_property_users_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the property_users table including attributes from ejendom details."""
    if ejendom_oplys_raw is None:
        logging.warning("Skipping property_users table creation: ejendom_oplys_raw is None.")
        return None

    logging.info("Starting creation of property_users table with attributes.")

    try:
        base_table_name = ejendom_oplys_raw.op().name

        # Unnest the Besaetning list and extract user identifiers + attributes
        prop_users = con.sql(f"""
            WITH unnested_herds AS (
                SELECT
                    Response.EjendomsOplysninger.ChrNummer AS chr_number_raw,
                    UNNEST(Response.EjendomsOplysninger.Besaetninger.Besaetning) AS herd
                FROM {base_table_name}
            )
            SELECT DISTINCT
                chr_number_raw,
                herd.Bruger.CvrNummer AS user_cvr_raw,
                herd.Bruger.CprNummer AS user_cpr_raw,
                herd.Bruger.Navn AS user_name_raw,
                herd.Bruger.Adresse AS user_address_raw,
                herd.Bruger.PostNummer AS user_postal_code_raw,
                herd.Bruger.PostDistrikt AS user_postal_district_raw,
                herd.Bruger.ByNavn AS user_city_raw,
                herd.Bruger.KommuneNummer AS user_municipality_code_raw,
                herd.Bruger.KommuneNavn AS user_municipality_name_raw,
                herd.Bruger.Land AS user_country_raw,
                herd.Bruger.TelefonNummer AS user_phone_raw,
                herd.Bruger.MobilNummer AS user_mobile_raw,
                herd.Bruger.Email AS user_email_raw,
                herd.Bruger.Adressebeskyttelse AS user_address_protection_raw,
                herd.Bruger.Reklamebeskyttelse AS user_advertising_protection_raw
            FROM unnested_herds
            WHERE chr_number_raw IS NOT NULL
              AND herd.Bruger IS NOT NULL
              AND (
                  (herd.Bruger.CvrNummer IS NOT NULL OR herd.Bruger.CprNummer IS NOT NULL)
                  OR
                  (herd.Bruger.Navn IS NOT NULL AND herd.Bruger.Adresse IS NOT NULL AND herd.Bruger.PostNummer IS NOT NULL)
              )
        """)

        # Clean and cast columns
        prop_users = prop_users.mutate(
            chr_number=ibis.coalesce(
                prop_users.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            user_cvr=prop_users.user_cvr_raw.cast(dt.string).strip().nullif(""),
            user_cpr=prop_users.user_cpr_raw.cast(dt.string).strip().nullif(""),
            user_name=prop_users.user_name_raw.cast(dt.string).strip().nullif(""),
            user_address=prop_users.user_address_raw.cast(dt.string).strip().nullif(""),
            user_postal_code=prop_users.user_postal_code_raw.cast(dt.string).strip().nullif(""),
            user_postal_district=prop_users.user_postal_district_raw.cast(dt.string).strip().nullif(""),
            user_city=prop_users.user_city_raw.cast(dt.string).strip().nullif(""),
            user_municipality_code=ibis.coalesce(
                prop_users.user_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            user_municipality_name=prop_users.user_municipality_name_raw.cast(dt.string).strip().nullif(""),
            user_country=prop_users.user_country_raw.cast(dt.string).strip().nullif(""),
            user_phone=prop_users.user_phone_raw.cast(dt.string).strip().nullif(""),
            user_mobile=prop_users.user_mobile_raw.cast(dt.string).strip().nullif(""),
            user_email=prop_users.user_email_raw.cast(dt.string).strip().nullif(""),
            user_address_protection=prop_users.user_address_protection_raw.cast(dt.string).strip().nullif(""),
            user_advertising_protection=prop_users.user_advertising_protection_raw.cast(dt.string).strip().nullif(""),
        )

        # Filter out rows with null identifiers after cleaning
        prop_users = prop_users.filter(prop_users.chr_number.notnull())

        # Define final columns order
        final_cols = [
            "chr_number",
            "user_cvr",
            "user_cpr",
            "user_name",
            "user_address",
            "user_postal_code",
            "user_postal_district",
            "user_city",
            "user_municipality_code",
            "user_municipality_name",
            "user_country",
            "user_phone",
            "user_mobile",
            "user_email",
            "user_address_protection",
            "user_advertising_protection",
        ]
        prop_users_final = prop_users.select(*[col for col in final_cols if col in prop_users.columns])

        # Save to parquet
        output_path = silver_dir / "property_users.parquet"
        rows = prop_users_final.count().execute()
        if rows == 0:
            logging.warning("Property users table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving property_users table with attributes ({rows} rows).")
        saved_path = export.save_table(output_path, prop_users_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save property_users table - no path returned")
            return None
        logging.info(f"Saved property_users table to {saved_path}")

        return prop_users_final

    except Exception as e:
        logging.error(f"Failed to create property_users table: {e}", exc_info=True)
        return None
