# Documentation: Populating Mock Data for Supabase Tables

## 1. Introduction

This document outlines the process and considerations for populating the PostgreSQL tables within your Supabase project with mock data. It assumes the database schema has been successfully created according to the specifications derived from the YAML configuration and subsequent modifications, including the migration to an externally provided integer `species_id`.

**Goal:** To insert realistic-enough data to test application features, data retrieval, aggregations (materialized views), and filtering (like `topN`).

**Prerequisites:**

*   The Supabase project exists.
*   The database schema (tables listed in Section 4) has been created successfully.
*   Access to the Supabase SQL Editor or another SQL client connected to the database.
*   A predefined list of integer IDs (`species_id`) corresponding to your species data (e.g., 101 for Pig, 102 for Cattle).

## 2. General Principles & Considerations

*   **Order of Insertion:** Respect foreign key relationships. You must insert data into referenced tables **first**. A recommended order is:
    1.  `species` (using your predefined integer IDs)
    2.  `companies`
    3.  `production_sites` (references `companies` and `species`)
    4.  `field_boundaries` (references `companies`)
    5.  All other tables (referencing the above)
*   **Consistency:** Use consistent primary keys (`id` for `companies`, `chr` for `production_sites`, generated `id` for `field_boundaries`) when inserting related data across multiple tables. Using SQL variables within a `DO $$ ... END $$;` block is highly recommended for managing these IDs.
*   **UUID Generation:** While you can pre-generate UUIDs, hardcoding them in scripts can be fragile and lead to errors if hidden characters or encoding issues occur (as seen during testing). For IDs like `field_boundaries.id` where precise values aren't needed beforehand, it's more robust to generate them dynamically within the script using `uuid_generate_v4()`:
    ```sql
    DO $$
    DECLARE
        field1_id uuid := uuid_generate_v4();
        field2_id uuid := uuid_generate_v4();
        -- ... other variables ...
    BEGIN
        INSERT INTO public.field_boundaries (id, ...) VALUES (field1_id, ...);
        INSERT INTO public.field_yearly_data (field_boundary_id, ...) VALUES (field1_id, ...);
        -- ... etc ...
    END $$;
    ```
    *Ensure the `uuid-ossp` extension is enabled (`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`).*
*   **`species_id` Handling:**
    *   When inserting into `public.species`, you **must** provide the predefined integer `species_id` value you have chosen for that species. The database will **not** auto-generate it.
    *   When inserting into related tables (`animal_production_log`, `vet_events`, `animal_transports`, `production_sites`), use the corresponding predefined integer `species_id` for the relevant species. **No lookup is needed** if you know the IDs beforehand.
*   **Schema Verification:** **Crucially, verify your `INSERT` statements against the *actual* current database schema** before running population scripts. Schemas can drift from documentation. Use `\\d table_name` in `psql` or inspect a recent database dump (`pg_dump`) to confirm column names, data types, and nullability constraints. Errors like "column ... does not exist" or "null value in column ... violates not-null constraint" often indicate a mismatch between the script and the live schema.
*   **Data Types:** Ensure inserted values match the column's `data_type`:
    *   `text`, `character varying`: Use single quotes (e.g., `'Svin'`, `'Markvej 1'`).
    *   `integer`, `numeric`: Do not use quotes (e.g., `2023`, `150.5`).
    *   `boolean`: Use `true` or `false` (no quotes).
    *   `date`: Use single quotes in 'YYYY-MM-DD' format (e.g., `'2023-10-27'`).
    *   `timestamp with time zone`: Use single quotes (e.g., `'2023-10-27 10:30:00+02'`) or database functions like `now()`.
    *   `uuid`: Use single quotes with a valid UUID string (if hardcoding) or use generated variables (recommended).
    *   `USER-DEFINED` (Geometry): Use PostGIS functions within single quotes:
        *   Point: `ST_GeomFromText('POINT(longitude latitude)', 4326)`
        *   Polygon: `ST_GeomFromText('POLYGON((lon1 lat1, lon2 lat2, ..., lon1 lat1))', 4326)` (Ensure coordinates use SRID 4326).
*   **NULL Constraints:** Columns marked `is_nullable = NO` **must** have a value provided. Columns marked `is_nullable = YES` can accept `NULL`.
*   **Text Buckets:** For `company_owners.ownership_percentage` (text) and `worker_injury_yearly_counts.injury_count_reported` (text), insert the appropriate descriptive string (e.g., `'50-75%'`, `'<5'`, `'2'`).
*   **Variety:** Insert data covering multiple years (e.g., 2021, 2022, 2023) and with sufficient variety in categorical columns (`crop_name`, `source_category`, `nationality`, etc.) to test aggregations and filtering effectively.
*   **Mock Data Script:** Using a single large SQL script with `DO $$ ... END $$;` blocks to declare variables for IDs is highly efficient for generating consistent mock data.

## 3. Table-by-Table Population Guide

*(Provide realistic values based on your testing needs. Examples use placeholders like `company1_id`, `field1_id`, `species_id_pig`, etc., assuming these are defined or known).*

---

### `species`

*   **Purpose:** Master list of animal species with predefined integer IDs.
*   **Key Columns:** `species_id` (INTEGER PK - **you provide this**), `species_code` (UNIQUE TEXT), `species_name` (TEXT).
*   **Example:**
    ```sql
    INSERT INTO public.species (species_id, species_code, species_name, default_animal_equivalent) VALUES
    (101, 'PIG', 'Svin', 0.3),
    (102, 'CATTLE', 'Kvæg', 1.0),
    (103, 'POULTRY', 'Fjerkræ', 0.014);
    ```

---

### `companies`

*   **Purpose:** Master list of companies.
*   **Key Columns:** `id` (UUID PK), `cvr_number` (UNIQUE), `municipality` (NOT NULL).
*   **Example:**
    ```sql
    INSERT INTO public.companies (id, cvr_number, company_name, address, postal_code, city, municipality, address_geom, advertisement_protection) VALUES
    ('a1a1a1a1-0001-0001-0001-a1a1a1a1a1a1', '10101010', 'Grønne Marker A/S', 'Markvej 1', '6000', 'Kolding', 'Kolding', ST_GeomFromText('POINT(9.47 55.49)', 4326), false);
    ```

---

### `production_sites`

*   **Purpose:** Physical locations/farms associated with a company.
*   **Key Columns:** `chr` (TEXT PK), `company_id` (FK -> `companies.id`), `main_species_id` (FK -> `species.species_id`).
*   **Example:**
    ```sql
    INSERT INTO public.production_sites (chr, company_id, site_name, address, postal_code, city, municipality, location_geom, main_species_id, capacity) VALUES
    ('CHR00101', company1_id, 'Markvej Stald 1', 'Markvej 1', '6000', 'Kolding', 'Kolding', ST_GeomFromText('POINT(9.471 55.491)', 4326), 102, 150); -- Using species_id 102 (Cattle)
    ```

---

### `company_owners`

*   **Purpose:** Ownership structure.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `ownership_percentage` (TEXT bucket).
*   **Example:**
    ```sql
    INSERT INTO public.company_owners (company_id, owner_name, ownership_percentage, ownership_bucket_text, effective_date) VALUES
    (company1_id, 'Peter Jensen Holding ApS', '60-80%', 'Mere end 50%', '2015-01-01'); -- Example with both columns
    ```

---

### `company_leadership`

*   **Purpose:** Directors, board members.
*   **Key Columns:** `company_id` (FK -> `companies.id`).
*   **Example:**
    ```sql
    INSERT INTO public.company_leadership (company_id, person_name, role_title, start_date) VALUES
    (company1_id, 'Peter Jensen', 'Direktør', '2010-05-01');
    ```

---

### `building_footprints`

*   **Purpose:** Geospatial polygons for buildings.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `geom` (geometry).
*   **Example:**
    ```sql
    INSERT INTO public.building_footprints (company_id, geom, building_name, type) VALUES
    (company1_id, ST_GeomFromText('POLYGON((9.470 55.490, 9.471 55.490, 9.471 55.489, 9.470 55.489, 9.470 55.490))', 4326), 'Hovedstald', 'Stald');
    ```

---

### `field_boundaries`

*   **Purpose:** Geospatial polygons for fields.
*   **Key Columns:** `id` (UUID PK), `company_id` (FK -> `companies.id`), `field_identifier` (UNIQUE within company), `geom` (geometry), `area_ha`.
*   **Example:**
    ```sql
    INSERT INTO public.field_boundaries (id, company_id, field_identifier, field_name, geom, area_ha) VALUES
    (field1_id, company1_id, 'C1_F01', 'Nordmarken', ST_GeomFromText('POLYGON((9.46 55.50, ...))', 4326), 25.5);
    ```

---

### `yearly_financials`

*   **Purpose:** Annual financial summary.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `year`.
*   **Example:**
    ```sql
    INSERT INTO public.yearly_financials (company_id, year, revenue, profit, total_subsidies) VALUES
    (company1_id, 2023, 9000000, 1360000, 320000);
    ```

---

### `subsidy_details`

*   **Purpose:** Breakdown of subsidies per type per year.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `year`, `subsidy_type`.
*   **Example:**
    ```sql
    INSERT INTO public.subsidy_details (company_id, year, subsidy_type, amount_dkk) VALUES
    (company1_id, 2023, 'Grundbetaling', 190000);
    ```

---

### `field_yearly_data`

This table stores yearly aggregated or calculated data specific to a field boundary.

**Columns (as per current schema + recent migration):**

*   `id` (uuid, PK, default gen_random_uuid()): Unique identifier for the yearly record.
*   `field_boundary_id` (uuid, FK -> field_boundaries.id, NOT NULL): Links to the specific field boundary.
*   `year` (integer, NOT NULL): The year the data pertains to.
*   `crop_name` (text): Name of the crop grown (e.g., 'Wheat', 'Barley').
*   `area_ha` (numeric, NOT NULL): The area of the field boundary for that year.
*   `fertilizer_amount_kg_ha` (numeric, NULL): Amount of fertilizer applied in kg per hectare. *(Note: This column was recently added via migration)*.
*   `is_organic` (boolean, default false): Indicates if cultivation was organic.
*   `n_leached_kg` (numeric): Calculated nitrogen leached in kg.
*   `pesticide_load_index` (numeric): Calculated pesticide load index.
*   `created_at` (timestamptz, default now(), NOT NULL): Timestamp of record creation.
*   `updated_at` (timestamptz, default now(), NOT NULL): Timestamp of last update.

**Notes:**

*   Columns like `fertilizer_type` and `cultivation_practices` were present in earlier designs but **do not exist** in the current schema based on `database-dump.sql`.
*   The `fertilizer_amount_kg_ha` column was recently added via migration and should be populated.
*   The `area_ha` column must be populated.

**Example:**
```sql
-- Assuming field1_id is a declared UUID variable referencing a field_boundary
INSERT INTO public.field_yearly_data (field_boundary_id, year, crop_name, area_ha, fertilizer_amount_kg_ha, n_leached_kg, pesticide_load_index) VALUES
(field1_id, 2023, 'Winter Wheat', 25.5, 180, 45.2, 1.9); -- Added fertilizer_amount_kg_ha
```

---

### `field_bnbo_areas`

*   **Purpose:** Defines areas *within* a field designated as BNBO and their status/area for a given year.
*   **Key Columns:** `field_boundary_id` (FK -> `field_boundaries.id`), `year`, `bnbo_status`, `area_ha`.
*   **Example:**
    ```sql
    INSERT INTO public.field_bnbo_areas (field_boundary_id, year, bnbo_status, area_ha) VALUES
    (field2_id, 2023, 'dealt_with', 5.0); -- 5 Ha of field2 was BNBO and dealt with in 2023
    ```

---

### `field_wetland_areas`

*   **Purpose:** Defines areas *within* a field designated as potential Wetlands and their status/area for a given year.
*   **Key Columns:** `field_boundary_id` (FK -> `field_boundaries.id`), `year`, `wetlands_status`, `area_ha`.
*   **Example:**
    ```sql
    INSERT INTO public.field_wetland_areas (field_boundary_id, year, wetlands_status, area_ha) VALUES
    (field3_id, 2023, 'not_dealt_with', 3.0); -- 3 Ha of field3 was potential wetland not dealt with in 2023
    ```

---

### `incidents`

*   **Purpose:** Log of specific incidents (e.g., slurry leaks).
*   **Key Columns:** `company_id` (FK -> `companies.id`), `incident_date`, `type`. Note: There is **no `chr`** column in this table according to the current schema.
*   **Example:**
    ```sql
    INSERT INTO public.incidents (company_id, incident_date, type, description) VALUES
    (company3_id, '2022-08-15 10:00:00+02', 'slurry_leak', 'Mindre overløb fra fortank ved CHR00302 under kraftig regn.'); -- Description might mention CHR, but it's not a separate column
    ```

---

### `pesticide_applications`

*   **Purpose:** Log of pesticide use.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `field_boundary_id` (Optional FK), `application_date`, `year`, `pesticide_name`, `risk_category` (TEXT).
*   **Example:**
    ```sql
    INSERT INTO public.pesticide_applications (company_id, field_boundary_id, application_date, year, pesticide_name, risk_category, ha_sprayed, contains_pfas, proximity_water_m) VALUES
    (company1_id, field1_id, '2023-04-15', 2023, 'Propulse', 'Medium', 25.5, false, 50);
    ```

---

### `worker_yearly_summary`

*   **Purpose:** High-level yearly worker aggregates. May be partly redundant if detailed counts are preferred from other tables.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `year`.
*   **Example:**
    ```sql
    INSERT INTO public.worker_yearly_summary (company_id, year, average_employee_count, active_visa_count, injuries_total) VALUES
    (company1_id, 2023, 9, NULL, NULL); -- Use NULL if counts come from other tables
    ```

---

### `employee_monthly_counts`

*   **Purpose:** Trend data for employee numbers.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `month_year` (DATE, e.g., first of month).
*   **Example:**
    ```sql
    INSERT INTO public.employee_monthly_counts (company_id, month_year, employee_count) VALUES
    (company3_id, '2023-12-01', 15);
    ```

---

### `visa_yearly_counts`

*   **Purpose:** Aggregated counts of first-time permits per nationality per year.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `year`, `nationality`, `first_permits_count`.
*   **Example:**
    ```sql
    INSERT INTO public.visa_yearly_counts(company_id, year, nationality, first_permits_count) VALUES
    (company3_id, 2023, 'Rumænien', 4);
    (company3_id, 2023, 'Ukraine', 2);
    ```

---

### `worker_injury_yearly_counts`

*   **Purpose:** Originally intended for aggregated injury counts per year.
*   **Status:** **This table was found *not* to exist** in the current database schema based on `database-dump.sql`. `INSERT` statements targeting this table in older scripts will fail and should be removed or adapted if the schema is changed later.
*   **(Original) Key Columns:** `company_id` (FK -> `companies.id`), `year`, `injury_count_reported` (TEXT).
*   **(Original) Example:**
    ```sql
    -- NOTE: The following will fail unless the table is created.
    -- INSERT INTO public.worker_injury_yearly_counts (company_id, year, injury_count_reported) VALUES
    -- (company3_id, 2023, '2');
    ```

---

### `animal_production_log`

*   **Purpose:** Log of animal production numbers.
*   **Key Columns:** `chr` (FK -> `production_sites.chr`), `year`, `species_id` (FK -> `species.species_id`), `species_name` (**TEXT NOT NULL**). Requires **known integer** `species_id` and the corresponding **species name**.
*   **Example:**
    ```sql
    INSERT INTO public.animal_production_log (chr, year, species_id, species_name, age_group, production_volume_equiv) VALUES
    ('CHR00101', 2023, 102, 'Kvæg', 'Kalve', 70); -- Assuming 102 is Cattle ID and 'Kvæg' is the name
    ```

---

### `site_yearly_summary`

*   **Purpose:** Annual summary data per production site.
*   **Key Columns:** `chr` (FK -> `production_sites.chr`), `year`.
*   **Example:**
    ```sql
    INSERT INTO public.site_yearly_summary (chr, year, owner_cvr, capacity, current_disease_status, production_equiv, antibiotics_ddd, transport_count) VALUES
    ('CHR00101', 2023, '10101010', 150, 'Clear', 170, 54, 46);
    ```

---

### `vet_events`

*   **Purpose:** Log of veterinary events. Note: `outcome` column removed.
*   **Key Columns:** `chr` (FK -> `production_sites.chr`), `event_date`, `event_type`, `species_id` (Optional FK -> `species.species_id`). Requires **known integer** `species_id`.
*   **Example:**
    ```sql
    INSERT INTO public.vet_events (chr, event_date, event_type, description, species_id) VALUES
    ('CHR00301', '2023-10-05 00:00:00+00', 'Sygdomsudbrud', 'PRRS konstateret...', 101); -- Assuming 101 is Pig ID
    ```

---

### `animal_transports`

*   **Purpose:** Log of animal movements.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `transport_date`, `animal_count`, `species_id` (Optional FK -> `species.species_id`), `species_name` (TEXT). Requires **known integer** `species_id` if used. Note: There is **no `chr`** column in this table.
*   **Example:**
    ```sql
    INSERT INTO public.animal_transports (company_id, transport_date, animal_count, species_id, species_name, destination_type, destination_details) VALUES
    (company3_id, '2023-11-25', 280, 101, 'Svin', 'Slagteri', 'Tican Thisted'); -- Assuming 101 is Pig ID and 'Svin' is the name
    ```

---

### `carbon_emission_factors`

*   **Purpose:** Detailed factors and activities for carbon accounting.
*   **Key Columns:** `company_id` (FK -> `companies.id`), `year`, `source_category`, `sub_source`, `co2e_tonnes`.
*   **Example:**
    ```sql
    INSERT INTO public.carbon_emission_factors (company_id, year, source_category, sub_source, activity_data, activity_unit, emission_factor, emission_factor_unit, co2e_tonnes) VALUES
    (company1_id, 2023, 'Energi', 'Diesel', 11000, 'L', 2.68, 'kg CO2e/L', 29.48); -- Example calculation
    ```

---

## 4. Running the Population Script

*   Combine all necessary `INSERT` statements into a single `.sql` file or paste them directly into the Supabase SQL Editor.
*   Using a `DO $$ ... END $$;` block with declared variables for IDs is recommended for consistency, especially when populating data across multiple related tables.
*   Execute the script. Monitor for any errors.

## 5. Refreshing Materialized Views (CRUCIAL)

**After** successfully inserting data into the base tables, the materialized views will **still be empty** or contain stale data. You **must** run the following commands to populate them:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY public.land_use_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.bnbo_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.wetlands_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.environment_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.animal_welfare_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.site_details_summary_ranked;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.site_species_production_ranked;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.animal_transport_weekly_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.carbon_summary;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.carbon_emission_details_yearly;
