"""Animal-domain helpers for api_export."""

from common.logging_utils import get_pipeline_logger

logger = get_pipeline_logger("api_export.animal")


def create_company_municipality_lookup(conn) -> bool:
    if "production_sites" not in _table_names(conn):
        return False

    columns = _table_columns(conn, "production_sites")
    if not {"company_id", "municipality", "capacity"}.issubset(columns):
        return False

    conn.execute("""
        CREATE OR REPLACE TABLE company_municipality AS
        SELECT
            CAST(company_id AS VARCHAR) AS cvr_number,
            arg_max(
                NULLIF(TRIM(CAST(municipality AS VARCHAR)), ''),
                COALESCE(TRY_CAST(capacity AS DOUBLE), 0)
            ) AS municipality
        FROM production_sites
        WHERE company_id IS NOT NULL
          AND NULLIF(TRIM(CAST(municipality AS VARCHAR)), '') IS NOT NULL
          AND NULLIF(TRIM(CAST(municipality AS VARCHAR)), '') <> 'Unknown'
        GROUP BY 1
    """)
    return True


def create_production_sites(conn) -> bool:
    tables = _table_names(conn)
    if "production_sites" in tables:
        create_company_municipality_lookup(conn)
        return True
    if "chr_properties" not in tables:
        logger.warning("Skipping production site derivation — missing chr_properties")
        return False

    user_cte = ""
    user_join = ""
    user_company_expr = "NULL"
    user_name_expr = "NULL"
    if "chr_property_users" in tables:
        user_cte = """
        , ranked_users AS (
            SELECT
                chr,
                LPAD(TRY_CAST(user_cvr AS BIGINT)::VARCHAR, 8, '0') AS company_id,
                CAST(user_name AS VARCHAR) AS company_name,
                ROW_NUMBER() OVER (
                    PARTITION BY chr
                    ORDER BY
                        CASE WHEN TRY_CAST(user_cvr AS BIGINT) IS NOT NULL THEN 0 ELSE 1 END,
                        CAST(user_name AS VARCHAR)
                ) AS row_num
            FROM chr_property_users
            WHERE chr IS NOT NULL
        )
        """
        user_join = "LEFT JOIN ranked_users ru ON p.chr = ru.chr AND ru.row_num = 1"
        user_company_expr = "ru.company_id"
        user_name_expr = "ru.company_name"

    owner_cte = ""
    owner_join = ""
    owner_company_expr = "NULL"
    owner_name_expr = "NULL"
    if "chr_property_owners" in tables:
        owner_cte = """
        , ranked_owners AS (
            SELECT
                chr,
                LPAD(TRY_CAST(owner_cvr AS BIGINT)::VARCHAR, 8, '0') AS company_id,
                CAST(owner_name AS VARCHAR) AS company_name,
                ROW_NUMBER() OVER (
                    PARTITION BY chr
                    ORDER BY
                        CASE WHEN TRY_CAST(owner_cvr AS BIGINT) IS NOT NULL THEN 0 ELSE 1 END,
                        CAST(owner_name AS VARCHAR)
                ) AS row_num
            FROM chr_property_owners
            WHERE chr IS NOT NULL
        )
        """
        owner_join = "LEFT JOIN ranked_owners ro ON p.chr = ro.chr AND ro.row_num = 1"
        owner_company_expr = "ro.company_id"
        owner_name_expr = "ro.company_name"

    species_capacity_sql = "SELECT NULL::BIGINT AS chr, NULL::VARCHAR AS species_code, 0::DOUBLE AS animals WHERE FALSE"
    if "herd_sizes" in tables:
        species_capacity_sql = """
            SELECT
                chr,
                CAST(species_code AS VARCHAR) AS species_code,
                SUM(COALESCE(count, 0))::DOUBLE AS animals
            FROM herd_sizes
            WHERE chr IS NOT NULL
              AND species_code IS NOT NULL
            GROUP BY 1, 2
        """
    elif "herds" in tables:
        species_capacity_sql = """
            SELECT
                chr,
                CAST(species_code AS VARCHAR) AS species_code,
                COUNT(*)::DOUBLE AS animals
            FROM herds
            WHERE chr IS NOT NULL
              AND species_code IS NOT NULL
            GROUP BY 1, 2
        """

    conn.execute(f"""
        CREATE OR REPLACE TABLE production_sites AS
        WITH properties AS (
            SELECT DISTINCT
                TRY_CAST(chr AS BIGINT) AS chr,
                CAST(address AS VARCHAR) AS address,
                CAST(municipality AS VARCHAR) AS municipality,
                TRY_CAST(latitude AS DOUBLE) AS latitude,
                TRY_CAST(longitude AS DOUBLE) AS longitude
            FROM chr_properties
            WHERE chr IS NOT NULL
        )
        {user_cte}
        {owner_cte}
        , site_companies AS (
            SELECT
                p.chr,
                COALESCE({user_company_expr}, {owner_company_expr}) AS company_id,
                COALESCE({user_name_expr}, {owner_name_expr}) AS company_name,
                p.address,
                p.municipality,
                p.latitude,
                p.longitude
            FROM properties p
            {user_join}
            {owner_join}
        ),
        species_capacity AS (
            {species_capacity_sql}
        ),
        ranked_species AS (
            SELECT
                chr,
                species_code,
                animals,
                ROW_NUMBER() OVER (
                    PARTITION BY chr
                    ORDER BY animals DESC, species_code
                ) AS row_num
            FROM species_capacity
        ),
        site_capacity AS (
            SELECT
                chr,
                SUM(animals) AS capacity
            FROM species_capacity
            GROUP BY 1
        )
        SELECT
            CAST(sc.chr AS VARCHAR) AS chr,
            sc.company_id,
            COALESCE(cap.capacity, 0) AS capacity,
            rs.species_code AS main_species_code,
            sc.municipality,
            sc.latitude,
            sc.longitude,
            COALESCE(NULLIF(TRIM(sc.address), ''), 'CHR ' || CAST(sc.chr AS VARCHAR))
                AS production_site_name,
            sc.company_name
        FROM site_companies sc
        LEFT JOIN site_capacity cap
          ON sc.chr = cap.chr
        LEFT JOIN ranked_species rs
          ON sc.chr = rs.chr AND rs.row_num = 1
        WHERE sc.company_id IS NOT NULL
          AND sc.chr IS NOT NULL
    """)
    create_company_municipality_lookup(conn)
    return True


def site_yearly_summary_count_query(
    production_sites_relation: str, antibiotic_usage_relation: str, herd_sizes_relation: str
) -> str:
    return f"""
        WITH antibiotic_summary AS (
            SELECT
                chr,
                year,
                cvr_number,
                SUM(animal_days) AS total_animal_days,
                SUM(animal_doses) AS total_animal_doses,
                COUNT(*) AS antibiotic_records
            FROM {antibiotic_usage_relation}
            WHERE chr IS NOT NULL
              AND year IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        herd_capacity AS (
            SELECT
                chr,
                SUM(count) AS total_capacity
            FROM {herd_sizes_relation}
            WHERE chr IS NOT NULL
            GROUP BY 1
        ),
        all_chr_years AS (
            SELECT DISTINCT chr, year
            FROM antibiotic_summary
            UNION
            SELECT DISTINCT chr, 2025 AS year
            FROM {herd_sizes_relation}
            WHERE chr IS NOT NULL
              AND chr NOT IN (SELECT DISTINCT chr FROM antibiotic_summary)
        ),
        site_info AS (
            SELECT
                TRY_CAST(chr AS BIGINT) AS chr,
                company_id
            FROM {production_sites_relation}
        )
        SELECT COUNT(*)
        FROM all_chr_years acy
        LEFT JOIN antibiotic_summary ab
          ON acy.chr = ab.chr AND acy.year = ab.year
        LEFT JOIN herd_capacity hc
          ON acy.chr = hc.chr
        LEFT JOIN site_info si
          ON acy.chr = si.chr
        WHERE acy.chr IS NOT NULL
          AND acy.year IS NOT NULL
          AND (
              COALESCE(ab.antibiotic_records, 0) > 0
              OR COALESCE(hc.total_capacity, 0) >= 0
          )
    """


def create_company_animal_summary(conn) -> bool:
    tables = _table_names(conn)
    if "companies" not in tables or "production_sites" not in tables:
        logger.warning("Skipping animal summary — missing companies or production_sites")
        return False

    if "company_municipality" not in tables and create_company_municipality_lookup(conn):
        tables.add("company_municipality")

    municipality_join = ""
    municipality_expr = "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), 'Ukendt kommune')"
    if "company_municipality" in tables:
        municipality_join = "LEFT JOIN company_municipality cm ON cm.cvr_number = ps.company_id"
        municipality_expr = (
            "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), "
            "cm.municipality, 'Ukendt kommune')"
        )

    herd_join = ""
    herd_select = "0 AS total_herds, 0 AS total_animals_registered,"
    if "herd_sizes" in tables:
        herd_join = """
            LEFT JOIN (
                SELECT
                    ps.company_id AS cvr_number,
                    COUNT(DISTINCT hs.herd_number) AS total_herds,
                    SUM(COALESCE(hs.count, 0)) AS total_animals_registered
                FROM production_sites ps
                JOIN herd_sizes hs
                  ON TRY_CAST(ps.chr AS BIGINT) = hs.chr
                GROUP BY 1
            ) herd USING (cvr_number)
        """
        herd_select = """
            COALESCE(herd.total_herds, 0) AS total_herds,
            COALESCE(herd.total_animals_registered, 0) AS total_animals_registered,
        """

    antibiotic_join = ""
    antibiotic_select = "0 AS total_animal_doses, 0 AS total_animal_days"
    if "antibiotic_usage" in tables:
        antibiotic_join = """
            LEFT JOIN (
                SELECT
                    ps.company_id AS cvr_number,
                    SUM(COALESCE(au.animal_doses, 0)) AS total_animal_doses,
                    SUM(COALESCE(au.animal_days, 0)) AS total_animal_days
                FROM production_sites ps
                JOIN antibiotic_usage au
                  ON TRY_CAST(ps.chr AS BIGINT) = au.chr
                GROUP BY 1
            ) antibiotics USING (cvr_number)
        """
        antibiotic_select = """
            COALESCE(antibiotics.total_animal_doses, 0) AS total_animal_doses,
            COALESCE(antibiotics.total_animal_days, 0) AS total_animal_days
        """

    conn.execute(f"""
        CREATE OR REPLACE TABLE company_animal_summary AS
        WITH site_counts AS (
            SELECT
                ps.company_id AS cvr_number,
                c.company_name,
                {municipality_expr} AS municipality,
                COUNT(DISTINCT ps.chr) AS production_site_count,
                SUM(COALESCE(ps.capacity, 0)) AS total_capacity
            FROM production_sites ps
            JOIN companies c
              ON ps.company_id = c.cvr_number::VARCHAR
            {municipality_join}
            GROUP BY 1, 2, 3
        )
        SELECT
            sites.cvr_number,
            sites.company_name,
            sites.municipality,
            sites.production_site_count,
            sites.total_capacity,
            {herd_select}
            {antibiotic_select}
        FROM site_counts sites
        {herd_join}
        {antibiotic_join}
    """)

    return True


def create_company_transport_summaries(conn) -> bool:
    tables = _table_names(conn)
    if "companies" not in tables or "production_sites" not in tables:
        logger.warning("Skipping transport summaries — missing companies or production_sites")
        return False

    if "company_municipality" not in tables and create_company_municipality_lookup(conn):
        tables.add("company_municipality")

    municipality_join = ""
    municipality_expr = "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), 'Ukendt kommune')"
    if "company_municipality" in tables:
        municipality_join = "LEFT JOIN company_municipality cm ON cm.cvr_number = ps.company_id"
        municipality_expr = (
            "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), "
            "cm.municipality, 'Ukendt kommune')"
        )

    raw_queries: list[str] = []

    cattle_transport_query = None
    if "cattle_movements" in tables:
        columns = _table_columns(conn, "cattle_movements")
        sender_column = (
            "reporting_herd_number"
            if "reporting_herd_number" in columns
            else "sender_chr_number"
            if "sender_chr_number" in columns
            else None
        )
        animal_column = (
            "animal_count"
            if "animal_count" in columns
            else "total_animals"
            if "total_animals" in columns
            else None
        )
        year_expr = _movement_year_expr(columns, table_alias="m")
        movement_type_filter = (
            "LOWER(CAST(m.movement_type AS VARCHAR)) = 'outgoing'"
            if "movement_type" in columns
            else "TRUE"
        )
        if sender_column and animal_column and year_expr:
            cattle_transport_query = f"""
                SELECT
                    ps.company_id AS cvr_number,
                    c.company_name,
                    {municipality_expr} AS municipality,
                    '12' AS species_code,
                    {year_expr} AS year,
                    SUM(COALESCE(m.{animal_column}, 0)) AS transported_animals,
                    'cattle_movements' AS source_dataset
                FROM cattle_movements m
                JOIN production_sites ps
                  ON TRY_CAST(ps.chr AS BIGINT) = TRY_CAST(m.{sender_column} AS BIGINT)
                JOIN companies c
                  ON ps.company_id = c.cvr_number::VARCHAR
                {municipality_join}
                WHERE {year_expr} IS NOT NULL
                  AND {movement_type_filter}
                GROUP BY 1, 2, 3, 4, 5, 7
            """

    if "transportation_analysis" in tables:
        columns = _table_columns(conn, "transportation_analysis")
        sender_column = (
            "sender_chr_number"
            if "sender_chr_number" in columns
            else "reporting_herd_number"
            if "reporting_herd_number" in columns
            else None
        )
        animal_column = (
            "total_animals"
            if "total_animals" in columns
            else "animal_count"
            if "animal_count" in columns
            else None
        )
        species_column = "species_code" if "species_code" in columns else None
        year_expr = _movement_year_expr(columns, table_alias="t")
        deleted_filter = (
            "COALESCE(t.is_deleted, false) = false" if "is_deleted" in columns else "TRUE"
        )
        if sender_column and animal_column and species_column and year_expr:
            raw_queries.append(f"""
                SELECT
                    ps.company_id AS cvr_number,
                    c.company_name,
                    {municipality_expr} AS municipality,
                    CAST(t.{species_column} AS VARCHAR) AS species_code,
                    {year_expr} AS year,
                    SUM(COALESCE(t.{animal_column}, 0)) AS transported_animals,
                    'transportation_analysis' AS source_dataset
                FROM transportation_analysis t
                JOIN production_sites ps
                  ON TRY_CAST(ps.chr AS BIGINT) = TRY_CAST(t.{sender_column} AS BIGINT)
                JOIN companies c
                  ON ps.company_id = c.cvr_number::VARCHAR
                {municipality_join}
                WHERE {deleted_filter}
                  AND {year_expr} IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, 7
            """)

    if not raw_queries and "pig_movements" in tables:
        columns = _table_columns(conn, "pig_movements")
        if {"sender_chr_number", "total_animals"}.issubset(columns):
            year_expr = _movement_year_expr(columns, table_alias="m")
            deleted_filter = (
                "COALESCE(m.is_deleted, false) = false" if "is_deleted" in columns else "TRUE"
            )
            if year_expr:
                raw_queries.append(f"""
                    SELECT
                        ps.company_id AS cvr_number,
                        c.company_name,
                        {municipality_expr} AS municipality,
                        '15' AS species_code,
                        {year_expr} AS year,
                        SUM(COALESCE(m.total_animals, 0)) AS transported_animals,
                        'pig_movements' AS source_dataset
                    FROM pig_movements m
                    JOIN production_sites ps
                      ON TRY_CAST(ps.chr AS BIGINT) = TRY_CAST(m.sender_chr_number AS BIGINT)
                    JOIN companies c
                      ON ps.company_id = c.cvr_number::VARCHAR
                    {municipality_join}
                    WHERE {deleted_filter}
                      AND {year_expr} IS NOT NULL
                    GROUP BY 1, 2, 3, 4, 5, 7
                """)

    if cattle_transport_query:
        raw_queries.append(cattle_transport_query)

    if not raw_queries:
        logger.warning("Skipping transport summaries — no compatible movement tables found")
        return False

    union_sql = " UNION ALL ".join(raw_queries)
    conn.execute(f"""
        CREATE OR REPLACE TABLE company_transport_species_yearly AS
        WITH raw AS ({union_sql}),
        source_preference AS (
            SELECT
                species_code,
                BOOL_OR(source_dataset = 'cattle_movements') AS has_cattle_movements
            FROM raw
            GROUP BY 1
        )
        SELECT
            raw.cvr_number,
            raw.company_name,
            raw.municipality,
            raw.species_code,
            raw.year,
            SUM(raw.transported_animals) AS transported_animals
        FROM raw
        JOIN source_preference sp
          ON raw.species_code = sp.species_code
        WHERE raw.species_code IS NOT NULL
          AND raw.year IS NOT NULL
          AND NOT (
              raw.species_code = '12'
              AND sp.has_cattle_movements
              AND raw.source_dataset <> 'cattle_movements'
          )
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1, 4, 5
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE company_animal_transport_yearly AS
        SELECT
            cvr_number,
            year,
            SUM(transported_animals) AS transported_animals
        FROM company_transport_species_yearly
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    return True


def _table_columns(conn, table_name: str) -> set[str]:
    try:
        return {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
    except Exception:
        return set()


def _table_names(conn) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }


def _movement_year_expr(columns: set[str], *, table_alias: str) -> str | None:
    if "movement_date" in columns:
        return f"CAST(EXTRACT(YEAR FROM TRY_CAST({table_alias}.movement_date AS DATE)) AS INTEGER)"
    if "source_period_end" in columns:
        return (
            f"CAST(EXTRACT(YEAR FROM TRY_CAST({table_alias}.source_period_end AS DATE)) AS INTEGER)"
        )
    if "source_period_start" in columns:
        return f"CAST(EXTRACT(YEAR FROM TRY_CAST({table_alias}.source_period_start AS DATE)) AS INTEGER)"
    return None
