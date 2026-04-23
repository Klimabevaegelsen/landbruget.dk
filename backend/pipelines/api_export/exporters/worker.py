"""Worker-domain SQL helpers for api_export."""


def worker_yearly_summary_count_query(
    employment_relation: str, work_permits_relation: str, incidents_relation: str
) -> str:
    return f"""
        WITH employee_yearly AS (
            SELECT
                CAST(company_id AS VARCHAR) AS company_id,
                EXTRACT(year FROM month_year)::INTEGER AS year
            FROM {employment_relation}
            WHERE company_id IS NOT NULL
              AND month_year IS NOT NULL
              AND employee_count > 0
            GROUP BY 1, 2
        ),
        visa_yearly AS (
            SELECT
                CAST(company_id AS VARCHAR) AS company_id,
                year
            FROM {work_permits_relation}
            WHERE company_id IS NOT NULL
              AND year IS NOT NULL
              AND first_permits_count > 0
            GROUP BY 1, 2
        ),
        injury_yearly AS (
            SELECT
                CAST(company_id AS VARCHAR) AS company_id,
                EXTRACT(year FROM date)::INTEGER AS year
            FROM {incidents_relation}
            WHERE company_id IS NOT NULL
              AND date IS NOT NULL
            GROUP BY 1, 2
        ),
        all_company_years AS (
            SELECT company_id, year FROM employee_yearly
            UNION
            SELECT company_id, year FROM visa_yearly
            UNION
            SELECT company_id, year FROM injury_yearly
        )
        SELECT COUNT(*) FROM all_company_years
    """
