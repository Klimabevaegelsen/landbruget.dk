"""Governance-domain SQL helpers for api_export."""


def _people_roles_cte(persons_relation: str) -> str:
    return f"""
        WITH leadership_flattened AS (
            SELECT
                cvr_number,
                person_data_json,
                idx AS leadership_idx
            FROM {persons_relation}
            CROSS JOIN generate_series(
                0::BIGINT,
                CASE
                    WHEN json_array_length(json_extract(person_data_json, '$.leadership')) > 0
                    THEN (json_array_length(json_extract(person_data_json, '$.leadership')) - 1)::BIGINT
                    ELSE 0::BIGINT
                END
            ) AS t(idx)
            WHERE json_extract(person_data_json, '$.leadership') IS NOT NULL
              AND json_array_length(json_extract(person_data_json, '$.leadership')) > 0
        ),
        roles AS (
            SELECT
                cvr_number,
                json_extract_string(
                    person_data_json,
                    '$.leadership[' || leadership_idx ||
                    '].organization.member_data[0].attributter[0].vaerdier[0].vaerdi'
                ) AS role
            FROM leadership_flattened
            WHERE json_extract(
                person_data_json,
                '$.leadership[' || leadership_idx || '].person.unit_number'
            ) IS NOT NULL
        )
    """


def leadership_count_query(persons_relation: str) -> str:
    return f"""
        {_people_roles_cte(persons_relation)}
        SELECT COUNT(*)
        FROM roles
        WHERE UPPER(role) IN (
            'DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND',
            'BESTYRELSESMEDLEM', 'LEDER', 'INTERESSENTER'
        )
    """


def owner_count_query(persons_relation: str) -> str:
    return f"""
        {_people_roles_cte(persons_relation)}
        SELECT COUNT(*)
        FROM roles
        WHERE UPPER(role) IN ('REEL EJER', 'INTERESSENTER')
    """
