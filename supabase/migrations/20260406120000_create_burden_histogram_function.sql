-- Create RPC function for national pesticide burden histogram.
-- Returns B/ha distribution across all fields for a given year,
-- binned in 0.5 B/ha increments from 0 to 12+.

CREATE OR REPLACE FUNCTION get_burden_histogram(p_year integer)
RETURNS TABLE(bin_start numeric, field_count bigint) AS $$
BEGIN
    RETURN QUERY
    WITH field_burdens AS (
        SELECT
            pa.field_uuid,
            CASE
                WHEN MAX(fb.area_ha) > 0
                THEN SUM(COALESCE(pa.total_burden_score, 0)) / MAX(fb.area_ha)
                ELSE 0
            END AS burden_per_ha
        FROM pesticide_applications pa
        INNER JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid
        WHERE pa.year = p_year
        GROUP BY pa.field_uuid
    )
    SELECT
        LEAST(FLOOR(fb2.burden_per_ha / 0.5) * 0.5, 12.0)::numeric AS bin_start,
        COUNT(*)::bigint AS field_count
    FROM field_burdens fb2
    WHERE fb2.burden_per_ha >= 0
    GROUP BY LEAST(FLOOR(fb2.burden_per_ha / 0.5) * 0.5, 12.0)
    ORDER BY bin_start;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_burden_histogram(integer) IS
    'National distribution of pesticide burden (B/ha) per field, binned in 0.5 increments. Source: Bekæmpelsesmiddelstatistik.';

-- Grant access to anon/authenticated for public read
GRANT EXECUTE ON FUNCTION get_burden_histogram(integer) TO anon, authenticated;
