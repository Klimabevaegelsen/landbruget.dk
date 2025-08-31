-- Migration: Rename production terminology to capacity terminology
-- This migration updates the database schema to use "capacity" instead of "production"
-- since we're tracking facility capacity, not actual production numbers.

-- 1. Rename the column in site_yearly_summary
ALTER TABLE site_yearly_summary RENAME COLUMN production_equiv TO capacity_count;

-- 2. Drop and recreate site_details_summary_ranked materialized view with capacity terminology
DROP MATERIALIZED VIEW IF EXISTS site_details_summary_ranked CASCADE;

CREATE MATERIALIZED VIEW "public"."site_details_summary_ranked" AS
 WITH "site_data" AS (
         SELECT "sys"."id",
            "sys"."chr",
            "sys"."year",
            "sys"."owner_cvr",
            "sys"."capacity",
            "sys"."current_disease_status",
            "sys"."capacity_count",
            "sys"."antibiotics_ddd",
            "sys"."transport_count",
            "ps"."site_name",
            "ps"."address",
            "ps"."municipality",
            "ps"."company_id",
            "ps"."main_species_code"
           FROM ("public"."site_yearly_summary" "sys"
             JOIN "public"."production_sites" "ps" ON (("sys"."chr" = "ps"."chr")))
        )
 SELECT "sd"."id",
    "sd"."chr",
    "sd"."year",
    "sd"."owner_cvr",
    "sd"."capacity",
    "sd"."current_disease_status",
    "sd"."capacity_count",
    "sd"."antibiotics_ddd",
    "sd"."transport_count",
    "sd"."site_name",
    "sd"."address",
    "sd"."municipality",
    "sd"."company_id",
    "sd"."main_species_code",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."capacity_count" DESC NULLS LAST) AS "rank_municipality_site_production",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."antibiotics_ddd" DESC NULLS LAST) AS "rank_municipality_site_antibiotics",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."transport_count" DESC NULLS LAST) AS "rank_municipality_site_transport"
   FROM "site_data" "sd";

-- Set ownership
ALTER TABLE "public"."site_details_summary_ranked" OWNER TO "postgres";

-- Grant permissions
GRANT SELECT ON "public"."site_details_summary_ranked" TO "anon";
GRANT SELECT ON "public"."site_details_summary_ranked" TO "authenticated";
GRANT SELECT ON "public"."site_details_summary_ranked" TO "service_role";

-- Create indexes for performance
CREATE INDEX "idx_site_details_summary_ranked_chr_year" ON "public"."site_details_summary_ranked" USING "btree" ("chr", "year");
CREATE INDEX "idx_site_details_summary_ranked_company_id" ON "public"."site_details_summary_ranked" USING "btree" ("company_id");
CREATE INDEX "idx_site_details_summary_ranked_municipality_year" ON "public"."site_details_summary_ranked" USING "btree" ("municipality", "year");

-- 3. Drop and recreate animal_welfare_summary materialized view with capacity terminology
DROP MATERIALIZED VIEW IF EXISTS animal_welfare_summary CASCADE;

CREATE MATERIALIZED VIEW "public"."animal_welfare_summary" AS
 WITH "yearly_aw_totals" AS (
         SELECT "c"."id" AS "company_id",
            "c"."municipality",
            "sys"."year",
            "count"(DISTINCT "sys"."chr") AS "site_count",
            "sum"("sys"."capacity_count") AS "total_animal_equivalents",
            "sum"("sys"."antibiotics_ddd") AS "total_ddd_usage",
            "sum"("sys"."transport_count") AS "total_animals_transported"
           FROM (("public"."companies" "c"
             JOIN "public"."production_sites" "ps" ON (("c"."id" = "ps"."company_id")))
             JOIN "public"."site_yearly_summary" "sys" ON (("ps"."chr" = "sys"."chr")))
          GROUP BY "c"."id", "c"."municipality", "sys"."year"
        )
 SELECT "awt"."company_id",
    "awt"."municipality",
    "awt"."year",
    "awt"."site_count",
    "awt"."total_animal_equivalents",
    "awt"."total_ddd_usage",
    "awt"."total_animals_transported",
    COALESCE(("awt"."total_ddd_usage" / NULLIF("awt"."total_animal_equivalents", (0)::numeric)), (0)::numeric) AS "ddd_usage_rate",
    "rank"() OVER (PARTITION BY "awt"."year" ORDER BY "awt"."total_animal_equivalents" DESC NULLS LAST) AS "rank_dk_total_animal_equivalents",
    "rank"() OVER (PARTITION BY "awt"."year", "awt"."municipality" ORDER BY "awt"."total_animal_equivalents" DESC NULLS LAST) AS "rank_municipality_total_animal_equivalents",
    "rank"() OVER (PARTITION BY "awt"."year" ORDER BY "awt"."total_ddd_usage" DESC NULLS LAST) AS "rank_dk_total_ddd_usage",
    "rank"() OVER (PARTITION BY "awt"."year", "awt"."municipality" ORDER BY "awt"."total_ddd_usage" DESC NULLS LAST) AS "rank_municipality_total_ddd_usage",
    "rank"() OVER (PARTITION BY "awt"."year" ORDER BY "awt"."total_animals_transported" DESC NULLS LAST) AS "rank_dk_total_animals_transported",
    "rank"() OVER (PARTITION BY "awt"."year", "awt"."municipality" ORDER BY "awt"."total_animals_transported" DESC NULLS LAST) AS "rank_municipality_total_animals_transported"
   FROM "yearly_aw_totals" "awt";

-- Set ownership
ALTER TABLE "public"."animal_welfare_summary" OWNER TO "postgres";

-- Grant permissions
GRANT SELECT ON "public"."animal_welfare_summary" TO "anon";
GRANT SELECT ON "public"."animal_welfare_summary" TO "authenticated";
GRANT SELECT ON "public"."animal_welfare_summary" TO "service_role";

-- 4. Refresh materialized views to populate with data
REFRESH MATERIALIZED VIEW "public"."site_details_summary_ranked";
REFRESH MATERIALIZED VIEW "public"."animal_welfare_summary";
