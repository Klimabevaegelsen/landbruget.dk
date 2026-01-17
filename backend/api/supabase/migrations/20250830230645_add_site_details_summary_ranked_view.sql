-- Migration: Add missing site_details_summary_ranked materialized view
-- This view is required for site-level KPI data and production site details
-- Fixes empty "Site nøgletal & placering" and "Site basis info" sections

-- Create the materialized view that joins site yearly data with production sites
CREATE MATERIALIZED VIEW "public"."site_details_summary_ranked" AS
 WITH "site_data" AS (
         SELECT "sys"."id",
            "sys"."chr",
            "sys"."year",
            "sys"."owner_cvr",
            "sys"."capacity",
            "sys"."current_disease_status",
            "sys"."production_equiv",
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
    "sd"."production_equiv",
    "sd"."antibiotics_ddd",
    "sd"."transport_count",
    "sd"."site_name",
    "sd"."address",
    "sd"."municipality",
    "sd"."company_id",
    "sd"."main_species_code",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."production_equiv" DESC NULLS LAST) AS "rank_municipality_site_production",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."antibiotics_ddd" DESC NULLS LAST) AS "rank_municipality_site_antibiotics",
    "rank"() OVER (PARTITION BY "sd"."year", "sd"."municipality" ORDER BY "sd"."transport_count" DESC NULLS LAST) AS "rank_municipality_site_transport"
   FROM "site_data" "sd";

-- Set ownership
ALTER TABLE "public"."site_details_summary_ranked" OWNER TO "postgres";

-- Create indexes for performance
CREATE INDEX "idx_site_details_summary_ranked_chr_year" ON "public"."site_details_summary_ranked" USING "btree" ("chr", "year");
CREATE INDEX "idx_site_details_summary_ranked_company_id" ON "public"."site_details_summary_ranked" USING "btree" ("company_id");
CREATE INDEX "idx_site_details_summary_ranked_municipality" ON "public"."site_details_summary_ranked" USING "btree" ("municipality");

-- Grant permissions
GRANT ALL ON TABLE "public"."site_details_summary_ranked" TO "anon";
GRANT ALL ON TABLE "public"."site_details_summary_ranked" TO "authenticated";
GRANT ALL ON TABLE "public"."site_details_summary_ranked" TO "service_role";

-- Note: The materialized view will be populated when site_yearly_summary has data
-- To refresh: REFRESH MATERIALIZED VIEW site_details_summary_ranked;
