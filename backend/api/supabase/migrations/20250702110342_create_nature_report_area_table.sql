CREATE TABLE IF NOT EXISTS "public"."nature_report_category" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "report" "text" NOT NULL,
    "category" "text" NOT NULL,
    "score_biodiversity" "numeric" NOT NULL,
    "score_climate" "numeric" NOT NULL,
    "score_nitrogen" "numeric" NOT NULL,
    "score_recreation" "numeric" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

CREATE TABLE IF NOT EXISTS "public"."nature_report_area" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "nature_report_category" "uuid" NOT NULL,
    "geom" "public"."geometry"(Polygon,4326) NOT NULL,
    "area_ha" "numeric" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

ALTER TABLE "public"."nature_report_area" OWNER TO "postgres";


ALTER TABLE ONLY "public"."nature_report_area"
    ADD CONSTRAINT "nature_report_area_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."nature_report_category"
    ADD CONSTRAINT "nature_report_category_pkey" PRIMARY KEY ("id");


CREATE INDEX "idx_nature_report_area_nature_report_category" ON "public"."nature_report_area" USING "btree" ("nature_report_category");

CREATE INDEX "idx_nature_report_are_geom" ON "public"."nature_report_area" USING "gist" ("geom");


ALTER TABLE ONLY "public"."nature_report_area"
    ADD CONSTRAINT "nature_report_area_nature_report_category_fkey" FOREIGN KEY ("nature_report_category") REFERENCES "public"."nature_report_category"("id") ON DELETE CASCADE;
