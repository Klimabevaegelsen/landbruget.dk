// Municipality Rankings API Endpoint
// Returns municipality-level rankings across different categories

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

interface MunicipalityRankingRequest {
  category?:
    | "land_use"
    | "production"
    | "pesticide_burden"
    | "pesticide_pfas"
    | "pesticide_glyphosate"
    | "antibiotic_usage"
    | "environmental"
    | "worker_safety"
    | "incidents"
    | "organic_farming"
    | "all";
  year?: number;
  limit?: number;
  sort_by?: string;
  sort_direction?: "asc" | "desc";
}

interface MunicipalityRanking {
  municipality: string;
  rank: number;
  value: number;
  metric: string;
  additional_data?: Record<string, any>;
}

interface MunicipalityRankingResponse {
  rankings: {
    land_use?: MunicipalityRanking[];
    production?: MunicipalityRanking[];
    pesticide_burden?: MunicipalityRanking[];
    pesticide_pfas?: MunicipalityRanking[];
    pesticide_glyphosate?: MunicipalityRanking[];
    antibiotic_usage?: MunicipalityRanking[];
    environmental?: MunicipalityRanking[];
    worker_safety?: MunicipalityRanking[];
    incidents?: MunicipalityRanking[];
    organic_farming?: MunicipalityRanking[];
  };
  metadata: {
    year: number;
    total_municipalities: number;
    generated_at: string;
    categories_included: string[];
  };
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    // Parse request parameters
    const url = new URL(req.url);
    const params: MunicipalityRankingRequest = {
      category: (url.searchParams.get("category") as any) || "all",
      year: parseInt(url.searchParams.get("year") || "2024"),
      limit: parseInt(url.searchParams.get("limit") || "50"),
      sort_by: url.searchParams.get("sort_by") || "total_area_ha",
      sort_direction: (url.searchParams.get("sort_direction") as any) || "desc",
    };

    // Initialize Supabase client
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseAnonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseAnonKey);

    const response: MunicipalityRankingResponse = {
      rankings: {},
      metadata: {
        year: params.year!,
        total_municipalities: 0,
        generated_at: new Date().toISOString(),
        categories_included: [],
      },
    };

    // Land Use Rankings
    if (params.category === "all" || params.category === "land_use") {
      const landUseRankings = await getLandUseRankings(supabase, params);
      response.rankings.land_use = landUseRankings;
      response.metadata.categories_included.push("land_use");
    }

    // Production Rankings
    if (params.category === "all" || params.category === "production") {
      const productionRankings = await getProductionRankings(supabase, params);
      response.rankings.production = productionRankings;
      response.metadata.categories_included.push("production");
    }

    // Pesticide Burden Rankings
    if (params.category === "all" || params.category === "pesticide_burden") {
      const pesticideRankings = await getPesticideBurdenRankings(
        supabase,
        params
      );
      response.rankings.pesticide_burden = pesticideRankings;
      response.metadata.categories_included.push("pesticide_burden");
    }

    // PFAS Pesticide Rankings
    if (params.category === "all" || params.category === "pesticide_pfas") {
      const pfasRankings = await getPFASPesticideRankings(supabase, params);
      response.rankings.pesticide_pfas = pfasRankings;
      response.metadata.categories_included.push("pesticide_pfas");
    }

    // Glyphosate Pesticide Rankings
    if (
      params.category === "all" ||
      params.category === "pesticide_glyphosate"
    ) {
      const glyphosateRankings = await getGlyphosatePesticideRankings(
        supabase,
        params
      );
      response.rankings.pesticide_glyphosate = glyphosateRankings;
      response.metadata.categories_included.push("pesticide_glyphosate");
    }

    // Antibiotic Usage Rankings
    if (params.category === "all" || params.category === "antibiotic_usage") {
      const antibioticRankings = await getAntibioticUsageRankings(
        supabase,
        params
      );
      response.rankings.antibiotic_usage = antibioticRankings;
      response.metadata.categories_included.push("antibiotic_usage");
    }

    // Environmental Rankings (nitrogen leaching)
    if (params.category === "all" || params.category === "environmental") {
      const environmentalRankings = await getEnvironmentalRankings(
        supabase,
        params
      );
      response.rankings.environmental = environmentalRankings;
      response.metadata.categories_included.push("environmental");
    }

    // Worker Safety and Incidents Rankings
    if (params.category === "all" || params.category === "worker_safety") {
      const workerSafetyRankings = await getWorkerSafetyRankings(
        supabase,
        params
      );
      response.rankings.worker_safety = workerSafetyRankings;
      response.metadata.categories_included.push("worker_safety");
    }

    // Incidents Rankings
    if (params.category === "all" || params.category === "incidents") {
      const incidentRankings = await getIncidentRankings(supabase, params);
      response.rankings.incidents = incidentRankings;
      response.metadata.categories_included.push("incidents");
    }

    // Organic Farming Rankings
    if (params.category === "all" || params.category === "organic_farming") {
      const organicRankings = await getOrganicFarmingRankings(supabase, params);
      response.rankings.organic_farming = organicRankings;
      response.metadata.categories_included.push("organic_farming");
    }

    // Calculate total municipalities (use land use as baseline)
    if (response.rankings.land_use) {
      response.metadata.total_municipalities =
        response.rankings.land_use.length;
    }

    return new Response(JSON.stringify(response), {
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json",
      },
    });
  } catch (error) {
    console.error("Municipality rankings error:", error);
    return new Response(
      JSON.stringify({
        error: "Internal server error",
        message: error.message,
      }),
      {
        status: 500,
        headers: {
          ...corsHeaders,
          "Content-Type": "application/json",
        },
      }
    );
  }
});

async function getLandUseRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("v_municipality_land_use_summary")
    .select("*")
    .eq("year", params.year) // Use requested year
    .order("total_area_ha", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_area_ha,
    metric: "total_agricultural_area_ha",
    additional_data: {
      total_fields: row.total_fields,
      avg_field_size: row.avg_field_size,
      organic_percentage: row.organic_percentage || 0,
      unique_companies: row.unique_companies,
      unique_crops: row.unique_crops,
    },
  }));
}

async function getProductionRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("municipality_animal_production_summary")
    .select("*")
    .order("total_animal_capacity", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_animal_capacity,
    metric: "total_animal_capacity",
    additional_data: {
      total_production_sites: row.total_production_sites,
      avg_site_capacity: Math.round(row.avg_site_capacity || 0),
      unique_companies: row.unique_companies,
    },
  }));
}

// Pesticide Burden Rankings (total burden)
async function getPesticideBurdenRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("municipality_pesticide_summary")
    .select("*")
    .eq("application_year", params.year || 2023) // Use most recent pesticide data
    .order("total_belastning", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_belastning || 0,
    metric: "total_pesticide_burden",
    additional_data: {
      total_applications: row.total_applications || 0,
      total_treated_area_ha: row.total_treated_area_ha || 0,
      health_burden: row.total_health_burden || 0,
      unique_companies: row.unique_companies || 0,
      glyphosate_burden: row.glyphosate_burden || 0,
      pfas_burden: row.pfas_burden || 0,
    },
  }));
}

// PFAS Pesticide Rankings
async function getPFASPesticideRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("municipality_pesticide_summary")
    .select("*")
    .eq("application_year", params.year || 2023)
    .order("pfas_belastning", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.pfas_belastning || 0,
    metric: "pfas_pesticide_burden",
    additional_data: {
      pfas_applications: row.pfas_applications || 0,
      total_pesticide_burden: row.total_pesticide_burden || 0,
      unique_companies: row.unique_companies || 0,
      total_treated_area_ha: row.total_treated_area_ha || 0,
    },
  }));
}

// Glyphosate Pesticide Rankings
async function getGlyphosatePesticideRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("municipality_pesticide_summary")
    .select("*")
    .eq("application_year", params.year || 2023)
    .order("glyphosate_belastning", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.glyphosate_belastning || 0,
    metric: "glyphosate_pesticide_burden",
    additional_data: {
      glyphosate_applications: row.glyphosate_applications || 0,
      total_pesticide_burden: row.total_pesticide_burden || 0,
      unique_companies: row.unique_companies || 0,
      total_treated_area_ha: row.total_treated_area_ha || 0,
    },
  }));
}

// Antibiotic Usage Rankings (using production sites with antibiotic data)
async function getAntibioticUsageRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("v_municipality_production_summary")
    .select("*")
    .order("total_antibiotics_ddd_2024", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_antibiotics_ddd_2024 || 0,
    metric: "total_antibiotic_ddd_usage",
    additional_data: {
      total_production_sites: row.total_sites || 0,
      sites_with_antibiotics: row.sites_with_2024_data || 0,
      avg_antibiotics_per_site: row.avg_antibiotics_ddd_2024 || 0,
      total_animal_capacity: row.total_capacity || 0,
      unique_companies: row.unique_companies || 0,
    },
  }));
}

// Environmental Rankings (nitrogen leaching)
async function getEnvironmentalRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("v_municipality_land_use_summary")
    .select("*")
    .eq("year", params.year || 2024)
    .order("avg_n_leached_kg", { ascending: false, nullsFirst: false })
    .limit(params.limit);

  if (error) throw error;

  // Filter out null values
  const validData = data.filter((row: any) => row.avg_n_leached_kg != null);

  return validData.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.avg_n_leached_kg || 0,
    metric: "avg_nitrogen_leaching_kg",
    additional_data: {
      total_fields: row.total_fields || 0,
      total_area_ha: row.total_area_ha || 0,
      avg_pesticide_load: row.avg_pesticide_load_index || 0,
      organic_percentage: row.organic_percentage || 0,
      unique_companies: row.unique_companies || 0,
    },
  }));
}

// Worker Safety Rankings (based on incident data)
async function getWorkerSafetyRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  // Get incident counts by municipality (via company location)
  const { data, error } = await supabase.rpc(
    "get_municipality_incident_summary",
    {
      target_year: params.year || 2024,
    }
  );

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_incidents || 0,
    metric: "total_workplace_incidents",
    additional_data: {
      companies_with_incidents: row.companies_with_incidents || 0,
      incident_types: row.incident_types || 0,
      severe_incidents: row.severe_incidents || 0,
      incident_rate_per_company: row.incident_rate_per_company || 0,
    },
  }));
}

// Incidents Rankings (separate from worker safety)
async function getIncidentRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  // Get all incident data by municipality
  const { data, error } = await supabase
    .from("incidents")
    .select(
      `
      *,
      companies!inner(municipality)
    `
    )
    .eq("year", params.year || 2024);

  if (error) throw error;

  // Group by municipality
  const municipalityIncidents = data.reduce((acc: any, incident: any) => {
    const municipality = incident.companies.municipality;
    if (!municipality) return acc;

    if (!acc[municipality]) {
      acc[municipality] = {
        municipality,
        total_incidents: 0,
        severity_high: 0,
        severity_medium: 0,
        severity_low: 0,
        unique_companies: new Set(),
        incident_types: new Set(),
      };
    }

    acc[municipality].total_incidents++;
    acc[municipality].unique_companies.add(incident.company_id);
    acc[municipality].incident_types.add(incident.type);

    if (incident.severity === "high") acc[municipality].severity_high++;
    else if (incident.severity === "medium")
      acc[municipality].severity_medium++;
    else acc[municipality].severity_low++;

    return acc;
  }, {});

  // Convert to array and sort
  const rankings = Object.values(municipalityIncidents)
    .map((data: any) => ({
      ...data,
      unique_companies: data.unique_companies.size,
      incident_types: data.incident_types.size,
    }))
    .sort((a: any, b: any) => b.total_incidents - a.total_incidents)
    .slice(0, params.limit);

  return rankings.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_incidents,
    metric: "total_incidents",
    additional_data: {
      companies_with_incidents: row.unique_companies,
      incident_types_count: row.incident_types,
      high_severity: row.severity_high,
      medium_severity: row.severity_medium,
      low_severity: row.severity_low,
    },
  }));
}

// Organic Farming Rankings
async function getOrganicFarmingRankings(
  supabase: any,
  params: MunicipalityRankingRequest
): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from("v_municipality_land_use_summary")
    .select("*")
    .eq("year", params.year || 2024)
    .order("organic_percentage", { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.organic_percentage || 0,
    metric: "organic_farming_percentage",
    additional_data: {
      organic_area_ha: row.organic_area_ha || 0,
      total_area_ha: row.total_area_ha || 0,
      total_fields: row.total_fields || 0,
      unique_companies: row.unique_companies || 0,
      avg_field_size: row.avg_field_size || 0,
    },
  }));
}
