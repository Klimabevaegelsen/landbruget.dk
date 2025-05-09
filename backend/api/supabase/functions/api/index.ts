import { serve } from 'std/http/server';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import yaml from 'js-yaml';
// --- Configuration ---
// const YAML_CONFIG_URL = 'https://raw.githubusercontent.com/Klimabevaegelsen/landbruget.dk/main/backend/api/supabase/functions/api/config.yaml'; // Removed
// const CONFIG_CACHE_DURATION_MS = 60 * 60 * 1000; // 1 hour cache // Removed
// List of tables/views where municipality filter is expected/valid in KPIs/Summaries
const TABLES_WITH_MUNICIPALITY_SUMMARY = [
  'land_use_summary',
  'bnbo_summary',
  'wetlands_summary',
  'environment_summary',
  'animal_welfare_summary',
  'carbon_summary',
  'site_details_summary_ranked' // Include if ranks depend on municipality
];
// --- In-memory Cache ---
let cachedConfig: any = null;
// let lastFetchTimestamp = 0; // Removed
// --- Helper: Fetch and Cache YAML Config ---
async function getYamlConfig() {
  // const now = Date.now(); // Removed
  // if (cachedConfig && now - lastFetchTimestamp < CONFIG_CACHE_DURATION_MS) { // Removed
  //   console.log("Serving cached config."); // Removed
  //   return cachedConfig; // Removed
  // } // Removed

  // New logic to read local file
  if (cachedConfig) {
    console.log("Serving in-memory config.");
    return cachedConfig;
  }

  console.log("Reading local config.yaml");
  try {
    // Assuming config.yaml is in the same directory as index.ts when deployed
    const yamlText = await Deno.readTextFile('./config.yaml');
    cachedConfig = yaml.load(yamlText);
    console.log("Config read and cached successfully.");
    return cachedConfig;
  } catch (error) {
    console.error("Error reading or parsing local config.yaml:", error);
    // If there was a previously cached version (e.g. from a prior successful read in a long-running instance, though less likely in serverless)
    // This part might be less relevant now as we expect the file to always be present.
    // Consider if an error here should be fatal. For now, it re-throws.
    throw error;
  }
}
// Define type for company details
type CompanyInfo = {
  id: string;
  municipality: string;
  cvr_number: string;
};
// --- Helper: Get Company Details (Lookup by ID - UUID) ---
async function getCompanyDetails(supabase: SupabaseClient, companyId: string): Promise<CompanyInfo | null> {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (!uuidRegex.test(companyId)) {
    console.warn(`Received invalid format for company ID: ${companyId}`);
    return null;
  }
  const { data, error } = await supabase.from('companies').select('id, municipality, cvr_number').eq('id', companyId).maybeSingle();
  if (error) {
    console.error(`Error fetching company details for ID ${companyId}:`, error);
    throw new Error(`Database error fetching company details.`);
  }
  return data || null;
}
// Define a basic type for chart data structures
type ChartData = {
  xAxis?: { label: string; values?: any[] }; // Made values optional
  yAxis?: { label: string; values?: any[] }; 
  series?: { name: string; data: any[]; type?: string; yAxis?: string }[];
};
// Define a general type for component processing results
type ComponentResult = {
  _key: string;
  _type: string;
  title?: string;
  error?: string;
  // Add specific fields from different component types as optional
  items?: any[]; // For infoCard
  rows?: any[]; // For dataGrid
  columns?: any[]; // For dataGrid
  allowFiltering?: boolean; // For dataGrid
  isCollapsible?: boolean; // For collapsibleDataGrid
  kpis?: any[]; // For kpiGroup
  data?: ChartData | any; // For charts, maps, timeline etc.
  config?: any; // For timeline
  sections?: any[]; // For iteratedSection
  iterationConfig?: any; // For iteratedSection
  [key: string]: any; // Allow other properties
};
// Define a type for DataGrid results specifically
type DataGridResult = {
  rows: any[];
  columns: any[];
  allowFiltering: boolean;
  error?: string;
  isCollapsible?: boolean; // Optional property
};
// Define a type for map layer results
type MapLayerResult = {
  name: string;
  type: 'geojson' | 'error';
  style?: string;
  data?: any; // GeoJSON FeatureCollection
  error?: string;
}
// --- Helper: Get Latest Year (Generalized) ---
async function getLatestYearForCompany(supabase: SupabaseClient, sourceTable: string, companyId: string, yearColumn = 'year', filterContext: Record<string, any> | null = {}) {
  // Refined to potentially filter by CHR if provided
  let query = supabase.from(sourceTable).select(yearColumn, {
    count: 'exact',
    head: false
  });
  // Apply context filter (CHR for site-specific latest year)
  if (filterContext?.chr && await tableHasColumn(supabase, sourceTable, 'chr')) {
    query = query.eq('chr', filterContext.chr);
    console.log(`getLatestYearForCompany: Filtering by CHR ${filterContext.chr} for ${sourceTable}`);
  } else if (await tableHasColumn(supabase, sourceTable, 'company_id')) {
    // Fallback to company_id if CHR not applicable/available
    query = query.eq('company_id', companyId);
  } else {
    // If no company_id or CHR, we can't reliably get the latest year *for this entity*
    console.warn(`getLatestYearForCompany: Cannot filter ${sourceTable} by companyId or context CHR. Finding global latest year (may be inaccurate).`);
  }
  const { data, error } = await query.order(yearColumn, {
    ascending: false
  }).limit(1).maybeSingle();
  if (error) {
    console.warn(`Could not determine latest year for ${sourceTable} (Company ${companyId}, Context ${JSON.stringify(filterContext)}):`, error.message);
    return null;
  }
  // Line 84 fix: Assume data is Record<string, any> or cast
  const resultData = data as Record<string, any> | null;
  const latestYear = resultData ? resultData[yearColumn] : null;
  console.log(`getLatestYearForCompany: Determined latest year for ${sourceTable} (Company ${companyId}, Context ${JSON.stringify(filterContext)}) as: ${latestYear}`);
  return latestYear;
}
// --- Helper: Check if table has a column (simple check, needs improvement/caching) ---
const columnExistenceCache = new Map();
async function tableHasColumn(supabase: SupabaseClient, tableName: string, columnName: string): Promise<boolean> {
  const cacheKey = `${tableName}.${columnName}`;
  if (columnExistenceCache.has(cacheKey)) {
    return columnExistenceCache.get(cacheKey);
  }
  try {
    // console.log(`Checking column existence: ${cacheKey}`);
    const { error } = await supabase.from(tableName).select(columnName, {
      count: 'exact',
      head: true
    }).limit(0);
    const exists = !error || error && !error.message.includes("does not exist") && !error.message.includes("relation") && !error.message.includes("missing FROM-clause");
    // console.log(`Column check result for ${cacheKey}: ${exists} (Error: ${error?.message})`);
    columnExistenceCache.set(cacheKey, exists);
    return exists;
  } catch (e) {
    console.warn(`Error checking column existence for ${cacheKey}:`, e);
    columnExistenceCache.set(cacheKey, false);
    return false;
  }
}
// --- Data Processing Functions ---
async function processInfoCard(supabase: SupabaseClient, companyId: string, municipality: string, params: any, context: Record<string, any> | null) {
  const { source, record } = params;
  const { mappings, filter: recordFilter } = record || {};
  if (!source || !mappings) throw new Error(`Invalid config for infoCard: missing source or mappings.`);
  console.log(`processInfoCard: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(recordFilter)}`);
  const selectColumns = mappings.map((m: any) => m.column).join(',');
  let query = supabase.from(source).select(selectColumns);
  // Apply company_id or primary ID filter
  if (source === 'companies') {
    query = query.eq('id', companyId);
  } else if (await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
  }
  // Apply specific record filters (potentially using context)
  if (recordFilter) {
    for(const key in recordFilter){
      let filterValue = recordFilter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processInfoCard: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processInfoCard: Could not resolve context for filter ${key}=${filterValue}`);
          return {
            items: [],
            error: `Unresolved context for filter ${key}`
          };
        }
      }
      // Apply the filter (now resolved)
      if (filterValue === 'latest') {
        const latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context); // Pass context for site-specific latest year
        if (latestYear) {
          query = query.eq('year', latestYear);
        } else {
          console.warn(`processInfoCard: Cannot apply 'latest' filter for ${source}, no year found for context ${JSON.stringify(context)}.`);
          // Decide behavior: maybe return empty or allow query without year? Returning empty for safety.
          return {
            items: []
          };
        }
      } else {
        query = query.eq(key, filterValue);
      }
    }
  }
  const { data, error } = await query.maybeSingle();
  if (error) {
    console.error(`Error fetching data for infoCard (${source}):`, error);
    return {
      items: [],
      error: `Database error: ${error.message}`
    };
  }
  if (!data) {
    console.log(`No data found for infoCard (${source}) with applied filters.`);
    return {
      items: []
    };
  }
  const items = mappings.map((mapping: any) => ({
      label: mapping.label,
      value: formatValue(data[mapping.column], mapping.format)
    }));
  return {
    items
  };
}
async function processDataGrid(supabase: SupabaseClient, companyId: string, _municipality: string, params: any, allowFiltering = false, context: Record<string, any> | null): Promise<DataGridResult> {
  const { source, table, orderBy, initialFilter } = params;
  const { columns } = table || {};
  if (!source || !columns) throw new Error(`Invalid config for dataGrid: missing source or columns.`);
  console.log(`processDataGrid: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(initialFilter)}`);

  // --- Reverted Logic: Standard approach --- 
  const selectColumns = columns.map((c: any) => c.column).join(',');
  let query: any = supabase.from(source).select(selectColumns);

  // Apply company_id filter if applicable (This should now work for field_yearly_data)
  if (await tableHasColumn(supabase, source, 'company_id')) {
    console.log(`processDataGrid: Applying company_id filter for ${source}`);
    query = query.eq('company_id', companyId);
  } else {
    console.log(`processDataGrid: Skipping company_id filter for ${source}`);
  }

  // Apply initial filters (potentially using context) 
  if (initialFilter) {
    for (const key in initialFilter) {
      let filterValue = initialFilter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1]; // Adjusted regex back
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processDataGrid: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processDataGrid: Could not resolve context for filter ${key}=${filterValue}`);
          return { rows: [], columns: columns, allowFiltering, error: `Unresolved context for filter ${key}` };
        }
      }

      // Apply the filter 
      if (filterValue === 'latest') {
         // Check latest year based on the source table 
        const latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context); 
        if (latestYear) {
           if (await tableHasColumn(supabase, source, 'year')) {
               query = query.eq('year', latestYear);
           } else {
               console.warn(`processDataGrid: Cannot apply 'latest' year filter. 'year' column not found on source ${source}.`);
               return { rows: [], columns: columns, allowFiltering };
           }
        } else {
          console.warn(`processDataGrid: Cannot apply 'latest' filter for ${source}, no year found for context ${JSON.stringify(context)}.`);
          return { rows: [], columns: columns, allowFiltering };
        }
      } else {
        // Apply other filters 
        if (await tableHasColumn(supabase, source, key)){
             query = query.eq(key, filterValue);
        } else {
             console.warn(`processDataGrid: Cannot apply filter ${key}=${filterValue}. Column not found on source ${source}.`);
             // Skip filter if column doesn't exist
        }
      }
    }
  }

  // Apply ordering 
  if (orderBy?.length > 0) {
    orderBy.forEach((order: any) => {
        // Check if order column exists before applying
        if (columns.some((c:any) => c.column === order.column)) {
             query = query.order(order.column, { ascending: order.direction === 'asc' });
        } else {
            console.warn(`processDataGrid: Cannot order by column ${order.column} as it's not selected from ${source}`);
        }
    });
  }

  // DEBUG: Log the final query structure before execution
  console.log(`DEBUG processDataGrid (${source}): Query before execution:`, query);
  // END DEBUG

  const { data, error } = await query;

  if (error) {
    // Keep the improved error logging
    console.error(`Error fetching data for dataGrid (${source}) with filter ${JSON.stringify(initialFilter)} and order ${JSON.stringify(orderBy)}:`, error);
    return {
      rows: [],
      columns: columns,
      allowFiltering,
      error: `Database error: ${error.message}`
    };
  }

  // --- Reverted Logic: Standard data processing --- 
  const processedData = data; // No need for special handling anymore

  const rows = processedData.map((row: any) => {
    const rowData: Record<string, any> = {};
    columns.forEach((col: any) => rowData[col.key] = formatValue(row[col.column], col.format));
    return rowData;
  });

  return {
    rows,
    columns: columns,
    allowFiltering
  };
}
async function processKpiGroup(supabase: SupabaseClient, companyId: string, municipality: string, params: any, context: Record<string, any> | null) {
  const { source, kpis } = params;
  const { timeContext, n = 1, metrics, filter: kpiFilter } = kpis || {};
  if (!source || !metrics) throw new Error(`Invalid config for kpiGroup: missing source or metrics.`);
  console.log(`processKpiGroup: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(kpiFilter)}`);
  let latestYear = null;
  if (timeContext === 'last_n_years') {
    latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
    if (!latestYear) {
      console.log(`No target years found for kpiGroup ${source}, company ${companyId}, context ${JSON.stringify(context)}.`);
      return {
        kpis: []
      };
    }
  } else {
    latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
    if (!latestYear) {
      console.log(`No target years found for kpiGroup ${source}, company ${companyId}, context ${JSON.stringify(context)}.`);
      return {
        kpis: []
      };
    }
  }
  const filterYear = latestYear; // Use the determined latest year
  // Select necessary columns
  const selectColumns = [
    'year',
    ...metrics.map((m: any)=>m.column),
    ...kpiFilter ? Object.keys(kpiFilter).map((k: any)=>k) : []
  ].filter((v, i, a)=>a.indexOf(v) === i).join(',');
  let query = supabase.from(source).select(selectColumns);
  // Apply company_id filter
  if (await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
  }
  // Apply municipality filter ONLY if source table expects it
  if (TABLES_WITH_MUNICIPALITY_SUMMARY.includes(source) && await tableHasColumn(supabase, source, 'municipality')) {
    console.log(`processKpiGroup: Applying municipality filter for ${source}`);
    query = query.eq('municipality', municipality);
  } else {
    console.log(`processKpiGroup: Skipping municipality filter for ${source}`);
  }
  // Filter by the specific year determined
  query = query.eq('year', filterYear);
  // Apply specific KPI filters (potentially using context)
  if (kpiFilter) {
    for(const key in kpiFilter){
      let filterValue = kpiFilter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.')) {
        if (!context) {
            console.warn(`processKpiGroup: Cannot resolve filter ${key}=${filterValue} because context is null.`);
            // Decide if this is an error or if the filter should be skipped
            // Returning error state for now, matching previous behaviour when resolution failed.
            return {
                kpis: [],
                error: `Cannot resolve context for filter ${key}: Context is null`
            };
        }
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processKpiGroup: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processKpiGroup: Could not resolve context for filter ${key}=${filterValue}`);
          return {
            kpis: [],
            error: `Unresolved context for filter ${key}`
          };
        }
      }
      // Apply the filter
      // SPECIAL HANDLING for 'latest' year filter
      if (key === 'year' && filterValue === 'latest') {
        const latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
        if (latestYear !== null) {
          query = query.eq('year', latestYear);
          console.log(`processKpiGroup: Applied 'latest' year filter as year = ${latestYear}`);
        } else {
          console.warn(`processKpiGroup: Cannot apply filter ${key}='latest' as latest year could not be determined for ${source} and context ${JSON.stringify(context)}.`);
          // Return empty results if latest year is crucial and not found
          return { kpis: [] };
        }
      } else {
          // Apply standard filter for other keys/values
          query = query.eq(key, filterValue);
      }
    }
  }
  const { data: resultData, error } = await query.maybeSingle();
  if (error) {
    console.error(`Error fetching data for kpiGroup (${source}):`, error);
    return {
      kpis: [],
      error: `Database error: ${error.message}`
    };
  }
  if (!resultData) {
    console.log(`No data found for kpiGroup (${source}) for year ${filterYear} and filters.`);
    return {
      kpis: []
    };
  }
  const kpiResults = metrics.map((metric: any)=>({
      key: metric.key,
      label: metric.label,
      value: formatValue(resultData[metric.column], metric.format)
    }));
  return {
    kpis: kpiResults
  };
}
async function processTimeSeriesChart(supabase: SupabaseClient, companyId: string, _municipality: string, params: any, chartType: string, context: Record<string, any> | null) {
  const { source, timeSeries, orderBy } = params;
  const { timeColumn, metrics, valueColumn, groupByColumn, filter } = timeSeries || {};
  const isSimpleSeries = metrics?.length > 0;
  const isGroupedSeries = valueColumn && groupByColumn;
  if (!source || !timeColumn || !isSimpleSeries && !isGroupedSeries) throw new Error(`Invalid config for ${chartType}: missing required timeSeries params.`);
  console.log(`processTimeSeriesChart: Source=${source}, Type=${chartType}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(filter)}`);
  let selectList = [
    timeColumn
  ];
  if (isSimpleSeries) selectList = [
    ...selectList,
    ...metrics.map((m: any)=>m.column)
  ];
  else if (isGroupedSeries) selectList = [
    ...selectList,
    valueColumn,
    groupByColumn
  ];
  selectList = [
    ...new Set(selectList)
  ]; // Deduplicate
  let query = supabase.from(source).select(selectList.join(','));
  // --- Special filter handling for animal_production_log ---
  let appliedCompanyFilter = false;
  if (source === 'animal_production_log') {
    const { data: sites, error: siteError } = await supabase.from('production_sites').select('chr').eq('company_id', companyId);
    if (siteError || !sites || sites.length === 0) {
      console.warn(`Could not find production sites (CHRs) for company ${companyId} to filter ${source}. Returning empty.`);
      return {
        data: {}
      };
    }
    const chrs = sites.map((s)=>s.chr);
    query = query.in('chr', chrs);
    appliedCompanyFilter = true;
    console.log(`processTimeSeriesChart: Applied CHR filter for ${source}:`, chrs);
  }
  // --- End special handling ---
  // Apply standard company_id filter if applicable and not already handled
  if (!appliedCompanyFilter && await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
  }
  // Apply specific filters (potentially using context)
  if (filter) {
    for(const key in filter){
      let filterValue = filter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processTimeSeriesChart: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processTimeSeriesChart: Could not resolve context for filter ${key}=${filterValue}`);
          return {
            data: {},
            error: `Unresolved context for filter ${key}`
          };
        }
      }
      // Apply the filter
      query = query.eq(key, filterValue);
    }
  }
  // Apply ordering
  if (orderBy?.length > 0) {
    orderBy.forEach((order: any)=>query = query.order(order.column, {
        ascending: order.direction === 'asc'
      }));
  } else {
    query = query.order(timeColumn, {
      ascending: true
    });
  }
  const { data, error } = await query;
  if (error) {
    console.error(`Error fetching data for ${chartType} (${source}):`, error);
    return {
      data: {},
      error: `Database error: ${error.message}`
    };
  }
  if (!data?.length) {
    console.log(`No data found for ${chartType} (${source}) company ${companyId}.`);
    return {
      data: {}
    };
  }
  const timeValues = [
    ...new Set(data.map((d: any)=>d[timeColumn]))
  ].sort((a, b)=>a - b);
  let chartData: ChartData = {};
  if (isSimpleSeries) {
    chartData.xAxis = {
      label: timeColumn,
      values: timeValues
    };
    chartData.series = metrics.map((metric: any)=>({
        name: metric.seriesName || metric.key,
        type: metric.type,
        yAxis: metric.yAxis,
        data: timeValues.map((t: any)=>data.find((d: any)=>d[timeColumn] === t)?.[metric.column] ?? null)
      }));
    chartData.yAxis = {
      label: "Value"
    };
  } else if (isGroupedSeries) {
    const groupKeys = [
      ...new Set(data.map((d: any)=>d[groupByColumn]))
    ].sort();
    chartData.xAxis = {
      label: timeColumn,
      values: timeValues
    };
    chartData.series = groupKeys.map((group: any)=>({
        name: group,
        data: timeValues.map((t: any)=>data.find((d: any)=>d[timeColumn] === t && d[groupByColumn] === group)?.[valueColumn] ?? 0)
      }));
    chartData.yAxis = {
      label: valueColumn
    };
  }
  return {
    data: chartData
  };
}
async function processCategoryChart(supabase: SupabaseClient, companyId: string, _municipality: string, params: any, chartType: string, context: Record<string, any> | null) {
  const { source, category, orderBy } = params;
  const { timeContext, n = 1, categoryColumn, valueColumn, stackByColumn, topN, filter } = category || {};
  if (!source || !categoryColumn || !valueColumn) throw new Error(`Invalid config for ${chartType}: missing required category params.`);
  console.log(`processCategoryChart: Source=${source}, Type=${chartType}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(filter)}`);
  // Determine the year for filtering based on timeContext
  let filterYear = null;
  if (timeContext === 'last_n_years') {
    filterYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
    if (!filterYear) {
      console.log(`No target year found for ${chartType} ${source}, company ${companyId}, context ${JSON.stringify(context)}.`);
      return {
        data: {}
      }; // Return empty data if no year found
    }
  } else {
    console.warn(`processCategoryChart: Unsupported timeContext '${timeContext}'. Defaulting to latest year.`);
    filterYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
    if (!filterYear) {
      console.log(`No target year found for ${chartType} ${source}, company ${companyId}, context ${JSON.stringify(context)}.`);
      return {
        data: {}
      };
    }
  }
  console.log(`processCategoryChart: Using filter year ${filterYear}`);
  // Define select list
  let selectList = [
    categoryColumn,
    valueColumn
  ];
  if (stackByColumn) selectList.push(stackByColumn);
  selectList = [
    ...new Set(selectList)
  ];
  let query = supabase.from(source).select(selectList.join(','));
  // Apply company_id filter if applicable
  if (await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
  }
  // Apply year filter
  if (await tableHasColumn(supabase, source, 'year')) {
    query = query.eq('year', filterYear);
  } else {
    console.warn(`processCategoryChart: Source table ${source} does not have a 'year' column for filtering.`);
  }
  // Apply specific filters (potentially using context)
  if (filter) {
    for(const key in filter){
      let filterValue = filter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processCategoryChart: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processCategoryChart: Could not resolve context for filter ${key}=${filterValue}`);
          return {
            data: {},
            error: `Unresolved context for filter ${key}`
          };
        }
      }
      // Apply the filter
      query = query.eq(key, filterValue);
    }
  }
  // Apply ordering if specified (useful for topN)
  if (orderBy?.length > 0) {
    orderBy.forEach((order: any)=>query = query.order(order.column, {
        ascending: order.direction === 'asc'
      }));
  } else if (topN) {
    // Default ordering for topN if not specified
    query = query.order(valueColumn, {
      ascending: false
    });
  }
  // Apply limit for topN
  if (topN) {
    query = query.limit(topN);
  }
  const { data, error } = await query;
  if (error) {
    console.error(`Error fetching data for ${chartType} (${source}):`, error);
    return {
      data: {},
      error: `Database error: ${error.message}`
    };
  }
  if (!data || data.length === 0) {
    console.log(`No data found for ${chartType} (${source}) company ${companyId} year ${filterYear}.`);
    return {
      data: {}
    }; // Return empty chart data structure
  }
  // --- Data Transformation for Category Charts (Horizontal Stacked Bar Example) ---
  let chartData: ChartData = {};
  const categories = [...new Set(data.map((d: any)=>d[categoryColumn]))]; // Get unique categories from the fetched (potentially limited by topN) data
  if (stackByColumn) {
    const stackKeys = [...new Set(data.map((d: any)=>d[stackByColumn]))].sort(); // Unique stack keys
    chartData.yAxis = {
      label: categoryColumn,
      values: categories
    }; // Categories on Y axis for horizontal
    chartData.series = stackKeys.map((stack: any)=>({
        name: formatValue(stack, 'boolean'),
        data: categories.map((cat: any)=>{
          const point = data.find((d: any)=>d[categoryColumn] === cat && d[stackByColumn] === stack);
          return point ? point[valueColumn] : 0; // Value for this category/stack combo
        })
      }));
    chartData.xAxis = {
      label: valueColumn
    }; // Values on X axis
  } else {
    // Non-stacked version (simple horizontal bar)
    chartData.yAxis = {
      label: categoryColumn,
      values: categories
    };
    chartData.series = [
      {
        name: valueColumn,
        data: categories.map((cat: any)=>{
          const point = data.find((d: any)=>d[categoryColumn] === cat);
          return point ? point[valueColumn] : 0;
        })
      }
    ];
    chartData.xAxis = {
      label: valueColumn
    };
  }
  return {
    data: chartData
  };
}
async function processMapChart(supabase: SupabaseClient, companyId: string, _municipality: string, params: any, _context: Record<string, any> | null) {
  const { map } = params;
  const { layers } = map || {};
  if (!layers?.length) throw new Error(`Invalid config for mapChart: missing or invalid layers.`);

  const processedLayers: MapLayerResult[] = [];

  for (const layer of layers){
    const { name, source, geometryColumn, properties = [], style } = layer;
    if (!source || !geometryColumn) {
      console.warn(`Skipping map layer "${name}" due to missing source or geometryColumn.`);
      continue;
    }
    console.log(`processMapChart: Processing layer ${name} (Source: ${source})`);
    // Use Supabase's built-in GeoJSON casting
    const selectString = `*, geojson: ${geometryColumn}`;
    let query = supabase.from(source).select(selectString);
    // Apply company_id or primary ID filter correctly
    if (source === 'companies') {
      query = query.eq('id', companyId); // Filter companies by PRIMARY id
      console.log(`processMapChart: Applying filter id=${companyId} for ${source}`);
    } else if (await tableHasColumn(supabase, source, 'company_id')) {
      query = query.eq('company_id', companyId);
      console.log(`processMapChart: Applying filter company_id=${companyId} for ${source}`);
    }
    // TODO: Refine special handling for field_boundaries needing latest crop data - requires RPC.
    if (source === 'field_boundaries' && (properties.includes('crop_name') || properties.includes('is_organic'))) {
      console.warn(`Map layer "${name}": Fetching basic field boundaries. Joining latest yearly data requires an RPC function.`);
      // Falling back to selecting only base properties defined in the YAML from field_boundaries
      const baseProperties = properties.filter((p: string) => p !== 'crop_name' && p !== 'is_organic');
      const simpleSelectString = `${baseProperties.join(',')}, geojson: ${geometryColumn}`;
      query = supabase.from(source).select(simpleSelectString).eq('company_id', companyId); // Assuming company_id exists
    }
    const { data, error } = await query;
    if (error) {
      console.error(`Error fetching data for map layer "${name}" (${source}):`, error);
      // Add error placeholder to the layer result
      processedLayers.push({
        name: name,
        type: 'error',
        error: `Database error: ${error.message}`
      });
      continue;
    }
    if (!data?.length) {
      console.log(`No data found for map layer "${name}" (${source}) company ${companyId}.`);
      processedLayers.push({
        name: name,
        type: 'geojson',
        style: style,
        data: {
          type: 'FeatureCollection',
          features: []
        }
      });
      continue;
    }
    const features = data.map((feature: any)=>{
      const featureProperties: Record<string, any> = {};
      properties.forEach((prop: any)=>{
        if (feature.hasOwnProperty(prop)) {
          featureProperties[prop] = feature[prop];
        }
      });
      if (!feature.geojson || typeof feature.geojson !== 'object') {
        console.warn(`Missing or invalid geojson data for feature in layer "${name}":`, feature);
        return null;
      }
      return {
        type: 'Feature',
        geometry: feature.geojson,
        properties: featureProperties
      };
    }).filter(Boolean);
    processedLayers.push({
      name: name,
      type: 'geojson',
      style: style,
      data: {
        type: 'FeatureCollection',
        features: features
      }
    });
  }
  // Get company address for centering
  const { data: companyGeomData } = await supabase.from('companies').select(`geojson: address_geom`).eq('id', companyId).maybeSingle();
  const center = companyGeomData?.geojson?.coordinates || [
    9.5,
    56.0
  ];
  const zoom = companyGeomData?.geojson ? 13 : 9;
  return {
    data: {
      center: center,
      zoom: zoom,
      layers: processedLayers
    }
  };
}
async function processTimeline(supabase: SupabaseClient, companyId: string, _municipality: string, params: any, context: Record<string, any> | null) {
  const { source, events, orderBy } = params;
  const { filter, dateColumn, descriptionColumn, groupByColumns, filterColumns } = events || {};
  if (!source || !dateColumn || !descriptionColumn) throw new Error(`Invalid config for timeline: missing required event parameters.`);
  console.log(`processTimeline: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(filter)}`);
  let selectList = [
    dateColumn,
    descriptionColumn
  ];
  if (groupByColumns) selectList.push(...groupByColumns);
  if (filterColumns) selectList.push(...filterColumns);
  selectList = [
    ...new Set(selectList)
  ];
  let query = supabase.from(source).select(selectList.join(','));
  // Apply company_id or CHR filter based on source and context
  let appliedContextFilter = false;
  if (context?.chr && await tableHasColumn(supabase, source, 'chr')) {
    query = query.eq('chr', context.chr);
    appliedContextFilter = true;
    console.log(`processTimeline: Applied context CHR filter: ${context.chr}`);
  } else if (await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
    appliedContextFilter = true;
    console.log(`processTimeline: Applied company_id filter: ${companyId}`);
  }
  // Apply other specific event filters from YAML (if not already handled by context)
  if (filter) {
    for(const key in filter){
      let filterValue = filter[key];
      // Resolve context only if this key wasn't the primary context key (like 'chr')
      if (!appliedContextFilter || key !== 'chr') {
        if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
          const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
          if (ctxKey && context[ctxKey] !== undefined) {
            filterValue = context[ctxKey];
          } else {
            console.warn(`processTimeline: Could not resolve context for filter ${key}=${filterValue}`);
            return {
              events: [],
              config: {},
              error: `Unresolved context for filter ${key}`
            };
          }
        }
        console.log(`processTimeline: Applying YAML filter ${key}=${filterValue}`);
        query = query.eq(key, filterValue);
      }
    }
  }
  // Apply ordering
  if (orderBy?.length > 0) {
    orderBy.forEach((order: any)=>query = query.order(order.column, {
        ascending: order.direction === 'asc'
      }));
  } else {
    query = query.order(dateColumn, {
      ascending: false
    });
  }
  const { data, error } = await query;
  if (error) {
    console.error(`Error fetching data for timeline (${source}):`, error);
    return {
      events: [],
      config: {},
      error: `Database error: ${error.message}`
    };
  }
  const processedEvents = data.map((event: any)=>{
    const eventData: Record<string, any> = {
      date: formatValue(event[dateColumn], 'datetime'),
      description: event[descriptionColumn]
    };
    if (groupByColumns) groupByColumns.forEach((col: any)=>eventData[col] = event[col]);
    if (filterColumns) filterColumns.forEach((col: any)=>eventData[col] = event[col]);
    return eventData;
  });
  return {
    events: processedEvents,
    config: {
      groupByColumns: groupByColumns || [],
      filterColumns: filterColumns || []
    }
  };
}
// --- Helper function for data formatting ---
function formatValue(value: any, format: any): string {
  if (value === null || value === undefined) return 'N/A';
  try {
    switch(format){
      case 'boolean':
        return value ? 'Ja' : 'Nej';
      case 'date':
        return new Date(value).toISOString().split('T')[0];
      case 'datetime':
        return new Date(value).toISOString();
      case 'currency':
        return new Intl.NumberFormat('da-DK', {
          style: 'currency',
          currency: 'DKK',
          minimumFractionDigits: 0,
          maximumFractionDigits: 0
        }).format(Number(value) || 0);
      case 'number':
        return new Intl.NumberFormat('da-DK', {
          minimumFractionDigits: 0,
          maximumFractionDigits: 1
        }).format(Number(value) || 0);
      default:
        return String(value);
    }
  } catch (e) {
    console.warn(`Error formatting value '${value}' with format '${format}':`, e);
    return String(value);
  }
}
// Define type for iterated section results
type IteratedSectionResult = {
    title: string;
    layout: string;
    content: ComponentResult[];
}
// --- Recursive Component Processor ---
async function processComponent(componentConfig: any, supabase: SupabaseClient, companyId: string, municipality: string, parentContext: Record<string, any> | null): Promise<ComponentResult> {
  const { _key, _type, title, dataSource, iteratorDataSource, iterationConfig, template } = componentConfig;
  console.log(`Processing component: ${_key} (${_type}), Context: ${JSON.stringify(parentContext)}`);
  let resultData: any = null; // Use any temporarily for varied results
  let processingError: string | null = null;

  try {
    // --- Handle Iterated Sections Recursively ---
    if (_type === 'iteratedSection' && iteratorDataSource && template) {
      const iteratorParams = iteratorDataSource.params;
      const { source: iterSource, columns: iterColumns, filter: iterFilter } = iteratorParams;
      if (!iterSource || !iterColumns) throw new Error(`Invalid iteratorDataSource for ${_key}`);

      let iterQuery = supabase.from(iterSource).select(iterColumns.join(','));

      // Apply base company_id filter to iterator source if applicable AND source is not the problematic one
      if (iterSource !== 'site_species_production_ranked' && await tableHasColumn(supabase, iterSource, 'company_id')) {
        iterQuery = iterQuery.eq('company_id', companyId);
      }

      // Apply specific iterator filters (potentially using parent context)
      if (iterFilter) {
        for(const key in iterFilter){
          let filterValue = iterFilter[key];
          if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && parentContext) {
            const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
            if (ctxKey && parentContext[ctxKey] !== undefined) {
              filterValue = parentContext[ctxKey];
            } else {
              throw new Error(`Could not resolve parent context for iterator filter ${key}=${iterFilter[key]}`);
            }
          }
          iterQuery = iterQuery.eq(key, filterValue);
        }
      }

      // TODO: Add iterator ordering?
      const { data: iteratorItems, error: iterError } = await iterQuery;
      if (iterError) throw new Error(`Failed to fetch iterator items for ${_key}: ${iterError.message}`);

      const iteratedSections: IteratedSectionResult[] = [];

      if (iteratorItems?.length > 0) {
        for (const item of iteratorItems){
          console.log(` > Iterating item for ${_key}:`, item);
          const sectionContent: ComponentResult[] = [];
          for (const templateComponent of template){
            // IMPORTANT: Resolve placeholders in the template component's config using the current 'item' as context
            let resolvedTemplateConfigStr = JSON.stringify(templateComponent);
            let contextResolved = true;
            for(const itemKey in item){
              const placeholder = `{iteratorContext.${itemKey}}`;
              const regexPlaceholder = new RegExp(placeholder.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g');
              if (resolvedTemplateConfigStr.includes(placeholder)) {
                if (item[itemKey] === null || item[itemKey] === undefined) {
                  console.warn(`Context key '${itemKey}' is null/undefined for item ${JSON.stringify(item)} in iterator ${_key}. Cannot resolve placeholder '${placeholder}'.`);
                  contextResolved = false;
                  break;
                }
                resolvedTemplateConfigStr = resolvedTemplateConfigStr.replace(regexPlaceholder, String(item[itemKey]));
              }
            }
            if (!contextResolved) {
              sectionContent.push({
                _key: templateComponent._key,
                _type: "error",
                title: templateComponent.title,
                error: "Failed to resolve required context."
              });
              continue;
            }
            const resolvedTemplateConfig = JSON.parse(resolvedTemplateConfigStr);
            // Recursively process the template component with the current 'item' as context
            const processedItem: ComponentResult = await processComponent(resolvedTemplateConfig, supabase, companyId, municipality, item);
            sectionContent.push(processedItem);
          } // End template component loop
          iteratedSections.push({
            title: iterationConfig?.titleField ? item[iterationConfig.titleField] : `Item ${iteratedSections.length + 1}`,
            layout: iterationConfig?.layout || 'default',
            content: sectionContent
          });
        } // End iterator item loop
      } else {
        console.log(`No items found for iterator: ${_key}`);
      }
      resultData = {
        iterationConfig: iterationConfig,
        sections: iteratedSections
      };
    } else if (dataSource?.params) {
      const params = dataSource.params;
      // Pass parentContext down to processing functions
      if (_type === 'infoCard') resultData = await processInfoCard(supabase, companyId, municipality, params, parentContext);
      // Unified dataGrid handling
      else if (_type === 'dataGrid') {
        // Pass allowFiltering and isCollapsible from componentConfig directly
        const allowFiltering = componentConfig.allowFiltering || false;
        const isCollapsible = componentConfig.isCollapsible || false;

        let gridResult: DataGridResult = await processDataGrid(supabase, companyId, municipality, params, allowFiltering, parentContext);
        
        // Set isCollapsible on the result if it was configured at the component level
        if (isCollapsible && !gridResult.error) {
            gridResult.isCollapsible = true;
        }
        resultData = gridResult;
      }
      else if (_type === 'kpiGroup') resultData = await processKpiGroup(supabase, companyId, municipality, params, parentContext);
      else if (_type === 'barChart' || _type === 'stackedBarChart' || _type === 'comboChart' || _type === 'lineChart' || _type === 'multiLineChart') resultData = await processTimeSeriesChart(supabase, companyId, municipality, params, _type, parentContext);
      else if (_type === 'horizontalStackedBarChart') resultData = await processCategoryChart(supabase, companyId, municipality, params, _type, parentContext);
      else if (_type === 'mapChart') resultData = await processMapChart(supabase, companyId, municipality, params, parentContext);
      else if (_type === 'timeline') resultData = await processTimeline(supabase, companyId, municipality, params, parentContext);
      else processingError = `Component type "${_type}" not implemented.`;
    } else if (_type !== 'iteratedSection') {
      processingError = `Component "${_key}" (${_type}) is missing dataSource or params.`;
    }
  } catch (error) {
    const err = error as Error;
    console.error(`Error processing component "${_key}" (${_type}) with context ${JSON.stringify(parentContext)}:`, err);
    processingError = `Failed to load data: ${err.message}`;
  }
  // Structure the final output for this component
  const finalResultData = resultData as (ComponentResult | null);
  if (finalResultData && !finalResultData.error && !processingError) {
    // Spread finalResultData first, then set title
    return {
      ...finalResultData, // Spread the processed data
      _key: _key,       // Ensure _key from config is used
      _type: _type,     // Ensure _type from config is used
      title: title       // Ensure title from config is used
    };
  } else {
    // Error path remains the same
    return {
      _key: _key,       
      _type: "error",   
      title: title || _key,
      error: finalResultData?.error || processingError || "Unknown processing error"
    };
  }
}
// --- Main Request Handler ---
serve(async (req)=>{
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response('ok', {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type'
      }
    });
  }
  const url = new URL(req.url);
  const companyIdParam = url.searchParams.get('id');
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  };
  if (!companyIdParam) return new Response(JSON.stringify({
    error: 'Company ID (UUID) query parameter is required'
  }), {
    status: 400,
    headers
  });
  let config;
  let companyInfo: CompanyInfo | null = null;
  let supabase: SupabaseClient;
  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.');
    }
    supabase = createClient(supabaseUrl, supabaseKey, {
      global: {
        fetch: fetch.bind(globalThis)
      }
    });
    companyInfo = await getCompanyDetails(supabase, companyIdParam);
    if (!companyInfo) {
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      const errorMsg = uuidRegex.test(companyIdParam) ? `Company with ID ${companyIdParam} not found` : `Invalid Company ID format provided`;
      return new Response(JSON.stringify({
        error: errorMsg
      }), {
        status: 404,
        headers
      });
    }
    config = await getYamlConfig();
    if (!config?.pageBuilder) throw new Error("Invalid or empty configuration loaded.");
    // Process Page Builder Components using the recursive processor
    const pageBuilderResults: ComponentResult[] = [];
    for (const componentConfig of config.pageBuilder){
      // Use non-null assertion after null check guarantees it's safe
      const processedResult: ComponentResult = await processComponent(componentConfig, supabase, companyInfo!.id, companyInfo!.municipality, null);
      pageBuilderResults.push(processedResult);
    }
    // Construct Final Response
    const responseBody = {
      metadata: {
        api_version: "1.2.0",
        generated_at: new Date().toISOString(),
        config_version: config?.metadata?.configVersion || "unknown",
        data_updated_at: null, // TODO: Could potentially get this from source tables?
        // Use non-null assertion after null check guarantees it's safe
        company_id: companyInfo!.id,
        company_cvr: companyInfo!.cvr_number,
        municipality: companyInfo!.municipality
      },
      pageBuilder: pageBuilderResults
    };
    return new Response(JSON.stringify(responseBody, null, 2), {
      headers
    });
  } catch (error) {
    const err = error as Error;
    console.error('Critical error in edge function:', err);
    const errorHeaders = {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    };
    return new Response(JSON.stringify({
      error: `Internal Server Error: ${err.message}`
    }), {
      status: 500,
      headers: errorHeaders
    });
  }
});
