import { serve } from 'std/http/server';
import { createClient } from '@supabase/supabase-js';
import yaml from 'js-yaml';
// --- Configuration ---
const YAML_CONFIG_URL = 'https://raw.githubusercontent.com/Klimabevaegelsen/landbruget.dk/main/backend/api/config.yaml';
const CONFIG_CACHE_DURATION_MS = 60 * 60 * 1000; // 1 hour cache
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
let cachedConfig = null;
let lastFetchTimestamp = 0;
// --- Helper: Fetch and Cache YAML Config ---
async function getYamlConfig() {
  const now = Date.now();
  if (cachedConfig && now - lastFetchTimestamp < CONFIG_CACHE_DURATION_MS) {
    console.log("Serving cached config.");
    return cachedConfig;
  }
  console.log("Fetching fresh config from:", YAML_CONFIG_URL);
  try {
    const response = await fetch(YAML_CONFIG_URL);
    if (!response.ok) throw new Error(`Failed to fetch YAML config: ${response.statusText}`);
    const yamlText = await response.text();
    cachedConfig = yaml.load(yamlText);
    lastFetchTimestamp = now;
    console.log("Config fetched and cached successfully.");
    return cachedConfig;
  } catch (error) {
    console.error("Error fetching or parsing YAML config:", error);
    if (cachedConfig) {
      console.warn("Returning stale cached config due to fetch error.");
      return cachedConfig;
    }
    throw error;
  }
}
// --- Helper: Get Company Details (Lookup by ID - UUID) ---
async function getCompanyDetails(supabase, companyId) {
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
// --- Helper: Get Latest Year (Generalized) ---
async function getLatestYearForCompany(supabase, sourceTable, companyId, yearColumn = 'year', filterContext = {}) {
  // Refined to potentially filter by CHR if provided
  let query = supabase.from(sourceTable).select(yearColumn, {
    count: 'exact',
    head: false
  });
  // Apply context filter (CHR for site-specific latest year)
  if (filterContext.chr && await tableHasColumn(supabase, sourceTable, 'chr')) {
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
  const latestYear = data ? data[yearColumn] : null;
  console.log(`getLatestYearForCompany: Determined latest year for ${sourceTable} (Company ${companyId}, Context ${JSON.stringify(filterContext)}) as: ${latestYear}`);
  return latestYear;
}
// --- Helper: Check if table has a column (simple check, needs improvement/caching) ---
const columnExistenceCache = new Map();
async function tableHasColumn(supabase, tableName, columnName) {
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
async function processInfoCard(supabase, companyId, municipality, params, context) {
  const { source, record } = params;
  const { mappings, filter: recordFilter } = record || {};
  if (!source || !mappings) throw new Error(`Invalid config for infoCard: missing source or mappings.`);
  console.log(`processInfoCard: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(recordFilter)}`);
  const selectColumns = mappings.map((m)=>m.column).join(',');
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
  const items = mappings.map((mapping)=>({
      label: mapping.label,
      value: formatValue(data[mapping.column], mapping.format)
    }));
  return {
    items
  };
}
async function processDataGrid(supabase, companyId, _municipality, params, allowFiltering = false, context) {
  const { source, table, orderBy, initialFilter } = params;
  const { columns } = table || {};
  if (!source || !columns) throw new Error(`Invalid config for dataGrid: missing source or columns.`);
  console.log(`processDataGrid: Source=${source}, Context=${JSON.stringify(context)}, Filter=${JSON.stringify(initialFilter)}`);
  const selectColumns = columns.map((c)=>c.column).join(',');
  let query = supabase.from(source).select(selectColumns);
  // Apply company_id filter if applicable
  if (await tableHasColumn(supabase, source, 'company_id')) {
    query = query.eq('company_id', companyId);
  }
  // Apply initial filters (potentially using context)
  if (initialFilter) {
    for(const key in initialFilter){
      let filterValue = initialFilter[key];
      // Resolve context placeholder if present
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
        const ctxKey = filterValue.match(/\{iteratorContext\.([^}]+)\}/)?.[1];
        if (ctxKey && context[ctxKey] !== undefined) {
          filterValue = context[ctxKey];
          console.log(`processDataGrid: Resolved filter ${key}=${filterValue} from context.`);
        } else {
          console.warn(`processDataGrid: Could not resolve context for filter ${key}=${filterValue}`);
          return {
            rows: [],
            columns: columns,
            allowFiltering,
            error: `Unresolved context for filter ${key}`
          };
        }
      }
      // Apply the filter
      if (filterValue === 'latest') {
        const latestYear = await getLatestYearForCompany(supabase, source, companyId, 'year', context);
        if (latestYear) {
          query = query.eq('year', latestYear);
        } else {
          console.warn(`processDataGrid: Cannot apply 'latest' filter for ${source}, no year found for context ${JSON.stringify(context)}.`);
          // Allow query without year filter to proceed? Or return empty? Returning empty.
          return {
            rows: [],
            columns: columns,
            allowFiltering
          };
        }
      } else {
        query = query.eq(key, filterValue);
      }
    }
  }
  // Apply ordering
  if (orderBy?.length > 0) {
    orderBy.forEach((order)=>query = query.order(order.column, {
        ascending: order.direction === 'asc'
      }));
  }
  const { data, error } = await query;
  if (error) {
    console.error(`Error fetching data for dataGrid (${source}):`, error);
    return {
      rows: [],
      columns: columns,
      allowFiltering,
      error: `Database error: ${error.message}`
    };
  }
  const rows = data.map((row)=>{
    const rowData = {};
    columns.forEach((col)=>rowData[col.key] = formatValue(row[col.column], col.format));
    return rowData;
  });
  return {
    rows,
    columns: columns,
    allowFiltering
  };
}
async function processKpiGroup(supabase, companyId, municipality, params, context) {
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
    ...metrics.map((m)=>m.column),
    ...kpiFilter ? Object.keys(kpiFilter).map((k)=>k) : []
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
      if (typeof filterValue === 'string' && filterValue.startsWith('{iteratorContext.') && context) {
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
      query = query.eq(key, filterValue);
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
  const kpiResults = metrics.map((metric)=>({
      key: metric.key,
      label: metric.label,
      value: formatValue(resultData[metric.column], metric.format)
    }));
  return {
    kpis: kpiResults
  };
}
async function processTimeSeriesChart(supabase, companyId, _municipality, params, chartType, context) {
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
    ...metrics.map((m)=>m.column)
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
    orderBy.forEach((order)=>query = query.order(order.column, {
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
    ...new Set(data.map((d)=>d[timeColumn]))
  ].sort((a, b)=>a - b);
  let chartData = {};
  if (isSimpleSeries) {
    chartData.xAxis = {
      label: timeColumn,
      values: timeValues
    };
    chartData.series = metrics.map((metric)=>({
        name: metric.seriesName || metric.key,
        type: metric.type,
        yAxis: metric.yAxis,
        data: timeValues.map((t)=>data.find((d)=>d[timeColumn] === t)?.[metric.column] ?? null)
      }));
    chartData.yAxis = {
      label: "Value"
    };
  } else if (isGroupedSeries) {
    const groupKeys = [
      ...new Set(data.map((d)=>d[groupByColumn]))
    ].sort();
    chartData.xAxis = {
      label: timeColumn,
      values: timeValues
    };
    chartData.series = groupKeys.map((group)=>({
        name: group,
        data: timeValues.map((t)=>data.find((d)=>d[timeColumn] === t && d[groupByColumn] === group)?.[valueColumn] ?? 0)
      }));
    chartData.yAxis = {
      label: valueColumn
    };
  }
  return {
    data: chartData
  };
}
async function processCategoryChart(supabase, companyId, _municipality, params, chartType, context) {
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
    orderBy.forEach((order)=>query = query.order(order.column, {
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
  let chartData = {};
  const categories = [
    ...new Set(data.map((d)=>d[categoryColumn]))
  ]; // Get unique categories from the fetched (potentially limited by topN) data
  if (stackByColumn) {
    const stackKeys = [
      ...new Set(data.map((d)=>d[stackByColumn]))
    ].sort(); // Unique stack keys
    chartData.yAxis = {
      label: categoryColumn,
      values: categories
    }; // Categories on Y axis for horizontal
    chartData.series = stackKeys.map((stack)=>({
        name: formatValue(stack, 'boolean'),
        data: categories.map((cat)=>{
          const point = data.find((d)=>d[categoryColumn] === cat && d[stackByColumn] === stack);
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
        data: categories.map((cat)=>{
          const point = data.find((d)=>d[categoryColumn] === cat);
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
async function processMapChart(supabase, companyId, _municipality, params, _context) {
  const { map } = params;
  const { layers } = map || {};
  if (!layers?.length) throw new Error(`Invalid config for mapChart: missing or invalid layers.`);
  const processedLayers = [];
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
      const baseProperties = properties.filter((p)=>p !== 'crop_name' && p !== 'is_organic');
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
    const features = data.map((feature)=>{
      const featureProperties = {};
      properties.forEach((prop)=>{
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
async function processTimeline(supabase, companyId, _municipality, params, context) {
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
    orderBy.forEach((order)=>query = query.order(order.column, {
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
  const processedEvents = data.map((event)=>{
    const eventData = {
      date: formatValue(event[dateColumn], 'datetime'),
      description: event[descriptionColumn]
    };
    if (groupByColumns) groupByColumns.forEach((col)=>eventData[col] = event[col]);
    if (filterColumns) filterColumns.forEach((col)=>eventData[col] = event[col]);
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
function formatValue(value, format) {
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
// --- Recursive Component Processor ---
async function processComponent(componentConfig, supabase, companyId, municipality, parentContext// Context from parent iterator (e.g., { chr: 'DK...', site_name: '...' })
) {
  const { _key, _type, title, dataSource, iteratorDataSource, iterationConfig, template } = componentConfig;
  console.log(`Processing component: ${_key} (${_type}), Context: ${JSON.stringify(parentContext)}`);
  let resultData = null;
  let processingError = null;
  try {
    // --- Handle Iterated Sections Recursively ---
    if (_type === 'iteratedSection' && iteratorDataSource && template) {
      const iteratorParams = iteratorDataSource.params;
      const { source: iterSource, columns: iterColumns, filter: iterFilter } = iteratorParams;
      if (!iterSource || !iterColumns) throw new Error(`Invalid iteratorDataSource for ${_key}`);
      let iterQuery = supabase.from(iterSource).select(iterColumns.join(','));
      // Apply base company_id filter to iterator source if applicable
      if (await tableHasColumn(supabase, iterSource, 'company_id')) {
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
      const iteratedSections = [];
      if (iteratorItems?.length > 0) {
        for (const item of iteratorItems){
          console.log(` > Iterating item for ${_key}:`, item);
          const sectionContent = [];
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
            const processedItem = await processComponent(resolvedTemplateConfig, supabase, companyId, municipality, item);
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
      else if (_type === 'dataGrid' || _type === 'filterableDataGrid' || _type === 'collapsibleDataGrid') {
        const allowFiltering = _type !== 'dataGrid';
        resultData = await processDataGrid(supabase, companyId, municipality, params, allowFiltering, parentContext);
        if (_type === 'collapsibleDataGrid') resultData.isCollapsible = true;
      } else if (_type === 'kpiGroup') resultData = await processKpiGroup(supabase, companyId, municipality, params, parentContext);
      else if (_type === 'barChart' || _type === 'stackedBarChart' || _type === 'comboChart' || _type === 'lineChart' || _type === 'multiLineChart') resultData = await processTimeSeriesChart(supabase, companyId, municipality, params, _type, parentContext);
      else if (_type === 'horizontalStackedBarChart') resultData = await processCategoryChart(supabase, companyId, municipality, params, _type, parentContext);
      else if (_type === 'mapChart') resultData = await processMapChart(supabase, companyId, municipality, params, parentContext);
      else if (_type === 'timeline') resultData = await processTimeline(supabase, companyId, municipality, params, parentContext);
      else processingError = `Component type "${_type}" not implemented.`;
    } else if (_type !== 'iteratedSection') {
      processingError = `Component "${_key}" (${_type}) is missing dataSource or params.`;
    }
  } catch (error) {
    console.error(`Error processing component "${_key}" (${_type}) with context ${JSON.stringify(parentContext)}:`, error);
    processingError = `Failed to load data: ${error.message}`;
  }
  // Structure the final output for this component
  if (resultData && !resultData.error && !processingError) {
    return {
      _key: _key,
      _type: _type,
      title: title,
      ...resultData
    };
  } else {
    return {
      _key: _key,
      _type: "error",
      title: title || _key,
      error: resultData?.error || processingError || "Unknown processing error"
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
  let companyInfo = null;
  let supabase;
  try {
    supabase = createClient(Deno.env.get('SUPABASE_URL'), Deno.env.get('SUPABASE_SERVICE_ROLE_KEY'), {
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
    const pageBuilderResults = [];
    for (const componentConfig of config.pageBuilder){
      const processedResult = await processComponent(componentConfig, supabase, companyInfo.id, companyInfo.municipality, null); // Start with null context
      pageBuilderResults.push(processedResult);
    }
    // Construct Final Response
    const responseBody = {
      metadata: {
        api_version: "1.2.0",
        generated_at: new Date().toISOString(),
        config_version: config?.metadata?.configVersion || "unknown",
        data_updated_at: null,
        company_id: companyInfo.id,
        company_cvr: companyInfo.cvr_number,
        municipality: companyInfo.municipality
      },
      pageBuilder: pageBuilderResults
    };
    return new Response(JSON.stringify(responseBody, null, 2), {
      headers
    });
  } catch (error) {
    console.error('Critical error in edge function:', error);
    const errorHeaders = {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    };
    return new Response(JSON.stringify({
      error: `Internal Server Error: ${error.message}`
    }), {
      status: 500,
      headers: errorHeaders
    });
  }
});
