import { defineCatalog } from "@json-render/core";
import { schema } from "@json-render/react/schema";
import { shadcnComponentDefinitions } from "@json-render/shadcn/catalog";
import { z } from "zod";

function pick<T extends Record<string, unknown>, K extends keyof T>(obj: T, keys: K[]): Pick<T, K> {
  const result = {} as Pick<T, K>;
  for (const key of keys) {
    if (key in obj) result[key] = obj[key];
  }
  return result;
}

export const catalog = defineCatalog(schema, {
  components: {
    // Layout components from shadcn
    ...pick(shadcnComponentDefinitions, [
      "Card",
      "Stack",
      "Grid",
      "Tabs",
      "Separator",
      "Heading",
      "Text",
      "Badge",
      "Alert",
      "Progress",
      "Table",
    ]),

    // Chart components (Recharts-based)
    BarChart: {
      props: z.object({
        data: z.array(z.record(z.string(), z.unknown())),
        xKey: z.string(),
        yKey: z.string(),
        title: z.string().nullable(),
        color: z.string().nullable(),
        height: z.number().nullable(),
      }),
      description:
        "Bar chart for comparing categories. Use for top-N rankings, distributions, comparisons. xKey is the category field, yKey is the numeric value.",
    },

    LineChart: {
      props: z.object({
        data: z.array(z.record(z.string(), z.unknown())),
        xKey: z.string(),
        yKeys: z.array(z.string()),
        title: z.string().nullable(),
        height: z.number().nullable(),
      }),
      description:
        "Line chart for time series or trends. Use when data has a year/date column and numeric values. xKey is the time axis, yKeys are the value series.",
    },

    PieChart: {
      props: z.object({
        data: z.array(z.record(z.string(), z.unknown())),
        nameKey: z.string(),
        valueKey: z.string(),
        title: z.string().nullable(),
        height: z.number().nullable(),
      }),
      description:
        "Pie/donut chart for proportional data. Use when showing parts of a whole (e.g., crop type distribution, organic vs conventional).",
    },

    KPICard: {
      props: z.object({
        label: z.string(),
        value: z.string(),
        format: z.enum(["number", "percent", "hectares", "text"]).nullable(),
        description: z.string().nullable(),
      }),
      description:
        "Single metric display card. Use for scalar results like counts, averages, totals, or single-row lookups.",
    },

    DataTable: {
      props: z.object({
        data: z.array(z.record(z.string(), z.unknown())),
        columns: z.array(z.string()).nullable(),
        pageSize: z.number().nullable(),
      }),
      description:
        "Sortable, paginated data table. Use for multi-row tabular results without a clear chart representation. Default fallback for complex result sets.",
    },

    MapView: {
      props: z.object({
        data: z.array(z.record(z.string(), z.unknown())),
        latKey: z.string(),
        lngKey: z.string(),
        labelKey: z.string().nullable(),
        popupKeys: z.array(z.string()).nullable(),
        title: z.string().nullable(),
        height: z.number().nullable(),
      }),
      description:
        "Interactive map with markers for geographic data. Use when results contain latitude/longitude columns (e.g., lat, lng, latitude, longitude, centroid_lat, centroid_lng). Shows markers for small datasets, clusters for large ones.",
    },
  },
  actions: {},
});

export const VISUALIZATION_RULES = [
  "You are generating a UI spec to visualize SQL query results from Danish agricultural data.",
  "RULES FOR CHOOSING COMPONENTS:",
  "- Single-row results or scalar aggregations (COUNT, AVG, SUM): use KPICard components in a Grid",
  "- Top-N rankings or categorical comparisons: use BarChart",
  "- Time series with year/date columns: use LineChart",
  "- Proportional data (parts of a whole): use PieChart",
  "- Results with latitude/longitude columns: use MapView",
  "- Multi-row tabular data without clear visualization: use DataTable",
  "- Combine multiple visualizations when appropriate (e.g., KPIs + chart + table)",
  "LAYOUT RULES:",
  "- Always wrap content in a Stack with direction='vertical' and gap='md'",
  "- Add a Heading with the question or a summary title",
  "- Use Grid with columns=2 or columns=3 for KPI cards",
  "- Use Card to wrap charts and tables",
  "DATA RULES:",
  "- For data arrays in chart/table/map components, use the string '$DATA' as the value",
  "- The frontend will inject the actual query results in place of '$DATA'",
  "- Column names in xKey, yKey, yKeys, latKey, lngKey etc. must match actual column names from the results",
  "NUMBER FORMATTING:",
  "- Danish locale: use period as thousand separator (e.g., 1.234,56)",
  "- For KPICard values, format numbers yourself in the value string",
];
