export interface Spec {
  root: string;
  elements: Record<
    string,
    {
      type: string;
      props: Record<string, unknown>;
      children: string[];
    }
  >;
}

/**
 * Replace "$DATA" placeholders in spec props with actual query results.
 * The AI generates specs with "$DATA" as a placeholder for data arrays;
 * this function injects the real DuckDB results before rendering.
 */
export function injectData(spec: Spec, data: Record<string, unknown>[]): Spec {
  const injected = structuredClone(spec);
  for (const element of Object.values(injected.elements)) {
    if (element.props) {
      for (const [key, value] of Object.entries(element.props)) {
        if (value === '$DATA') {
          element.props[key] = data;
        }
      }
    }
  }
  return injected;
}

/**
 * Summarize query results for the AI prompt.
 * Sends column info and sample rows instead of the full dataset.
 */
export function summarizeResults(
  data: Record<string, unknown>[],
  maxSampleRows = 10
): {
  rowCount: number;
  columns: { name: string; type: string; sample: unknown }[];
  sampleRows: Record<string, unknown>[];
} {
  if (data.length === 0) {
    return { rowCount: 0, columns: [], sampleRows: [] };
  }

  const firstRow = data[0];
  const columns = Object.entries(firstRow).map(([name, value]) => ({
    name,
    type: inferType(value),
    sample: value,
  }));

  return {
    rowCount: data.length,
    columns,
    sampleRows: data.slice(0, maxSampleRows),
  };
}

function inferType(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'number')
    return Number.isInteger(value) ? 'integer' : 'float';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'string') {
    if (/^\d{4}-\d{2}-\d{2}/.test(value)) return 'date';
    if (/^-?\d+\.?\d*$/.test(value)) return 'numeric_string';
    return 'string';
  }
  if (typeof value === 'object') return 'object';
  return 'unknown';
}
