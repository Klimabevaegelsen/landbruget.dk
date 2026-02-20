# Data Explorer - DuckDB-WASM Browser SQL Interface

A Next.js application that enables users to explore and query Parquet files stored on Cloudflare R2 using DuckDB-WASM directly in the browser.

## Architecture

This data explorer allows users to:
1. Browse available datasets from a manifest
2. Write and execute SQL queries using DuckDB SQL dialect
3. View results with sorting and pagination
4. Export results to CSV

All data processing happens **in the browser** using DuckDB-WASM. The Parquet files are streamed from R2 on-demand (no full download required).

## Core Components

### 1. `/src/lib/duckdb.ts` - DuckDB Initialization

Manages the DuckDB-WASM connection lifecycle:

```typescript
import { initDuckDB, executeQuery, registerParquetTable } from '@/lib/duckdb';

// Initialize (happens automatically on first use)
await initDuckDB();

// Register a Parquet file as a queryable table
await registerParquetTable('my_table', 'https://r2-bucket.dev/data.parquet');

// Execute SQL queries
const results = await executeQuery('SELECT * FROM my_table LIMIT 100');
```

**Key Features**:
- Singleton connection pattern (prevents multiple initializations)
- Automatic httpfs extension loading for remote file access
- Connection pooling and error handling
- CSV export utility

### 2. `/src/components/DatasetBrowser.tsx` - Dataset Browser

Displays available datasets from a manifest file:

```typescript
<DatasetBrowser
  r2BaseUrl="https://your-bucket.r2.dev"
  manifestPath="manifest.json"
  onTableSelect={(dataset) => handleSelect(dataset)}
/>
```

**Features**:
- Loads dataset list from `manifest.json` on R2
- Shows metadata (row count, columns, size)
- Expandable schema preview
- One-click table selection for querying

**Expected Manifest Format**:
```json
{
  "version": "1.0",
  "generatedAt": "2024-01-10T19:00:00Z",
  "datasets": [
    {
      "name": "table_name",
      "displayName": "Human Readable Name",
      "description": "What this dataset contains",
      "url": "https://r2-bucket.dev/path/to/file.parquet",
      "rowCount": 123456,
      "sizeBytes": 5242880,
      "lastUpdated": "2024-01-10",
      "columns": 15
    }
  ]
}
```

### 3. `/src/components/SQLEditor.tsx` - SQL Query Editor

CodeMirror-based SQL editor with syntax highlighting:

```typescript
<SQLEditor
  initialQuery="SELECT * FROM table LIMIT 100"
  onExecute={(query) => handleExecute(query)}
  isExecuting={isLoading}
  disabled={!tableSelected}
/>
```

**Features**:
- SQL syntax highlighting (DuckDB dialect)
- Line numbers and code folding
- Cmd/Ctrl+Enter keyboard shortcut to execute
- Dark theme (One Dark)
- Query statistics (lines, characters)

### 4. `/src/components/ResultsTable.tsx` - Results Display

Data table with sorting, pagination, and export:

```typescript
<ResultsTable
  data={queryResults}
  loading={isExecuting}
  error={errorMessage}
/>
```

**Features**:
- Column sorting (click headers)
- Pagination (50 rows per page)
- Type-aware rendering (numbers, booleans, null)
- CSV export button
- Loading and error states

## Usage Example

See `/src/app/example/page.tsx` for a complete integration example:

```typescript
'use client';

import { useState } from 'react';
import { DatasetBrowser } from '@/components/DatasetBrowser';
import { SQLEditor } from '@/components/SQLEditor';
import { ResultsTable } from '@/components/ResultsTable';
import { executeQuery, registerParquetTable } from '@/lib/duckdb';

export default function ExplorerPage() {
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);

  async function handleExecute(query: string) {
    setLoading(true);
    try {
      const data = await executeQuery(query);
      setResults(data);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="grid grid-cols-3 gap-6">
      <DatasetBrowser
        r2BaseUrl="https://your-r2.dev"
        onTableSelect={(ds) => registerParquetTable(ds.name, ds.url)}
      />
      <div className="col-span-2">
        <SQLEditor onExecute={handleExecute} isExecuting={loading} />
        <ResultsTable data={results} loading={loading} />
      </div>
    </div>
  );
}
```

## DuckDB SQL Dialect

DuckDB supports standard SQL with additional features:

```sql
-- Basic queries
SELECT * FROM table_name LIMIT 100;

-- Filtering and aggregation
SELECT category, COUNT(*) as count, AVG(value) as avg_value
FROM table_name
WHERE date >= '2024-01-01'
GROUP BY category
ORDER BY count DESC;

-- Joins
SELECT a.*, b.name
FROM table_a a
JOIN table_b b ON a.id = b.id;

-- Window functions
SELECT *,
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank
FROM table_name;

-- JSON operations (if columns contain JSON)
SELECT json_extract(metadata, '$.field') as extracted
FROM table_name;
```

See [DuckDB SQL Documentation](https://duckdb.org/docs/sql/introduction) for full reference.

## Performance Considerations

### Browser Limitations
- **Memory**: DuckDB-WASM runs in browser memory. Large result sets (>100k rows) may cause performance issues.
- **Loading**: First query may be slower while DuckDB initializes (~2-3 seconds).
- **Streaming**: Parquet files are streamed on-demand, so queries only download necessary columns/rows.

### Best Practices
1. **Use LIMIT**: Always limit results in development (`LIMIT 1000`)
2. **Select Specific Columns**: `SELECT col1, col2` instead of `SELECT *`
3. **Filter Early**: Use WHERE clauses to reduce data scanned
4. **Pagination**: Use LIMIT/OFFSET for large result sets
5. **Aggregation**: Do aggregations in DuckDB, not in JavaScript

### Example Manifest Generation

You can generate the manifest file using a script:

```python
import json
from pathlib import Path
import duckdb

def generate_manifest(parquet_files, output_path):
    datasets = []

    for file_path in parquet_files:
        # Get metadata from Parquet file
        conn = duckdb.connect()
        result = conn.execute(f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT *) as column_count
            FROM read_parquet('{file_path}')
        """).fetchone()

        file_size = Path(file_path).stat().st_size

        datasets.append({
            "name": Path(file_path).stem,
            "displayName": Path(file_path).stem.replace('_', ' ').title(),
            "description": "Description here",
            "url": f"https://your-r2.dev/{Path(file_path).name}",
            "rowCount": result[0],
            "sizeBytes": file_size,
            "lastUpdated": "2024-01-10",
            "columns": result[1]
        })

    manifest = {
        "version": "1.0",
        "generatedAt": "2024-01-10T19:00:00Z",
        "datasets": datasets
    }

    with open(output_path, 'w') as f:
        json.dump(manifest, f, indent=2)
```

## React 19 Patterns

This application follows React 19 best practices:

- **Client Components**: All interactive components use `'use client'` directive
- **Functional Components**: Pure functions with explicit prop types
- **TypeScript Strict Mode**: No `any` types, full type safety
- **Tailwind CSS v4**: Utility-first styling with `cn()` helper
- **Accessibility**: Semantic HTML, ARIA labels, keyboard navigation

## Dependencies

- `@duckdb/duckdb-wasm`: DuckDB WebAssembly build
- `@codemirror/*`: Code editor with SQL syntax highlighting
- `@tanstack/react-table`: Powerful table component
- `lucide-react`: Icon library
- `clsx` + `tailwind-merge`: Class name utilities

## Development

```bash
# Install dependencies
npm install

# Run dev server
npm run dev

# Build for production
npm run build

# Run production build
npm run start
```

## Deployment Considerations

### R2 CORS Configuration

Your R2 bucket must allow CORS from your domain:

```json
{
  "AllowOrigins": ["https://your-domain.com"],
  "AllowMethods": ["GET", "HEAD"],
  "AllowHeaders": ["Range"],
  "ExposeHeaders": ["Accept-Ranges", "Content-Length", "Content-Range"],
  "MaxAgeSeconds": 3600
}
```

### Cloudflare Workers (Optional)

For private buckets, you can add a Cloudflare Worker to proxy requests with authentication:

```javascript
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.pathname.slice(1);

    const object = await env.MY_BUCKET.get(key);
    if (!object) return new Response('Not Found', { status: 404 });

    return new Response(object.body, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Access-Control-Allow-Origin': '*',
      },
    });
  },
};
```

## Future Enhancements

Potential improvements:
- [ ] Query history and saved queries
- [ ] Query builder UI (visual query construction)
- [ ] Chart visualization for query results
- [ ] Advanced filtering UI
- [ ] Share query results via URL
- [ ] Multi-table joins with visual schema
- [ ] Integration with Gemini AI for natural language queries

## Troubleshooting

### "Failed to initialize DuckDB"
- Check browser console for detailed error
- Ensure browser supports WebAssembly
- Try disabling browser extensions

### "Query failed: HTTP error"
- Verify R2 URL is accessible (test in browser)
- Check CORS configuration on R2 bucket
- Ensure Parquet file is valid

### "Out of memory"
- Reduce result set size with LIMIT
- Select fewer columns
- Close other browser tabs
- Try on a device with more RAM

## License

This component is part of the Landbruget.dk project.
