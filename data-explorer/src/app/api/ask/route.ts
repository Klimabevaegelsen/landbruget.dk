import { GoogleGenerativeAI } from '@google/generative-ai';
import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const R2_BASE_URL = process.env.NEXT_PUBLIC_R2_BASE_URL || 'https://pub-b8c2f72ba51b4fe6804e9bb92280567c.r2.dev';

type Dataset = {
  name: string;
  displayName: string;
  description: string;
  layer: string;
  columns: number;
  rowCount: number;
  url: string;
  schema?: Array<{ column_name: string; column_type: string }>;
};

type Manifest = { datasets: Dataset[] };

type CatalogColumn = {
  description: string;
  notes?: string;
};

type CatalogEntry = {
  name: string;
  description: string;
  columns?: Record<string, CatalogColumn>;
  columnDescriptions?: Record<string, CatalogColumn>;
  columnStats?: Record<string, {
    min?: string;
    max?: string;
    nullPct?: number;
    distinctCount?: number;
    sampleValues?: string[];
    isEmpty?: boolean;
  }>;
};

type Catalog = { datasets: CatalogEntry[] };

// Cache manifest and catalog in memory
let manifestCache: { data: Manifest; fetchedAt: number } | null = null;
let catalogCache: { data: Catalog; fetchedAt: number } | null = null;

async function fetchManifest(): Promise<Manifest | null> {
  const now = Date.now();
  if (manifestCache && now - manifestCache.fetchedAt < 5 * 60 * 1000) {
    return manifestCache.data;
  }
  const res = await fetch(`${R2_BASE_URL}/manifest.json`);
  if (!res.ok) return null;
  const data = await res.json();
  manifestCache = { data, fetchedAt: now };
  return data;
}

async function fetchCatalog(): Promise<Catalog | null> {
  const now = Date.now();
  if (catalogCache && now - catalogCache.fetchedAt < 5 * 60 * 1000) {
    return catalogCache.data;
  }
  try {
    const res = await fetch(`${R2_BASE_URL}/data_catalog.json`);
    if (!res.ok) return null;
    const data = await res.json();
    catalogCache = { data, fetchedAt: now };
    return data;
  } catch {
    return null;
  }
}

function loadSchemaDoc(filename: string): string {
  try {
    const p = path.join(process.cwd(), '..', 'schema', filename);
    return fs.readFileSync(p, 'utf-8');
  } catch {
    return '';
  }
}

// Stage 1: lightweight catalogue — just names + descriptions, no columns
function buildCatalogue(datasets: Dataset[]): string {
  return datasets
    .map(ds => `- ${ds.name}: ${ds.displayName}. ${ds.description} (${ds.rowCount.toLocaleString()} rows)`)
    .join('\n');
}

// Stage 2: full schema for selected tables, enriched with catalog column descriptions
function buildDetailedSchema(datasets: Dataset[], catalogByName: Record<string, CatalogEntry>): string {
  return datasets.map(ds => {
    const catalogEntry = catalogByName[ds.name];
    const colDescs = catalogEntry?.columnDescriptions ?? catalogEntry?.columns ?? {};
    const colStats = catalogEntry?.columnStats ?? {};

    let tableDesc = catalogEntry?.description || ds.description;

    const cols = ds.schema?.length
      ? ds.schema.map(c => {
          const desc = colDescs[c.column_name];
          const stats = colStats[c.column_name];
          let line = `  ${c.column_name} (${c.column_type})`;
          if (stats?.isEmpty) {
            line += ` — WARNING: this column contains no real data in this table (all null/zero)`;
          } else {
            if (desc?.description) line += ` — ${desc.description}`;
            if (desc?.notes) line += ` [NOTE: ${desc.notes}]`;
            if (stats?.sampleValues?.length) line += ` [e.g. ${stats.sampleValues.slice(0, 3).join(', ')}]`;
            if (stats?.min !== undefined && stats?.max !== undefined && stats.min && stats.max) {
              line += ` [range: ${stats.min}–${stats.max}]`;
            }
          }
          return line;
        }).join('\n')
      : '  (schema unavailable)';

    return `TABLE: ${ds.name}\n  Display: ${ds.displayName}\n  Description: ${tableDesc}\n  Rows: ${ds.rowCount.toLocaleString()}\nCOLUMNS:\n${cols}`;
  }).join('\n\n');
}

export async function POST(request: NextRequest) {
  try {
    const apiKey = process.env.GOOGLE_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: 'GOOGLE_API_KEY not configured' }, { status: 503 });
    }

    const body = await request.json();
    const { question } = body;
    if (!question || typeof question !== 'string' || !question.trim()) {
      return NextResponse.json({ error: 'Missing or invalid "question" field' }, { status: 400 });
    }

    const [manifest, catalog, relationships, exampleQueries] = await Promise.all([
      fetchManifest(),
      fetchCatalog(),
      Promise.resolve(loadSchemaDoc('relationships.md')),
      Promise.resolve(loadSchemaDoc('example_queries.md')),
    ]);

    if (!manifest) {
      return NextResponse.json({ error: 'Could not load dataset manifest' }, { status: 503 });
    }

    // Build catalog lookup map (name → entry)
    const catalogByName: Record<string, CatalogEntry> = {};
    if (catalog?.datasets) {
      for (const entry of catalog.datasets) {
        catalogByName[entry.name] = entry;
      }
    }

    const genAI = new GoogleGenerativeAI(apiKey);

    // ── Stage 1: table selection ─────────────────────────────────────────────
    const selectorModel = genAI.getGenerativeModel({ model: 'gemini-flash-latest' });

    const catalogue = buildCatalogue(manifest.datasets);
    const selectionResult = await selectorModel.generateContent(`You are a schema linker for a Danish agricultural database.

Given this question, identify the 1-8 most relevant table names from the catalogue below.
Return ONLY a JSON array of table name strings. No explanation. No markdown.

QUESTION: ${question}

AVAILABLE TABLES:
${catalogue}`);

    let selectedNames: string[] = [];
    try {
      const raw = selectionResult.response.text().trim();
      const cleaned = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
      selectedNames = JSON.parse(cleaned);
      if (!Array.isArray(selectedNames)) selectedNames = [];
    } catch {
      selectedNames = manifest.datasets.map(d => d.name);
    }

    // Resolve selected datasets (guard against hallucinated names)
    const selectedDatasets = selectedNames
      .map(name => manifest.datasets.find(d => d.name === name))
      .filter((d): d is Dataset => d !== undefined);

    const datasetsForSQL = selectedDatasets.length > 0 ? selectedDatasets : manifest.datasets;

    // ── Stage 2: SQL generation ──────────────────────────────────────────────
    const sqlModel = genAI.getGenerativeModel({
      model: 'gemini-flash-latest',
      systemInstruction: `You are an expert SQL query generator for Danish agricultural data stored in DuckDB.

RULES:
1. Only generate SELECT queries — never INSERT, UPDATE, DELETE, DROP, CREATE.
2. Use DuckDB SQL dialect. Table and column names are case-sensitive.
3. Always include LIMIT (default 100 if not specified).
4. Use proper SQL formatting with keywords in UPPERCASE.
5. JOIN multiple tables when the question requires it.
6. Accept questions in Danish and English — reply in the same language.
7. CRITICAL: Reference tables by their registered name only (e.g. FROM field_environmental_analysis_properties_2024). NEVER use parquet URLs or file paths in FROM clauses.
8. CRITICAL: CVR numbers are stored as VARCHAR in some tables and INT32 in others. Always use TRY_CAST when joining CVR across tables, e.g.: TRY_CAST(a.cvr_number AS INTEGER) = b.cvr_number
9. CRITICAL: Do NOT use spatial functions (ST_Area, ST_Intersects, ST_GeomFromWKB, etc.) — the spatial extension is not available in the query engine. Instead, use pre-computed numeric columns. For wetland area queries use field_analysis_field_wetland_intersections_{year} and COUNT(*) or COUNT(DISTINCT wetland_id) — each row represents one wetland intersection. Never select or operate on geometry columns.

${relationships ? `## TABLE RELATIONSHIPS & JOIN PATTERNS\n\n${relationships}\n` : ''}
${exampleQueries ? `## EXAMPLE QUERIES\n\n${exampleQueries}\n` : ''}

RESPONSE FORMAT — always valid JSON, no markdown fences:
{
  "sql": "SELECT ... LIMIT 100;",
  "tables": ["exact_table_name_1", "exact_table_name_2"],
  "explanation": "Brief explanation in the same language as the question."
}`,
    });

    const detailedSchema = buildDetailedSchema(datasetsForSQL, catalogByName);
    const sqlResult = await sqlModel.generateContent(`SELECTED TABLES FOR THIS QUERY:

${detailedSchema}

QUESTION: ${question}

Generate the SQL query. Respond with valid JSON only.`);

    const text = sqlResult.response.text().trim();

    let parsedResponse;
    try {
      const cleaned = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
      parsedResponse = JSON.parse(cleaned);
    } catch {
      return NextResponse.json(
        { error: 'Failed to parse AI response as JSON.', rawResponse: text },
        { status: 500 }
      );
    }

    if (!parsedResponse.sql || !parsedResponse.explanation) {
      return NextResponse.json(
        { error: 'AI response missing "sql" or "explanation".', rawResponse: text },
        { status: 500 }
      );
    }

    if (!parsedResponse.sql.trim().toUpperCase().startsWith('SELECT')) {
      return NextResponse.json(
        { error: 'Generated query is not a SELECT statement.', sql: parsedResponse.sql },
        { status: 400 }
      );
    }

    // Resolve table URLs for the frontend to register in DuckDB
    const tables: string[] = parsedResponse.tables || [];
    const tableUrls: Record<string, string> = {};
    for (const name of tables) {
      const ds = manifest.datasets.find(d => d.name === name);
      if (ds) tableUrls[name] = ds.url;
    }

    return NextResponse.json({ sql: parsedResponse.sql, explanation: parsedResponse.explanation, tables, tableUrls });

  } catch (error) {
    console.error('Error in /api/ask:', error);
    if (error instanceof Error) {
      if (error.message.includes('quota') || error.message.includes('rate limit')) {
        return NextResponse.json({ error: 'API rate limit exceeded. Please try again later.' }, { status: 429 });
      }
      if (error.message.includes('API key') || error.message.includes('authentication')) {
        return NextResponse.json({ error: 'API authentication failed.' }, { status: 401 });
      }
    }
    return NextResponse.json(
      { error: 'Failed to generate SQL query', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
