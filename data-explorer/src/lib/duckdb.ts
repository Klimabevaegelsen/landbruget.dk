/**
 * DuckDB-WASM initialization and query utilities for browser-based data exploration
 *
 * This module provides a singleton DuckDB connection that can read Parquet files
 * from R2 storage and execute SQL queries in the browser.
 */

import * as duckdb from "@duckdb/duckdb-wasm";

/** Sanitize a SQL identifier by removing characters that aren't alphanumeric or underscores */
function sanitizeIdentifier(name: string): string {
  return name.replace(/[^a-zA-Z0-9_]/g, "");
}

// DuckDB connection singleton
let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;
let isInitializing = false;

/**
 * Initialize DuckDB-WASM with HTTPS and S3 file system support
 * Loads the required WASM bundles from CDN
 */
export async function initDuckDB(): Promise<void> {
  // Return if already initialized
  if (db !== null) return;

  // Prevent multiple concurrent initializations
  if (isInitializing) {
    while (isInitializing) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    return;
  }

  try {
    isInitializing = true;

    // Select WASM bundle based on browser capabilities
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

    // Select appropriate bundle (eh = exception handling, coi = cross-origin isolation)
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    // Create worker
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      }),
    );
    const worker = new Worker(worker_url);

    // Initialize logger
    const logger = new duckdb.ConsoleLogger();

    // Instantiate DuckDB
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // Create connection
    conn = await db.connect();

    // Install and load extensions
    await conn.query(`INSTALL httpfs;`);
    await conn.query(`LOAD httpfs;`);
    try {
      await conn.query(`INSTALL spatial;`);
      await conn.query(`LOAD spatial;`);
      console.info("Spatial extension loaded");
    } catch (e) {
      console.warn("Spatial extension not available in this DuckDB-WASM build:", e);
    }

    // Configure S3 settings for public R2 bucket (no authentication required)
    await conn.query(`
      SET s3_region='auto';
      SET s3_url_style='path';
      SET s3_use_ssl=true;
    `);

    console.info("DuckDB initialized successfully");
  } catch (error) {
    console.error("Failed to initialize DuckDB:", error);
    throw new Error(
      `DuckDB initialization failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  } finally {
    isInitializing = false;
  }
}

/**
 * Execute a SQL query and return results as an array of objects
 * Automatically initializes DuckDB if not already initialized
 *
 * @param query - SQL query string (DuckDB SQL dialect)
 * @returns Array of result rows as objects
 * @throws Error if query execution fails
 */
export async function executeQuery<T = Record<string, unknown>>(query: string): Promise<T[]> {
  // Initialize if needed
  if (!conn) {
    await initDuckDB();
  }

  if (!conn) {
    throw new Error("DuckDB connection not available");
  }

  try {
    // Execute query
    const result = await conn.query(query);

    // Convert to array of objects
    const rows: T[] = [];
    for (let i = 0; i < result.numRows; i++) {
      const row = result.get(i);
      if (row) {
        rows.push(row as T);
      }
    }

    return rows;
  } catch (error) {
    console.error("Query execution failed:", error);
    throw new Error(`Query failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Register a Parquet file from R2 as a DuckDB table
 * The file will be accessible for querying without downloading the entire file
 *
 * @param tableName - Name to use for the table in DuckDB
 * @param parquetUrl - Full HTTPS URL to the Parquet file on R2
 * @throws Error if registration fails
 */
export async function registerParquetTable(tableName: string, parquetUrl: string): Promise<void> {
  if (!conn) {
    await initDuckDB();
  }

  if (!conn) {
    throw new Error("DuckDB connection not available");
  }

  try {
    // Sanitize inputs to prevent SQL injection
    const safeName = sanitizeIdentifier(tableName);
    const safeUrl = parquetUrl.replace(/'/g, "''");

    // Create view directly from Parquet URL
    // DuckDB will stream data as needed
    await conn.query(`
      CREATE OR REPLACE VIEW "${safeName}" AS
      SELECT * FROM read_parquet('${safeUrl}');
    `);

    console.info(`Registered table '${tableName}' from ${parquetUrl}`);
  } catch (error) {
    console.error(`Failed to register table '${tableName}':`, error);
    throw new Error(
      `Table registration failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Get table schema information
 *
 * @param tableName - Name of the table to inspect
 * @returns Array of column information (name, type, nullable)
 */
export async function getTableSchema(
  tableName: string,
): Promise<Array<{ column_name: string; column_type: string; null: string }>> {
  const safeName = sanitizeIdentifier(tableName);
  return executeQuery<{
    column_name: string;
    column_type: string;
    null: string;
  }>(`DESCRIBE "${safeName}";`);
}

/**
 * Get a preview of table data (first N rows)
 *
 * @param tableName - Name of the table
 * @param limit - Number of rows to return (default: 10)
 * @returns Array of sample rows
 */
export async function getTablePreview<T = Record<string, unknown>>(
  tableName: string,
  limit: number = 10,
): Promise<T[]> {
  const safeName = sanitizeIdentifier(tableName);
  return executeQuery<T>(`SELECT * FROM "${safeName}" LIMIT ${Math.max(0, Math.floor(limit))};`);
}

/**
 * Get count of rows in a table
 *
 * @param tableName - Name of the table
 * @returns Number of rows
 */
export async function getTableRowCount(tableName: string): Promise<number> {
  const safeName = sanitizeIdentifier(tableName);
  const result = await executeQuery<{ count: number }>(
    `SELECT COUNT(*) as count FROM "${safeName}";`,
  );
  return result[0]?.count ?? 0;
}

/**
 * Close the DuckDB connection and clean up resources
 * Should be called when the application unmounts
 */
export async function closeDuckDB(): Promise<void> {
  try {
    if (conn) {
      await conn.close();
      conn = null;
    }
    if (db) {
      await db.terminate();
      db = null;
    }
    console.info("DuckDB connection closed");
  } catch (error) {
    console.error("Error closing DuckDB:", error);
  }
}

/**
 * Check if DuckDB is initialized and ready
 */
export function isDuckDBReady(): boolean {
  return db !== null && conn !== null;
}

/**
 * Export query results to CSV format
 *
 * @param data - Query result data
 * @param filename - Name for the CSV file
 */
export function exportToCSV(data: Record<string, unknown>[], filename: string): void {
  if (data.length === 0) {
    console.warn("No data to export");
    return;
  }

  // Get headers from first row
  const headers = Object.keys(data[0]);

  // Create CSV content
  const csvRows = [
    headers.join(","), // Header row
    ...data.map((row) =>
      headers
        .map((header) => {
          const value = row[header];
          // Handle strings with commas or quotes
          if (typeof value === "string" && (value.includes(",") || value.includes('"'))) {
            return `"${value.replace(/"/g, '""')}"`;
          }
          return value ?? "";
        })
        .join(","),
    ),
  ];

  const csvContent = csvRows.join("\n");

  // Create blob and download
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename.endsWith(".csv") ? filename : `${filename}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}
