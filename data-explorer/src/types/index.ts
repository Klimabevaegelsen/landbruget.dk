/**
 * Type definitions for the Data Explorer application
 */

/**
 * Metadata for a single dataset/table
 */
export interface DatasetMetadata {
  /** Internal table name (used in SQL queries) */
  name: string;
  /** Human-readable display name */
  displayName: string;
  /** Description of what the dataset contains */
  description: string;
  /** Full HTTPS URL to the Parquet file on R2 */
  url: string;
  /** Total number of rows in the dataset */
  rowCount: number;
  /** File size in bytes */
  sizeBytes: number;
  /** ISO date string of last update */
  lastUpdated: string;
  /** Number of columns in the dataset */
  columns: number;
  /** Schema information (loaded on demand) */
  schema?: ColumnSchema[];
}

/**
 * Schema information for a table column
 */
export interface ColumnSchema {
  /** Column name */
  column_name: string;
  /** DuckDB type (VARCHAR, INTEGER, DOUBLE, etc.) */
  column_type: string;
  /** Whether the column allows NULL values */
  null: 'YES' | 'NO';
}

/**
 * Manifest file format (stored on R2)
 */
export interface ManifestData {
  /** Manifest format version */
  version: string;
  /** ISO timestamp when manifest was generated */
  generatedAt: string;
  /** List of available datasets */
  datasets: DatasetMetadata[];
}

/**
 * Query execution result
 */
export interface QueryResult<T = Record<string, unknown>> {
  /** Result rows */
  data: T[];
  /** Execution time in milliseconds */
  executionTimeMs?: number;
  /** Number of rows returned */
  rowCount: number;
}

/**
 * Query execution error
 */
export interface QueryError {
  /** Error message from DuckDB */
  message: string;
  /** SQL query that failed */
  query: string;
  /** Error type (syntax, execution, connection) */
  type: 'syntax' | 'execution' | 'connection';
}

/**
 * DuckDB connection state
 */
export type ConnectionState = 'disconnected' | 'initializing' | 'ready' | 'error';

/**
 * Supported data types in results
 */
export type DataValue = string | number | boolean | null | object;
