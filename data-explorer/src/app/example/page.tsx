'use client';

import { useState } from 'react';
import { DatasetBrowser } from '@/components/DatasetBrowser';
import { SQLEditor } from '@/components/SQLEditor';
import { ResultsTable } from '@/components/ResultsTable';
import { AskInput } from '@/components/AskInput';
import { executeQuery, registerParquetTable } from '@/lib/duckdb';
import { getR2BaseUrl } from '@/lib/r2';
import type { DatasetMetadata } from '@/types';

export default function ExamplePage() {
  const [selectedDataset, setSelectedDataset] = useState<DatasetMetadata | null>(null);
  const [queryResults, setQueryResults] = useState<Record<string, unknown>[]>([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentQuery, setCurrentQuery] = useState<string>('');

  const R2_BASE_URL = getR2BaseUrl();

  async function handleTableSelect(dataset: DatasetMetadata) {
    setSelectedDataset(dataset);
    setError(null);

    // Register the table in DuckDB if not already done
    try {
      await registerParquetTable(dataset.name, dataset.url);
    } catch (err) {
      console.error('Error registering table:', err);
    }

    // Update query when dataset is selected
    const defaultQuery = `SELECT * FROM ${dataset.name} LIMIT 100;`;
    setCurrentQuery(defaultQuery);
  }

  function handleSqlGenerated(sql: string) {
    // When AI generates SQL, update the editor
    setCurrentQuery(sql);
  }

  async function handleExecuteQuery(query: string) {
    setIsExecuting(true);
    setError(null);
    setQueryResults([]);

    try {
      const results = await executeQuery(query);
      setQueryResults(results);
    } catch (err) {
      console.error('Query execution failed:', err);
      setError(err instanceof Error ? err.message : 'Query execution failed');
    } finally {
      setIsExecuting(false);
    }
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto py-8 px-4">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2">Data Explorer</h1>
          <p className="text-muted-foreground">
            Browse datasets, write SQL queries, and explore agricultural data
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left sidebar - Dataset browser */}
          <div className="lg:col-span-1">
            <DatasetBrowser
              r2BaseUrl={R2_BASE_URL}
              onTableSelect={handleTableSelect}
            />
          </div>

          {/* Main content - AI input, SQL editor and results */}
          <div className="lg:col-span-2 space-y-6">
            <AskInput
              onSqlGenerated={handleSqlGenerated}
              disabled={!selectedDataset}
            />

            <SQLEditor
              initialQuery={currentQuery}
              onExecute={handleExecuteQuery}
              isExecuting={isExecuting}
              disabled={!selectedDataset}
            />

            <ResultsTable
              data={queryResults}
              loading={isExecuting}
              error={error}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
