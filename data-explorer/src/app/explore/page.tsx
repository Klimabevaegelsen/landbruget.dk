'use client';

import { useState } from 'react';
import Link from 'next/link';
import { ArrowLeft, Menu, X } from 'lucide-react';
import { DatasetBrowser } from '@/components/DatasetBrowser';
import { AskInput } from '@/components/AskInput';
import { SQLEditor } from '@/components/SQLEditor';
import { ResultsTable } from '@/components/ResultsTable';
import { executeQuery, registerParquetTable } from '@/lib/duckdb';
import { cn } from '@/lib/utils';
import type { DatasetMetadata } from '@/types';

export default function ExplorePage() {
  const [selectedDataset, setSelectedDataset] = useState<DatasetMetadata | null>(null);
  const [currentQuery, setCurrentQuery] = useState<string>('');
  const [queryResults, setQueryResults] = useState<Record<string, unknown>[]>([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);

  // R2 base URL - should match your actual R2 bucket
  const R2_BASE_URL = process.env.NEXT_PUBLIC_R2_BASE_URL || 'https://your-r2-bucket.r2.dev';

  async function handleTableSelect(dataset: DatasetMetadata) {
    setSelectedDataset(dataset);
    setError(null);

    try {
      await registerParquetTable(dataset.name, dataset.url);
      const defaultQuery = `SELECT * FROM ${dataset.name} LIMIT 100;`;
      setCurrentQuery(defaultQuery);
    } catch (err) {
      console.error('Error registering table:', err);
      setError(err instanceof Error ? err.message : 'Failed to register table');
    }
  }

  async function handleSqlGenerated(sql: string, tableUrls?: Record<string, string>) {
    // Register all tables the AI referenced before setting the query
    if (tableUrls) {
      for (const [name, url] of Object.entries(tableUrls)) {
        try {
          await registerParquetTable(name, url);
        } catch (err) {
          console.error(`Failed to register table ${name}:`, err);
        }
      }
    }
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
    <div className="min-h-screen bg-background flex flex-col">
      {/* Header */}
      <header className="border-b bg-card shadow-sm sticky top-0 z-50">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="inline-flex items-center gap-2 text-sm font-semibold text-muted-foreground hover:text-foreground transition-colors"
              >
                <ArrowLeft className="h-4 w-4" />
                <span className="hidden sm:inline">Back to Home</span>
              </Link>
              <div className="h-6 w-px bg-border hidden sm:block" />
              <h1 className="text-xl font-bold">Data Explorer</h1>
            </div>

            {/* Mobile sidebar toggle */}
            <button
              type="button"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="lg:hidden p-2 rounded-lg hover:bg-accent transition-colors"
              aria-label="Toggle sidebar"
            >
              {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Sidebar - Dataset Browser */}
        <aside
          className={cn(
            'w-full lg:w-80 xl:w-96 bg-card border-r flex-shrink-0',
            'transition-all duration-300 ease-in-out',
            'fixed lg:static inset-0 top-[73px] z-40',
            sidebarOpen
              ? 'translate-x-0'
              : '-translate-x-full lg:translate-x-0 lg:w-0 lg:border-r-0'
          )}
        >
          <div className="h-full overflow-y-auto">
            <div className="p-6">
              <DatasetBrowser
                r2BaseUrl={R2_BASE_URL}
                onTableSelect={handleTableSelect}
              />
            </div>
          </div>
        </aside>

        {/* Overlay for mobile */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-black/50 z-30 lg:hidden"
            onClick={() => setSidebarOpen(false)}
            aria-hidden="true"
          />
        )}

        {/* Main Panel */}
        <main className="flex-1 overflow-y-auto">
          <div className="container mx-auto px-4 py-6 max-w-7xl space-y-6">
            {/* Natural Language Input — always visible */}
            <AskInput
              onSqlGenerated={handleSqlGenerated}
            />

            {/* Selected dataset info (optional) */}
            {selectedDataset && (
              <div className="rounded-lg border bg-card p-4 shadow-sm">
                <div className="flex items-start justify-between">
                  <div>
                    <h2 className="text-sm font-semibold mb-0.5">
                      Active table: <span className="font-mono text-primary">{selectedDataset.name}</span>
                    </h2>
                    <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                      <span>{new Intl.NumberFormat('da-DK').format(selectedDataset.rowCount)} rows</span>
                      <span>{selectedDataset.columns} columns</span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setSelectedDataset(null)}
                    className="text-muted-foreground hover:text-foreground transition-colors text-xs"
                  >
                    Clear
                  </button>
                </div>
              </div>
            )}

            {/* SQL Editor */}
            <SQLEditor
              initialQuery={currentQuery}
              onExecute={handleExecuteQuery}
              isExecuting={isExecuting}
              disabled={false}
            />

            {/* Results Table */}
            <ResultsTable
              data={queryResults}
              loading={isExecuting}
              error={error}
            />
          </div>
        </main>
      </div>

      {/* Footer */}
      <footer className="border-t bg-card">
        <div className="container mx-auto px-4 py-4">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-muted-foreground">
            <div>
              <span>Powered by </span>
              <a
                href="https://duckdb.org"
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-foreground hover:text-primary transition-colors"
              >
                DuckDB-WASM
              </a>
              <span> and </span>
              <a
                href="https://ai.google.dev/gemini-api"
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-foreground hover:text-primary transition-colors"
              >
                Gemini AI
              </a>
            </div>
            <div>
              Part of{' '}
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-foreground hover:text-primary transition-colors"
              >
                Landbruget.dk
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}
