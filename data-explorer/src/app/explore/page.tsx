'use client';

import { useState, useCallback } from 'react';
import Link from 'next/link';
import { ArrowLeft, Menu, X, Table2, Sparkles, LayoutGrid } from 'lucide-react';
import { DatasetBrowser } from '@/components/DatasetBrowser';
import { AskInput } from '@/components/AskInput';
import { SQLEditor } from '@/components/SQLEditor';
import { ResultsTable } from '@/components/ResultsTable';
import { SmartResults } from '@/components/SmartResults';
import { executeQuery, registerParquetTable } from '@/lib/duckdb';
import { injectData, type Spec } from '@/lib/render/spec-utils';
import { getR2BaseUrl } from '@/lib/r2';
import { cn } from '@/lib/utils';
import type { DatasetMetadata } from '@/types';

type ViewMode = 'smart' | 'table' | 'both';

export default function ExplorePage() {
  const [selectedDataset, setSelectedDataset] =
    useState<DatasetMetadata | null>(null);
  const [currentQuery, setCurrentQuery] = useState<string>('');
  const [queryResults, setQueryResults] = useState<Record<string, unknown>[]>(
    []
  );
  const [isExecuting, setIsExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);

  // Smart visualization state
  const [uiSpec, setUISpec] = useState<Spec | null>(null);
  const [isGeneratingUI, setIsGeneratingUI] = useState(false);
  const [uiError, setUIError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>('smart');
  const [lastQuestion, setLastQuestion] = useState<{
    question: string;
    explanation: string;
  } | null>(null);

  const R2_BASE_URL = getR2BaseUrl();

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

  async function handleSqlGenerated(
    sql: string,
    tableUrls?: Record<string, string>,
    meta?: { question: string; explanation: string }
  ) {
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
    if (meta) {
      setLastQuestion(meta);
    }
  }

  const generateVisualization = useCallback(
    async (
      results: Record<string, unknown>[],
      meta: { question: string; explanation: string },
      sql: string
    ) => {
      setIsGeneratingUI(true);
      setUIError(null);
      setUISpec(null);

      try {
        const response = await fetch('/api/visualize', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            question: meta.question,
            sql,
            explanation: meta.explanation,
            results: results.slice(0, 500), // Limit payload size
          }),
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.error || 'Failed to generate visualization');
        }

        // Inject actual data into spec placeholders
        const specWithData = injectData(data.spec, results);
        setUISpec(specWithData);
      } catch (err) {
        console.error('Visualization generation failed:', err);
        setUIError(
          err instanceof Error
            ? err.message
            : 'Failed to generate visualization'
        );
      } finally {
        setIsGeneratingUI(false);
      }
    },
    []
  );

  async function handleExecuteQuery(query: string) {
    setIsExecuting(true);
    setError(null);
    setQueryResults([]);
    setUISpec(null);
    setUIError(null);

    try {
      const results = await executeQuery(query);
      setQueryResults(results);

      // Generate smart visualization if we have a question context
      if (lastQuestion && results.length > 0) {
        generateVisualization(results, lastQuestion, query);
      }
    } catch (err) {
      console.error('Query execution failed:', err);
      setError(err instanceof Error ? err.message : 'Query execution failed');
    } finally {
      setIsExecuting(false);
    }
  }

  const hasResults = queryResults.length > 0;
  const hasSmartView = uiSpec || isGeneratingUI || uiError;

  return (
    <div className="bg-background flex min-h-screen flex-col">
      {/* Header */}
      <header className="bg-card sticky top-0 z-50 border-b shadow-sm">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="text-muted-foreground hover:text-foreground inline-flex items-center gap-2 text-sm font-semibold transition-colors"
              >
                <ArrowLeft className="h-4 w-4" />
                <span className="hidden sm:inline">Back to Home</span>
              </Link>
              <div className="bg-border hidden h-6 w-px sm:block" />
              <h1 className="text-xl font-bold">Data Explorer</h1>
            </div>

            {/* Mobile sidebar toggle */}
            <button
              type="button"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="hover:bg-accent rounded-lg p-2 transition-colors lg:hidden"
              aria-label="Toggle sidebar"
            >
              {sidebarOpen ? (
                <X className="h-5 w-5" />
              ) : (
                <Menu className="h-5 w-5" />
              )}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex flex-1 overflow-hidden">
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
            className="fixed inset-0 z-30 bg-black/50 lg:hidden"
            onClick={() => setSidebarOpen(false)}
            aria-hidden="true"
          />
        )}

        {/* Main Panel */}
        <main className="flex-1 overflow-y-auto">
          <div className="container mx-auto max-w-7xl space-y-6 px-4 py-6">
            {/* Natural Language Input — always visible */}
            <AskInput onSqlGenerated={handleSqlGenerated} />

            {/* Selected dataset info (optional) */}
            {selectedDataset && (
              <div className="bg-card rounded-lg border p-4 shadow-sm">
                <div className="flex items-start justify-between">
                  <div>
                    <h2 className="mb-0.5 text-sm font-semibold">
                      Active table:{' '}
                      <span className="text-primary font-mono">
                        {selectedDataset.name}
                      </span>
                    </h2>
                    <div className="text-muted-foreground flex flex-wrap gap-3 text-xs">
                      <span>
                        {new Intl.NumberFormat('da-DK').format(
                          selectedDataset.rowCount
                        )}{' '}
                        rows
                      </span>
                      <span>{selectedDataset.columns} columns</span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setSelectedDataset(null)}
                    className="text-muted-foreground hover:text-foreground text-xs transition-colors"
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

            {/* View Mode Toggle */}
            {hasResults && hasSmartView && (
              <div className="bg-muted/50 flex w-fit items-center gap-1 rounded-lg p-1">
                <button
                  onClick={() => setViewMode('smart')}
                  className={cn(
                    'inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
                    viewMode === 'smart'
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <Sparkles className="h-3.5 w-3.5" />
                  Smart View
                </button>
                <button
                  onClick={() => setViewMode('table')}
                  className={cn(
                    'inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
                    viewMode === 'table'
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <Table2 className="h-3.5 w-3.5" />
                  Table
                </button>
                <button
                  onClick={() => setViewMode('both')}
                  className={cn(
                    'inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
                    viewMode === 'both'
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <LayoutGrid className="h-3.5 w-3.5" />
                  Both
                </button>
              </div>
            )}

            {/* Smart Results */}
            {(viewMode === 'smart' || viewMode === 'both') && hasSmartView && (
              <SmartResults
                spec={uiSpec}
                loading={isGeneratingUI}
                error={uiError}
              />
            )}

            {/* Results Table */}
            {(viewMode === 'table' || viewMode === 'both' || !hasSmartView) && (
              <ResultsTable
                data={queryResults}
                loading={isExecuting}
                error={error}
              />
            )}
          </div>
        </main>
      </div>

      {/* Footer */}
      <footer className="border-t">
        <div className="container mx-auto px-4 py-3">
          <div className="text-muted-foreground flex items-center justify-between text-xs">
            <span>
              Part of{' '}
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="hover:text-foreground transition-colors"
              >
                Landbruget.dk
              </a>
            </span>
            <span>DuckDB-WASM · Gemini AI</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
