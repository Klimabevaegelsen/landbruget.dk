'use client';

import { useState, useCallback } from 'react';
import Link from 'next/link';
import {
  ArrowLeft,
  Table2,
  Sparkles,
  LayoutGrid,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react';
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
      setCurrentQuery(`SELECT * FROM ${dataset.name} LIMIT 100;`);
    } catch (err) {
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
    if (meta) setLastQuestion(meta);
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
            results: results.slice(0, 500),
          }),
        });
        const data = await response.json();
        if (!response.ok)
          throw new Error(data.error || 'Failed to generate visualization');
        setUISpec(injectData(data.spec, results));
      } catch (err) {
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
      if (lastQuestion && results.length > 0) {
        generateVisualization(results, lastQuestion, query);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query execution failed');
    } finally {
      setIsExecuting(false);
    }
  }

  const hasResults = queryResults.length > 0;
  const hasSmartView = uiSpec || isGeneratingUI || uiError;

  return (
    <div
      className="flex min-h-screen flex-col"
      style={{ background: 'var(--background)' }}
    >
      {/* ── Header ─────────────────────────────────────── */}
      <header
        className="sticky top-0 z-50 border-b"
        style={{
          borderColor: 'var(--border)',
          background: 'color-mix(in oklch, var(--background) 95%, transparent)',
          backdropFilter: 'blur(8px)',
        }}
      >
        <div className="flex h-11 items-center gap-4 px-5">
          {/* Left: back link */}
          <Link
            href="/"
            className="flex items-center gap-1.5 transition-colors"
            style={{ color: 'var(--muted-foreground)' }}
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            <span className="text-[10px] font-semibold tracking-[0.18em] uppercase">
              Landbruget.dk
            </span>
          </Link>

          {/* Divider */}
          <div className="h-4 w-px" style={{ background: 'var(--border)' }} />

          {/* Sidebar toggle */}
          <button
            type="button"
            onClick={() => setSidebarOpen(!sidebarOpen)}
            className="hidden items-center gap-1.5 transition-colors lg:flex"
            style={{ color: 'var(--muted-foreground)' }}
            aria-label="Toggle sidebar"
          >
            {sidebarOpen ? (
              <PanelLeftClose className="h-4 w-4" />
            ) : (
              <PanelLeftOpen className="h-4 w-4" />
            )}
          </button>

          {/* Right: active dataset indicator + page label */}
          <div className="ml-auto flex items-center gap-4">
            {selectedDataset && (
              <div className="flex items-center gap-2">
                <span
                  className="h-1.5 w-1.5 rounded-full"
                  style={{ background: 'var(--primary)' }}
                />
                <span
                  className="text-[11px] font-semibold"
                  style={{
                    fontFamily: 'var(--font-geist-mono)',
                    color: 'var(--primary)',
                  }}
                >
                  {selectedDataset.name}
                </span>
              </div>
            )}
            <span
              className="text-[10px] font-bold tracking-[0.2em] uppercase"
              style={{ color: 'var(--foreground)' }}
            >
              Explorer
            </span>
          </div>

          {/* Mobile sidebar toggle */}
          <button
            type="button"
            onClick={() => setSidebarOpen(!sidebarOpen)}
            className="text-[10px] font-semibold tracking-wider uppercase transition-colors lg:hidden"
            style={{ color: 'var(--muted-foreground)' }}
          >
            {sidebarOpen ? 'Hide' : 'Datasets'}
          </button>
        </div>
      </header>

      {/* ── Body ───────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">
        {/* ── Sidebar ──────────────────────────────────── */}
        <aside
          className={cn(
            'flex flex-col flex-shrink-0 border-r',
            'transition-all duration-300 ease-in-out',
            'fixed lg:static inset-0 top-11 z-40',
            sidebarOpen
              ? 'translate-x-0 w-72'
              : '-translate-x-full lg:translate-x-0 lg:w-0 lg:border-r-0 lg:overflow-hidden'
          )}
          style={{ borderColor: 'var(--border)', background: 'var(--card)' }}
        >
          {/* Green top accent bar */}
          <div
            className="h-0.5 flex-shrink-0"
            style={{ background: 'var(--primary)' }}
          />
          <div className="flex-1 overflow-y-auto p-5">
            <DatasetBrowser
              r2BaseUrl={R2_BASE_URL}
              onTableSelect={handleTableSelect}
            />
          </div>
        </aside>

        {/* Mobile overlay */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 z-30 lg:hidden"
            style={{ background: 'rgba(0,0,0,0.25)' }}
            onClick={() => setSidebarOpen(false)}
            aria-hidden="true"
          />
        )}

        {/* ── Main panel ───────────────────────────────── */}
        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-3xl space-y-8 px-6 pt-8 pb-16">
            {/* Ask section */}
            <section>
              <AskInput onSqlGenerated={handleSqlGenerated} />
            </section>

            {/* Section rule */}
            <div
              className="border-t"
              style={{ borderColor: 'var(--border)' }}
            />

            {/* Active dataset banner */}
            {selectedDataset && (
              <div
                className="flex items-center justify-between border-l-2 py-0.5 pl-4"
                style={{ borderColor: 'var(--primary)' }}
              >
                <div className="flex items-center gap-3">
                  <span
                    className="text-sm font-semibold"
                    style={{
                      fontFamily: 'var(--font-geist-mono)',
                      color: 'var(--primary)',
                    }}
                  >
                    {selectedDataset.name}
                  </span>
                  <span
                    className="text-xs"
                    style={{ color: 'var(--muted-foreground)' }}
                  >
                    {new Intl.NumberFormat('da-DK').format(
                      selectedDataset.rowCount
                    )}{' '}
                    rows
                    {' · '}
                    {selectedDataset.columns} cols
                  </span>
                </div>
                <button
                  type="button"
                  onClick={() => setSelectedDataset(null)}
                  className="text-[10px] font-semibold tracking-wider uppercase transition-colors"
                  style={{ color: 'var(--muted-foreground)' }}
                >
                  Clear
                </button>
              </div>
            )}

            {/* SQL Editor */}
            <section>
              <SQLEditor
                initialQuery={currentQuery}
                onExecute={handleExecuteQuery}
                isExecuting={isExecuting}
                disabled={false}
              />
            </section>

            {/* View mode tabs */}
            {hasResults && hasSmartView && (
              <div
                className="flex items-center gap-0 border-b"
                style={{ borderColor: 'var(--border)' }}
              >
                {[
                  { mode: 'smart' as ViewMode, icon: Sparkles, label: 'Smart' },
                  { mode: 'table' as ViewMode, icon: Table2, label: 'Table' },
                  { mode: 'both' as ViewMode, icon: LayoutGrid, label: 'Both' },
                ].map(({ mode, icon: Icon, label }) => (
                  <button
                    key={mode}
                    onClick={() => setViewMode(mode)}
                    className={cn(
                      'flex items-center gap-1.5 px-4 py-2 text-[10px] uppercase tracking-[0.15em] font-semibold',
                      'border-b-2 -mb-px transition-colors'
                    )}
                    style={
                      viewMode === mode
                        ? {
                            borderColor: 'var(--primary)',
                            color: 'var(--primary)',
                          }
                        : {
                            borderColor: 'transparent',
                            color: 'var(--muted-foreground)',
                          }
                    }
                  >
                    <Icon className="h-3 w-3" />
                    {label}
                  </button>
                ))}
              </div>
            )}

            {/* Results */}
            <div>
              {(viewMode === 'smart' || viewMode === 'both') &&
                hasSmartView && (
                  <SmartResults
                    spec={uiSpec}
                    loading={isGeneratingUI}
                    error={uiError}
                  />
                )}
              {(viewMode === 'table' ||
                viewMode === 'both' ||
                !hasSmartView) && (
                <ResultsTable
                  data={queryResults}
                  loading={isExecuting}
                  error={error}
                />
              )}
            </div>
          </div>

          {/* Slim footer */}
          <footer
            className="border-t px-6 py-3"
            style={{ borderColor: 'var(--border)' }}
          >
            <div
              className="flex items-center justify-between text-[10px] tracking-wider uppercase"
              style={{ color: 'var(--muted-foreground)' }}
            >
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="transition-colors hover:text-[var(--foreground)]"
              >
                Landbruget.dk
              </a>
              <span>DuckDB-WASM · Gemini AI</span>
            </div>
          </footer>
        </main>
      </div>
    </div>
  );
}
