'use client';

import { useState, useEffect, useMemo } from 'react';
import { Database, Table, FileText, ChevronRight, Search } from 'lucide-react';
import { cn } from '@/lib/utils';
import { registerParquetTable, getTableSchema } from '@/lib/duckdb';
import type { DatasetMetadata, ManifestData } from '@/types';

interface DatasetBrowserProps {
  r2BaseUrl: string;
  manifestPath?: string;
  onTableSelect: (dataset: DatasetMetadata) => void;
  className?: string;
}

export function DatasetBrowser({
  r2BaseUrl,
  manifestPath = 'manifest.json',
  onTableSelect,
  className,
}: DatasetBrowserProps) {
  const [datasets, setDatasets] = useState<DatasetMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [expandedDataset, setExpandedDataset] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  const filteredDatasets = useMemo(() => {
    if (!search.trim()) return datasets;
    const q = search.toLowerCase();
    return datasets.filter(d =>
      d.name.toLowerCase().includes(q) || d.displayName.toLowerCase().includes(q)
    );
  }, [datasets, search]);

  // Load manifest on mount
  useEffect(() => {
    async function loadManifest() {
      try {
        setLoading(true);
        setError(null);

        const manifestUrl = `${r2BaseUrl}/${manifestPath}`;
        const response = await fetch(manifestUrl);

        if (!response.ok) {
          throw new Error(`Failed to load manifest: ${response.statusText}`);
        }

        const manifest: ManifestData = await response.json();
        setDatasets(manifest.datasets);
      } catch (err) {
        console.error('Error loading manifest:', err);
        setError(err instanceof Error ? err.message : 'Failed to load datasets');
      } finally {
        setLoading(false);
      }
    }

    loadManifest();
  }, [r2BaseUrl, manifestPath]);

  // Load schema when dataset is expanded
  async function handleDatasetExpand(dataset: DatasetMetadata) {
    if (expandedDataset === dataset.name) {
      setExpandedDataset(null);
      return;
    }

    setExpandedDataset(dataset.name);

    // Load schema if not already loaded
    if (!dataset.schema) {
      try {
        // Register table in DuckDB
        await registerParquetTable(dataset.name, dataset.url);

        // Get schema
        const schema = await getTableSchema(dataset.name);

        // Update dataset with schema
        setDatasets(prev =>
          prev.map(d =>
            d.name === dataset.name ? { ...d, schema: schema as DatasetMetadata['schema'] } : d
          )
        );
      } catch (err) {
        console.error('Error loading schema:', err);
      }
    }
  }

  function handleTableSelect(dataset: DatasetMetadata) {
    setSelectedDataset(dataset.name);
    onTableSelect(dataset);
  }

  function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  }

  function formatNumber(num: number): string {
    return new Intl.NumberFormat('da-DK').format(num);
  }

  if (loading) {
    return (
      <div className={cn('space-y-4', className)}>
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-5 w-5 animate-pulse" />
          <span>Loading datasets...</span>
        </div>
        {[1, 2, 3].map(i => (
          <div key={i} className="h-24 bg-muted animate-pulse rounded-lg" />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className={cn('rounded-lg border border-destructive bg-destructive/10 p-4', className)}>
        <div className="flex items-start gap-3">
          <FileText className="h-5 w-5 text-destructive mt-0.5" />
          <div>
            <h3 className="font-semibold text-destructive mb-1">Error Loading Datasets</h3>
            <p className="text-sm text-muted-foreground">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (datasets.length === 0) {
    return (
      <div className={cn('rounded-lg border border-dashed p-8 text-center', className)}>
        <Database className="h-12 w-12 text-muted-foreground mx-auto mb-3" />
        <h3 className="font-semibold mb-1">No Datasets Available</h3>
        <p className="text-sm text-muted-foreground">
          No datasets found in the manifest.
        </p>
      </div>
    );
  }

  return (
    <div className={cn('space-y-3', className)}>
      <div className="flex items-center gap-2 mb-4">
        <Database className="h-5 w-5 text-primary" />
        <h2 className="text-lg font-semibold">Available Datasets</h2>
        <span className="text-sm text-muted-foreground">({filteredDatasets.length})</span>
      </div>

      <div className="relative mb-4">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
        <input
          type="text"
          placeholder="Search datasets..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="w-full pl-9 pr-3 py-2 text-sm rounded-lg border bg-background focus:outline-none focus:ring-2 focus:ring-primary"
        />
      </div>

      <div className="space-y-2">
        {filteredDatasets.map(dataset => (
          <div
            key={dataset.name}
            className={cn(
              'rounded-lg border transition-all',
              selectedDataset === dataset.name
                ? 'border-primary bg-primary/5'
                : 'border-border bg-card hover:bg-accent/50'
            )}
          >
            {/* Dataset Header */}
            <button
              type="button"
              onClick={() => handleDatasetExpand(dataset)}
              className="w-full p-4 text-left flex items-start gap-3"
            >
              <ChevronRight
                className={cn(
                  'h-5 w-5 text-muted-foreground mt-0.5 transition-transform shrink-0',
                  expandedDataset === dataset.name && 'rotate-90'
                )}
              />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <Table className="h-4 w-4 text-primary shrink-0" />
                  <h3 className="font-semibold truncate">{dataset.displayName}</h3>
                </div>
                <p className="text-sm text-muted-foreground line-clamp-2 mb-2">
                  {dataset.description}
                </p>
                <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                  <span>{formatNumber(dataset.rowCount)} rows</span>
                  <span>{dataset.columns} columns</span>
                  <span>{formatBytes(dataset.sizeBytes)}</span>
                </div>
              </div>
            </button>

            {/* Schema Details (when expanded) */}
            {expandedDataset === dataset.name && (
              <div className="border-t px-4 pb-4">
                {dataset.schema ? (
                  <div className="space-y-3">
                    <div className="pt-3">
                      <h4 className="text-sm font-semibold mb-2">Schema</h4>
                      <div className="space-y-1">
                        {dataset.schema.map(col => (
                          <div
                            key={col.column_name}
                            className="flex items-center gap-2 text-sm bg-muted/50 rounded px-2 py-1"
                          >
                            <span className="font-mono text-primary">{col.column_name}</span>
                            <span className="text-muted-foreground">:</span>
                            <span className="text-muted-foreground font-mono text-xs">
                              {col.column_type}
                            </span>
                            {col.null === 'YES' && (
                              <span className="text-xs text-muted-foreground ml-auto">nullable</span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => handleTableSelect(dataset)}
                      className={cn(
                        'w-full py-2 px-4 rounded-lg font-semibold text-sm transition-colors',
                        selectedDataset === dataset.name
                          ? 'bg-primary text-primary-foreground'
                          : 'bg-secondary text-secondary-foreground hover:bg-secondary/80'
                      )}
                    >
                      {selectedDataset === dataset.name ? 'Selected' : 'Query This Dataset'}
                    </button>
                  </div>
                ) : (
                  <div className="py-3 text-sm text-muted-foreground flex items-center gap-2">
                    <div className="h-4 w-4 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                    <span>Loading schema...</span>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
