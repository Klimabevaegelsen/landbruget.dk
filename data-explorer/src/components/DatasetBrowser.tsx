"use client";

import { useState, useEffect, useMemo } from "react";
import { ChevronDown, Search, AlertTriangle, Layers } from "lucide-react";
import { cn } from "@/lib/utils";
import { registerParquetTable, getTableSchema } from "@/lib/duckdb";
import type { DatasetMetadata, ManifestData } from "@/types";

interface DatasetBrowserProps {
  r2BaseUrl: string;
  manifestPath?: string;
  onTableSelect: (dataset: DatasetMetadata) => void;
  className?: string;
}

export function DatasetBrowser({
  r2BaseUrl,
  manifestPath = "manifest.json",
  onTableSelect,
  className,
}: DatasetBrowserProps) {
  const [datasets, setDatasets] = useState<DatasetMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [expandedDataset, setExpandedDataset] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  const filteredDatasets = useMemo(() => {
    if (!search.trim()) return datasets;
    const q = search.toLowerCase();
    return datasets.filter(
      (d) => d.name.toLowerCase().includes(q) || d.displayName.toLowerCase().includes(q),
    );
  }, [datasets, search]);

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
        setError(err instanceof Error ? err.message : "Failed to load datasets");
      } finally {
        setLoading(false);
      }
    }
    loadManifest();
  }, [r2BaseUrl, manifestPath]);

  async function handleDatasetExpand(dataset: DatasetMetadata) {
    if (expandedDataset === dataset.name) {
      setExpandedDataset(null);
      return;
    }
    setExpandedDataset(dataset.name);

    if (!dataset.schema) {
      try {
        await registerParquetTable(dataset.name, dataset.url);
        const schema = await getTableSchema(dataset.name);
        setDatasets((prev) =>
          prev.map((d) =>
            d.name === dataset.name ? { ...d, schema: schema as DatasetMetadata["schema"] } : d,
          ),
        );
      } catch (err) {
        console.error("Error loading schema:", err);
      }
    }
  }

  function handleTableSelect(dataset: DatasetMetadata) {
    setSelectedDataset(dataset.name);
    onTableSelect(dataset);
  }

  function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
  }

  function formatNumber(num: number): string {
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
    if (num >= 1_000) return `${(num / 1_000).toFixed(0)}K`;
    return new Intl.NumberFormat("da-DK").format(num);
  }

  if (loading) {
    return (
      <div className={cn("space-y-0", className)}>
        {/* Header */}
        <div className="mb-5 flex items-center justify-between">
          <span
            className="text-[10px] font-semibold tracking-[0.2em] uppercase"
            style={{ color: "var(--muted-foreground)" }}
          >
            Datasets
          </span>
          <span className="text-[10px]" style={{ color: "var(--muted-foreground)" }}>
            Loading…
          </span>
        </div>
        {/* Skeleton rows */}
        <div className="space-y-0">
          {[1, 2, 3, 4, 5].map((i) => (
            <div
              key={i}
              className="animate-pulse border-b py-3"
              style={{ borderColor: "var(--border)" }}
            >
              <div className="mb-1.5 h-3 w-3/4 rounded" style={{ background: "var(--muted)" }} />
              <div className="h-2.5 w-1/2 rounded" style={{ background: "var(--muted)" }} />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={cn("py-4", className)}>
        <div className="flex items-start gap-2.5">
          <AlertTriangle
            className="mt-0.5 h-4 w-4 flex-shrink-0"
            style={{ color: "var(--destructive)" }}
          />
          <div>
            <p className="mb-1 text-xs font-semibold" style={{ color: "var(--destructive)" }}>
              Error Loading Datasets
            </p>
            <p className="text-xs" style={{ color: "var(--muted-foreground)" }}>
              {error}
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (datasets.length === 0) {
    return (
      <div className={cn("py-8 text-center", className)}>
        <Layers className="mx-auto mb-3 h-8 w-8" style={{ color: "var(--muted-foreground)" }} />
        <p className="text-xs" style={{ color: "var(--muted-foreground)" }}>
          No datasets available
        </p>
      </div>
    );
  }

  return (
    <div className={cn("", className)}>
      {/* Section header */}
      <div className="mb-4 flex items-center justify-between">
        <span
          className="text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          Datasets
        </span>
        <span
          className="text-[10px] tabular-nums"
          style={{
            fontFamily: "var(--font-geist-mono)",
            color: "var(--muted-foreground)",
          }}
        >
          {filteredDatasets.length}
        </span>
      </div>

      {/* Search */}
      <div className="relative mb-4">
        <Search
          className="pointer-events-none absolute top-1/2 left-0 h-3.5 w-3.5 -translate-y-1/2"
          style={{ color: "var(--muted-foreground)" }}
        />
        <input
          type="text"
          placeholder="Filter…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full border-b bg-transparent pr-2 pb-1.5 pl-5 text-xs transition-colors outline-none"
          style={{
            borderColor: search ? "var(--primary)" : "var(--border)",
            color: "var(--foreground)",
          }}
        />
      </div>

      {/* Dataset list */}
      <div>
        {filteredDatasets.map((dataset) => {
          const isSelected = selectedDataset === dataset.name;
          const isExpanded = expandedDataset === dataset.name;

          return (
            <div
              key={dataset.name}
              className={cn("border-b transition-colors", isSelected && "border-l-2 pl-2")}
              style={{
                borderColor: "var(--border)",
                ...(isSelected
                  ? {
                      borderLeftColor: "var(--primary)",
                      borderBottomColor: "var(--border)",
                    }
                  : {}),
              }}
            >
              {/* Row: expand trigger */}
              <button
                type="button"
                onClick={() => handleDatasetExpand(dataset)}
                className="group w-full py-3 text-left"
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    {/* Table name in mono */}
                    <div
                      className="mb-0.5 truncate text-[12px] leading-tight font-semibold"
                      style={{
                        fontFamily: "var(--font-geist-mono)",
                        color: isSelected ? "var(--primary)" : "var(--foreground)",
                      }}
                    >
                      {dataset.name}
                    </div>
                    {/* Display name / description */}
                    <div
                      className="truncate text-[11px] leading-tight"
                      style={{ color: "var(--muted-foreground)" }}
                    >
                      {dataset.displayName}
                    </div>
                    {/* Stats row */}
                    <div
                      className="mt-1.5 flex items-center gap-2 text-[10px] tabular-nums"
                      style={{
                        fontFamily: "var(--font-geist-mono)",
                        color: "var(--muted-foreground)",
                      }}
                    >
                      <span>{formatNumber(dataset.rowCount)} rows</span>
                      <span className="h-2.5 w-px" style={{ background: "var(--border)" }} />
                      <span>{dataset.columns} cols</span>
                      <span className="h-2.5 w-px" style={{ background: "var(--border)" }} />
                      <span>{formatBytes(dataset.sizeBytes)}</span>
                    </div>
                  </div>
                  <ChevronDown
                    className={cn(
                      "mt-0.5 h-3.5 w-3.5 flex-shrink-0 transition-transform duration-200",
                      isExpanded && "rotate-180",
                    )}
                    style={{ color: "var(--muted-foreground)" }}
                  />
                </div>
              </button>

              {/* Schema expansion */}
              {isExpanded && (
                <div className="pb-3" style={{ borderTop: `1px solid var(--border)` }}>
                  {dataset.schema ? (
                    <div className="pt-2.5">
                      {/* Schema column list */}
                      <div className="mb-3 space-y-0">
                        {dataset.schema.map((col) => (
                          <div
                            key={col.column_name}
                            className="flex items-baseline justify-between gap-2 py-1"
                          >
                            <span
                              className="truncate text-[11px] font-medium"
                              style={{
                                fontFamily: "var(--font-geist-mono)",
                                color: "var(--foreground)",
                              }}
                            >
                              {col.column_name}
                            </span>
                            <span
                              className="flex-shrink-0 text-[10px]"
                              style={{
                                fontFamily: "var(--font-geist-mono)",
                                color: "var(--muted-foreground)",
                              }}
                            >
                              {col.column_type}
                              {col.null === "YES" && <span className="ml-1 opacity-60">?</span>}
                            </span>
                          </div>
                        ))}
                      </div>

                      {/* Select button */}
                      <button
                        type="button"
                        onClick={() => handleTableSelect(dataset)}
                        className={cn(
                          "w-full py-2 text-[11px] font-semibold uppercase tracking-wider transition-colors",
                          isSelected
                            ? "border border-transparent"
                            : "border border-[var(--border)] hover:border-[var(--primary)]",
                        )}
                        style={
                          isSelected
                            ? {
                                background: "var(--primary)",
                                color: "var(--primary-foreground)",
                              }
                            : {
                                background: "transparent",
                                color: "var(--foreground)",
                              }
                        }
                      >
                        {isSelected ? "✓ Active" : "Query"}
                      </button>
                    </div>
                  ) : (
                    <div
                      className="flex items-center gap-2 pt-2.5 text-[11px]"
                      style={{ color: "var(--muted-foreground)" }}
                    >
                      <div
                        className="h-3 w-3 animate-spin rounded-full border-2 border-t-transparent"
                        style={{ borderColor: "var(--primary)" }}
                      />
                      Loading schema…
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
