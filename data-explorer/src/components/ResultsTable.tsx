"use client";

import { useState, useMemo } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import {
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
  Download,
  Loader2,
  AlertTriangle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { exportToCSV } from "@/lib/duckdb";

interface ResultsTableProps {
  data: Record<string, unknown>[];
  loading?: boolean;
  error?: string | null;
  className?: string;
}

export function ResultsTable({
  data,
  loading = false,
  error = null,
  className,
}: ResultsTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    if (data.length === 0) return [];
    const firstRow = data[0];
    return Object.keys(firstRow).map((key) => ({
      accessorKey: key,
      header: ({ column }) => (
        <button
          type="button"
          onClick={() => column.toggleSorting()}
          className="flex items-center gap-1.5 transition-colors"
          style={{ color: "var(--muted-foreground)" }}
        >
          <span className="truncate text-[10px] font-semibold tracking-[0.12em] uppercase">
            {key}
          </span>
          {column.getIsSorted() === "asc" ? (
            <ChevronUp className="h-3 w-3 flex-shrink-0" style={{ color: "var(--primary)" }} />
          ) : column.getIsSorted() === "desc" ? (
            <ChevronDown className="h-3 w-3 flex-shrink-0" style={{ color: "var(--primary)" }} />
          ) : (
            <ChevronsUpDown className="h-3 w-3 flex-shrink-0 opacity-30" />
          )}
        </button>
      ),
      cell: ({ getValue }) => {
        const value = getValue();

        if (value === null || value === undefined) {
          return (
            <span className="text-[11px] italic" style={{ color: "var(--muted-foreground)" }}>
              null
            </span>
          );
        }

        if (typeof value === "boolean") {
          return (
            <span
              className="text-[11px] font-semibold"
              style={{
                fontFamily: "var(--font-geist-mono)",
                color: value ? "#2d7a4f" : "#c53030",
              }}
            >
              {value.toString()}
            </span>
          );
        }

        if (typeof value === "number") {
          return (
            <span
              className="block text-right text-[11px] tabular-nums"
              style={{
                fontFamily: "var(--font-geist-mono)",
                color: "var(--foreground)",
              }}
            >
              {new Intl.NumberFormat("da-DK").format(value)}
            </span>
          );
        }

        if (typeof value === "object") {
          return (
            <span
              className="block truncate text-[11px]"
              style={{
                fontFamily: "var(--font-geist-mono)",
                color: "var(--muted-foreground)",
              }}
            >
              {JSON.stringify(value)}
            </span>
          );
        }

        return (
          <span className="block truncate text-[12px]" style={{ color: "var(--foreground)" }}>
            {String(value)}
          </span>
        );
      },
    }));
  }, [data]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 50 } },
  });

  function handleExportCSV() {
    if (data.length === 0) return;
    const timestamp = new Date().toISOString().split("T")[0];
    exportToCSV(data, `query_results_${timestamp}.csv`);
  }

  if (loading) {
    return (
      <div className={cn("py-12", className)}>
        <div className="flex flex-col items-center gap-3">
          <Loader2 className="h-6 w-6 animate-spin" style={{ color: "var(--primary)" }} />
          <span
            className="text-[11px] tracking-wider uppercase"
            style={{ color: "var(--muted-foreground)" }}
          >
            Executing query
          </span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div
        className={cn("border-l-2 pl-4 py-2", className)}
        style={{ borderColor: "var(--destructive)" }}
      >
        <div className="flex items-start gap-2.5">
          <AlertTriangle
            className="mt-0.5 h-4 w-4 flex-shrink-0"
            style={{ color: "var(--destructive)" }}
          />
          <div>
            <p className="mb-2 text-xs font-semibold" style={{ color: "var(--destructive)" }}>
              Query Error
            </p>
            <pre
              className="text-[11px] whitespace-pre-wrap"
              style={{
                fontFamily: "var(--font-geist-mono)",
                color: "var(--muted-foreground)",
              }}
            >
              {error}
            </pre>
          </div>
        </div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className={cn("py-12 text-center", className)}>
        <p
          className="text-[11px] tracking-wider uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          Run a query to see results
        </p>
      </div>
    );
  }

  const { pageIndex, pageSize } = table.getState().pagination;
  const from = pageIndex * pageSize + 1;
  const to = Math.min((pageIndex + 1) * pageSize, data.length);

  return (
    <div className={cn("", className)}>
      {/* Header row */}
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span
            className="text-[10px] font-semibold tracking-[0.2em] uppercase"
            style={{ color: "var(--muted-foreground)" }}
          >
            Results
          </span>
          <span
            className="text-[10px] tabular-nums"
            style={{
              fontFamily: "var(--font-geist-mono)",
              color: "var(--muted-foreground)",
            }}
          >
            {new Intl.NumberFormat("da-DK").format(data.length)} rows
          </span>
        </div>
        <button
          type="button"
          onClick={handleExportCSV}
          className="flex items-center gap-1.5 text-[10px] tracking-wider uppercase transition-colors"
          style={{ color: "var(--muted-foreground)" }}
        >
          <Download className="h-3 w-3" />
          CSV
        </button>
      </div>

      {/* Table */}
      <div className="overflow-hidden border" style={{ borderColor: "var(--border)" }}>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr
                  key={headerGroup.id}
                  style={{
                    background: "var(--muted)",
                    borderBottom: `1px solid var(--border)`,
                  }}
                >
                  {headerGroup.headers.map((header) => (
                    <th key={header.id} className="px-4 py-2.5 text-left">
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row, idx) => (
                <tr
                  key={row.id}
                  className="transition-colors"
                  style={{
                    background:
                      idx % 2 === 0
                        ? "var(--background)"
                        : "color-mix(in oklch, var(--muted) 30%, var(--background))",
                    borderBottom: `1px solid var(--border)`,
                  }}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="max-w-xs px-4 py-2">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {table.getPageCount() > 1 && (
        <div className="mt-3 flex items-center justify-between">
          <span
            className="text-[10px] tabular-nums"
            style={{
              fontFamily: "var(--font-geist-mono)",
              color: "var(--muted-foreground)",
            }}
          >
            {from}–{to} of {new Intl.NumberFormat("da-DK").format(data.length)}
          </span>

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => table.setPageIndex(0)}
              disabled={!table.getCanPreviousPage()}
              className="text-[10px] tracking-wider uppercase transition-colors disabled:opacity-30"
              style={{ color: "var(--muted-foreground)" }}
            >
              ⟨⟨
            </button>
            <button
              type="button"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
              className="text-[11px] transition-colors disabled:opacity-30"
              style={{ color: "var(--muted-foreground)" }}
            >
              ←
            </button>
            <span
              className="text-[10px] tabular-nums"
              style={{
                fontFamily: "var(--font-geist-mono)",
                color: "var(--foreground)",
              }}
            >
              {pageIndex + 1} / {table.getPageCount()}
            </span>
            <button
              type="button"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
              className="text-[11px] transition-colors disabled:opacity-30"
              style={{ color: "var(--muted-foreground)" }}
            >
              →
            </button>
            <button
              type="button"
              onClick={() => table.setPageIndex(table.getPageCount() - 1)}
              disabled={!table.getCanNextPage()}
              className="text-[10px] tracking-wider uppercase transition-colors disabled:opacity-30"
              style={{ color: "var(--muted-foreground)" }}
            >
              ⟩⟩
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
