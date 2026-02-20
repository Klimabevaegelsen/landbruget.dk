'use client';

import { useState, useMemo } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import {
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
  Download,
  Table as TableIcon,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Loader2,
  AlertCircle,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { exportToCSV } from '@/lib/duckdb';

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

  // Generate columns from data
  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    if (data.length === 0) return [];

    const firstRow = data[0];
    return Object.keys(firstRow).map(key => ({
      accessorKey: key,
      header: ({ column }) => {
        return (
          <button
            type="button"
            onClick={() => column.toggleSorting()}
            className="flex items-center gap-2 hover:text-primary transition-colors font-semibold"
          >
            <span className="truncate">{key}</span>
            {column.getIsSorted() === 'asc' ? (
              <ChevronUp className="h-4 w-4" />
            ) : column.getIsSorted() === 'desc' ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronsUpDown className="h-4 w-4 opacity-50" />
            )}
          </button>
        );
      },
      cell: ({ getValue }) => {
        const value = getValue();

        // Handle different value types
        if (value === null || value === undefined) {
          return <span className="text-muted-foreground italic">null</span>;
        }

        if (typeof value === 'boolean') {
          return (
            <span className={cn('font-semibold', value ? 'text-green-600' : 'text-red-600')}>
              {value.toString()}
            </span>
          );
        }

        if (typeof value === 'number') {
          return (
            <span className="font-mono text-right block">
              {new Intl.NumberFormat('da-DK').format(value)}
            </span>
          );
        }

        if (typeof value === 'object') {
          return (
            <code className="text-xs bg-muted px-1 py-0.5 rounded">
              {JSON.stringify(value)}
            </code>
          );
        }

        return <span className="truncate block">{String(value)}</span>;
      },
    }));
  }, [data]);

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
    },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: {
      pagination: {
        pageSize: 50,
      },
    },
  });

  function handleExportCSV() {
    if (data.length === 0) return;

    // Generate filename with timestamp
    const timestamp = new Date().toISOString().split('T')[0];
    const filename = `query_results_${timestamp}.csv`;

    exportToCSV(data, filename);
  }

  // Loading state
  if (loading) {
    return (
      <div className={cn('rounded-lg border p-8', className)}>
        <div className="flex flex-col items-center justify-center gap-3 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span className="font-semibold">Executing query...</span>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className={cn('rounded-lg border border-destructive bg-destructive/10 p-6', className)}>
        <div className="flex items-start gap-3">
          <AlertCircle className="h-5 w-5 text-destructive mt-0.5 shrink-0" />
          <div className="flex-1">
            <h3 className="font-semibold text-destructive mb-1">Query Error</h3>
            <pre className="text-sm text-muted-foreground whitespace-pre-wrap font-mono bg-muted/50 p-3 rounded mt-2">
              {error}
            </pre>
          </div>
        </div>
      </div>
    );
  }

  // Empty state
  if (data.length === 0) {
    return (
      <div className={cn('rounded-lg border border-dashed p-8', className)}>
        <div className="flex flex-col items-center justify-center gap-3 text-muted-foreground">
          <TableIcon className="h-12 w-12" />
          <div className="text-center">
            <h3 className="font-semibold mb-1">No Results</h3>
            <p className="text-sm">
              Run a query to see results here
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <TableIcon className="h-5 w-5 text-primary" />
          <h3 className="text-lg font-semibold">Query Results</h3>
          <span className="text-sm text-muted-foreground">
            ({new Intl.NumberFormat('da-DK').format(data.length)} rows)
          </span>
        </div>
        <button
          type="button"
          onClick={handleExportCSV}
          className={cn(
            'inline-flex items-center gap-2 px-3 py-2 rounded-lg',
            'text-sm font-semibold',
            'bg-secondary text-secondary-foreground',
            'hover:bg-secondary/80 transition-colors',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
          )}
        >
          <Download className="h-4 w-4" />
          <span>Export CSV</span>
        </button>
      </div>

      {/* Table */}
      <div className="rounded-lg border overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead className="bg-muted/50">
              {table.getHeaderGroups().map(headerGroup => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map(header => (
                    <th
                      key={header.id}
                      className="px-4 py-3 text-left text-sm border-b"
                    >
                      {header.isPlaceholder
                        ? null
                        : flexRender(
                            header.column.columnDef.header,
                            header.getContext()
                          )}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row, idx) => (
                <tr
                  key={row.id}
                  className={cn(
                    'hover:bg-accent/50 transition-colors',
                    idx % 2 === 0 ? 'bg-background' : 'bg-muted/20'
                  )}
                >
                  {row.getVisibleCells().map(cell => (
                    <td
                      key={cell.id}
                      className="px-4 py-2 text-sm border-b max-w-md"
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext()
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          Showing {table.getState().pagination.pageIndex * table.getState().pagination.pageSize + 1} to{' '}
          {Math.min(
            (table.getState().pagination.pageIndex + 1) * table.getState().pagination.pageSize,
            data.length
          )}{' '}
          of {new Intl.NumberFormat('da-DK').format(data.length)} rows
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => table.setPageIndex(0)}
            disabled={!table.getCanPreviousPage()}
            className={cn(
              'p-2 rounded-lg border transition-colors',
              'hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed'
            )}
            aria-label="First page"
          >
            <ChevronsLeft className="h-4 w-4" />
          </button>
          <button
            type="button"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
            className={cn(
              'p-2 rounded-lg border transition-colors',
              'hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed'
            )}
            aria-label="Previous page"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>

          <span className="text-sm px-3">
            Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()}
          </span>

          <button
            type="button"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
            className={cn(
              'p-2 rounded-lg border transition-colors',
              'hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed'
            )}
            aria-label="Next page"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
          <button
            type="button"
            onClick={() => table.setPageIndex(table.getPageCount() - 1)}
            disabled={!table.getCanNextPage()}
            className={cn(
              'p-2 rounded-lg border transition-colors',
              'hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed'
            )}
            aria-label="Last page"
          >
            <ChevronsRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
