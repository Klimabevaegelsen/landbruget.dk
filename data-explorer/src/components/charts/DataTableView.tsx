'use client';

import { useMemo, useState } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  flexRender,
  type SortingState,
  type ColumnDef,
} from '@tanstack/react-table';

interface DataTableViewProps {
  data: Record<string, unknown>[];
  columns?: string[] | null;
  pageSize?: number | null;
}

export function DataTableView({
  data,
  columns: columnFilter,
  pageSize = 25,
}: DataTableViewProps) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    if (data.length === 0) return [];
    const keys = columnFilter ?? Object.keys(data[0]);
    return keys.map((key) => ({
      accessorKey: key,
      header: key,
      cell: ({ getValue }) => {
        const val = getValue();
        if (val === null || val === undefined) {
          return <span className="text-muted-foreground italic">null</span>;
        }
        if (typeof val === 'number') {
          return new Intl.NumberFormat('da-DK').format(val);
        }
        if (typeof val === 'boolean') {
          return (
            <span className={val ? 'text-green-600' : 'text-red-500'}>
              {String(val)}
            </span>
          );
        }
        return String(val);
      },
    }));
  }, [data, columnFilter]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: pageSize ?? 25 } },
  });

  if (data.length === 0) {
    return (
      <p className="text-muted-foreground p-4 text-sm">No data to display.</p>
    );
  }

  return (
    <div>
      <div className="overflow-x-auto rounded-lg border">
        <table className="w-full text-sm">
          <thead className="bg-muted/50">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    className="text-muted-foreground hover:text-foreground cursor-pointer px-3 py-2 text-left font-medium whitespace-nowrap select-none"
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
                    {header.column.getIsSorted() === 'asc' && ' ↑'}
                    {header.column.getIsSorted() === 'desc' && ' ↓'}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id} className="hover:bg-muted/30 border-t">
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-3 py-2 whitespace-nowrap">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {table.getPageCount() > 1 && (
        <div className="text-muted-foreground mt-3 flex items-center justify-between text-xs">
          <span>
            Page {table.getState().pagination.pageIndex + 1} of{' '}
            {table.getPageCount()} ({data.length.toLocaleString('da-DK')} rows)
          </span>
          <div className="flex gap-1">
            <button
              type="button"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
              className="hover:bg-muted rounded border px-2 py-1 disabled:opacity-30"
            >
              Previous
            </button>
            <button
              type="button"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
              className="hover:bg-muted rounded border px-2 py-1 disabled:opacity-30"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
