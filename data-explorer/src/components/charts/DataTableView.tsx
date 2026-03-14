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
      <div
        className="overflow-x-auto border"
        style={{ borderColor: 'var(--border)' }}
      >
        <table className="w-full border-collapse">
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr
                key={headerGroup.id}
                style={{
                  background: 'var(--muted)',
                  borderBottom: '1px solid var(--border)',
                }}
              >
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    className="cursor-pointer px-3 py-2.5 text-left whitespace-nowrap select-none"
                  >
                    <span
                      className="text-[10px] font-semibold tracking-[0.12em] uppercase"
                      style={{ color: 'var(--muted-foreground)' }}
                    >
                      {flexRender(
                        header.column.columnDef.header,
                        header.getContext()
                      )}
                      {header.column.getIsSorted() === 'asc' && ' ↑'}
                      {header.column.getIsSorted() === 'desc' && ' ↓'}
                    </span>
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row, idx) => (
              <tr
                key={row.id}
                style={{
                  background:
                    idx % 2 === 0
                      ? 'var(--background)'
                      : 'color-mix(in oklch, var(--muted) 30%, var(--background))',
                  borderBottom: '1px solid var(--border)',
                }}
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className="px-3 py-2 text-[12px] whitespace-nowrap"
                    style={{ color: 'var(--foreground)' }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {table.getPageCount() > 1 && (
        <div className="mt-3 flex items-center justify-between">
          <span
            className="text-[10px] tabular-nums"
            style={{
              fontFamily: 'var(--font-geist-mono)',
              color: 'var(--muted-foreground)',
            }}
          >
            {table.getState().pagination.pageIndex + 1} / {table.getPageCount()}{' '}
            ({data.length.toLocaleString('da-DK')} rows)
          </span>
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
              className="text-[11px] transition-colors disabled:opacity-30"
              style={{ color: 'var(--muted-foreground)' }}
            >
              ←
            </button>
            <button
              type="button"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
              className="text-[11px] transition-colors disabled:opacity-30"
              style={{ color: 'var(--muted-foreground)' }}
            >
              →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
