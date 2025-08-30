'use client';
import * as React from 'react';
import {
  ColumnDef,
  SortingState,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { DataTablePagination } from './data-table-pagination';
import { cn } from '@/lib/utils';
import { ArrowTopRightOnSquareIcon } from '@heroicons/react/24/outline';

interface DataTableProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[];
  data: TData[];
  filterable: boolean;
  yearFilter?: {
    enabled: boolean;
    availableYears: number[];
    selectedYear?: number;
    onYearChange?: (year: number) => void;
  };
}

export function DynamicDataTable<TData, TValue>({
  columns,
  data,
  filterable,
  yearFilter,
}: DataTableProps<TData, TValue>) {
  const [sorting, setSorting] = React.useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = React.useState<string>('');

  const rowCount = data.length;

  // Helper function to format cell values based on column format
  const formatCellValue = (
    value: unknown,
    format?: string
  ): React.ReactNode => {
    if (value === null || value === undefined) return '';

    switch (format) {
      case 'decimal':
        return typeof value === 'number' ? value.toFixed(2) : String(value);
      case 'coordinate':
        return typeof value === 'number' ? value.toFixed(6) : String(value);
      case 'boolean':
        return value ? 'Ja' : 'Nej';
      case 'link':
        return (
          <a
            href={String(value)}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center text-blue-600 underline hover:text-blue-800"
          >
            Se luftfoto
            <ArrowTopRightOnSquareIcon className="ml-1 h-3 w-3" />
          </a>
        );
      default:
        return String(value);
    }
  };

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    onSortingChange: setSorting,
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: 'includesString',
    state: {
      sorting,
      globalFilter,
    },
    onGlobalFilterChange: setGlobalFilter,
  });

  return (
    <div className="">
      {/* Year Filter and Search Controls */}
      {(yearFilter?.enabled || filterable) && (
        <div className="flex items-center gap-4 py-4">
          {yearFilter?.enabled && (
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium">År:</span>
              <div className="flex gap-1">
                {yearFilter.availableYears.map((year) => (
                  <Button
                    key={year}
                    variant={
                      yearFilter.selectedYear === year ? 'default' : 'outline'
                    }
                    size="sm"
                    onClick={() => yearFilter.onYearChange?.(year)}
                    className="h-8 px-3 text-xs"
                  >
                    {year}
                  </Button>
                ))}
              </div>
            </div>
          )}
          {filterable && (
            <Input
              placeholder="Filtrering"
              value={globalFilter ?? ''}
              onChange={(e) => table.setGlobalFilter(String(e.target.value))}
              className="max-w-sm"
            />
          )}
        </div>
      )}
      <div
        className={cn(
          'rounded border border-slate-300',
          rowCount > 10 && 'min-h-[575px]'
        )}
      >
        <Table>
          <TableHeader className="">
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  return (
                    <TableHead key={header.id}>
                      {header.isPlaceholder
                        ? null
                        : flexRender(
                            header.column.columnDef.header,
                            header.getContext()
                          )}
                    </TableHead>
                  );
                })}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map((row, index) => (
                <TableRow
                  index={index}
                  key={row.id}
                  data-state={row.getIsSelected() && 'selected'}
                >
                  {row.getVisibleCells().map((cell) => {
                    const columnDef = cell.column.columnDef as ColumnDef<
                      TData,
                      TValue
                    > & { meta?: { format?: string } };
                    const format = columnDef.meta?.format;
                    const cellValue = cell.getValue();

                    return (
                      <TableCell key={cell.id}>
                        {format
                          ? formatCellValue(cellValue, format)
                          : flexRender(
                              cell.column.columnDef.cell,
                              cell.getContext()
                            )}
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="h-24 text-center"
                >
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      {rowCount > 10 && <DataTablePagination table={table} />}
    </div>
  );
}
