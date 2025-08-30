'use client';

import { BaseDataGrid } from '@/services/supabase/types';
import { DynamicDataTable } from '@/components/table/dynamic-table';
import { ColumnDef } from '@tanstack/react-table';
import {
  ArrowsUpDownIcon,
  ArrowUpIcon,
  ArrowDownIcon,
} from '@heroicons/react/24/outline';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';

export function BlockTable({ grid }: { grid: BaseDataGrid }) {
  const { isInCategoryWithData } = useCategoryDataContext();

  // Check if this table should show a placeholder
  const placeholderDataType = shouldShowPlaceholder(grid._key);
  if (placeholderDataType) {
    return <PlaceholderChart dataType={placeholderDataType} />;
  }

  // Check if table has no data - only show placeholder if not in category with data
  if (!grid.rows || grid.rows.length === 0) {
    if (!isInCategoryWithData) {
      return <NoDataPlaceholder />;
    } else {
      return (
        <div className="py-8 text-center text-gray-500">
          Ingen data tilgængelig for denne tabel
        </div>
      );
    }
  }
  const columns: ColumnDef<Record<string, string | number | boolean>>[] =
    grid.columns.map((col) => ({
      accessorKey: col.key,
      header: ({ column }) => {
        return (
          <div
            onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
            className="group flex cursor-pointer items-center"
          >
            {col.label}
            {column.getIsSorted() === 'asc' ? (
              <ArrowUpIcon className="ml-2 size-3 text-black" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDownIcon className="ml-2 size-3 text-black" />
            ) : (
              <div className="ml-2 size-3">
                <ArrowsUpDownIcon className="hidden group-hover:block" />
              </div>
            )}
          </div>
        );
      },
      meta: {
        format: (col as { format?: string }).format, // Pass format information to column
      },
    }));

  return (
    <div className="">
      <DynamicDataTable
        columns={columns}
        data={grid.rows}
        filterable={grid.allowFiltering}
      />
    </div>
  );
}
