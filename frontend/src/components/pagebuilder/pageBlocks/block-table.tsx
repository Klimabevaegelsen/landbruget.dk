"use client";

import { BaseDataGrid } from "@/services/supabase/types";
import { DynamicDataTable } from "@/components/table/dynamic-table";
import { ColumnDef } from "@tanstack/react-table";
import {
  ArrowsUpDownIcon,
  ArrowUpIcon,
  ArrowDownIcon,
} from "@heroicons/react/24/outline";

export function BlockTable({ grid }: { grid: BaseDataGrid }) {
  const columns: ColumnDef<Record<string, string | number | boolean>>[] =
    grid.columns.map((col) => ({
      accessorKey: col.key,
      header: ({ column }) => {
        return (
          <div
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
            className="flex items-center cursor-pointer group"
          >
            {col.label}
            {column.getIsSorted() === "asc" ? (
              <ArrowUpIcon className="ml-2 size-3 text-black" />
            ) : column.getIsSorted() === "desc" ? (
              <ArrowDownIcon className="ml-2 size-3 text-black" />
            ) : (
              <div className="ml-2 size-3">
                <ArrowsUpDownIcon className="hidden group-hover:block" />
              </div>
            )}
          </div>
        );
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
