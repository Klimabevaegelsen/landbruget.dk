import { BaseDataGrid } from "@/services/supabase/types";
import { JsonRender } from "@/components/common/json-render";
import { DynamicDataTable } from "@/components/table/dynamic-table";
import { ColumnDef } from "@tanstack/react-table";

export function BlockTable({ grid }: { grid: BaseDataGrid }) {
  const columns: ColumnDef<Record<string, string | number | boolean>>[] =
    grid.columns.map((col) => ({
      accessorKey: col.key,
      header: col.label,
    }));

  return (
    <div className="">
      <DynamicDataTable columns={columns} data={grid.rows} />
    </div>
  );
}
