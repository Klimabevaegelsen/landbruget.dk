import { Table } from "@tanstack/react-table";
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useEffect, useState } from "react";

interface DataTablePaginationProps<TData> {
  table: Table<TData>;
}

export function DataTablePagination<TData>({
  table,
}: DataTablePaginationProps<TData>) {
  const [pageSize, setPageSize] = useState(
    table.getState().pagination.pageSize
  );
  const [hide, setHide] = useState(false);

  useEffect(() => {
    // if pageSize is below 10 and there is less than 10 rows, hide this component
    if (pageSize < 10 && table.getRowCount() < 10) {
      setHide(true);
    }
  }, [pageSize, table]);

  if (hide) {
    return null;
  }

  return (
    <div className="flex items-center justify-between p-2">
      <div className="flex items-center justify-between  w-full space-x-6 lg:space-x-8">
        <div className="flex items-center space-x-2">
          <p className="text-sm font-medium">Rækker pr. side</p>
          <Select
            value={`${table.getState().pagination.pageSize}`}
            onValueChange={(value) => {
              table.setPageSize(Number(value));
              setPageSize(Number(value));
            }}
          >
            <SelectTrigger className="h-8 w-[70px]">
              <SelectValue placeholder={table.getState().pagination.pageSize} />
            </SelectTrigger>
            <SelectContent side="top">
              {[10, 20, 30, 40, 50].map((pageSize) => (
                <SelectItem key={pageSize} value={`${pageSize}`}>
                  {pageSize}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex w-[100px] items-center justify-center text-sm font-medium">
          Side {table.getState().pagination.pageIndex + 1} af{" "}
          {table.getPageCount()}
        </div>
        <div className="flex items-center space-x-2">
          <Button
            variant="outline"
            className="hidden h-8 w-8 p-0 lg:flex"
            onClick={() => table.setPageIndex(0)}
            disabled={!table.getCanPreviousPage()}
          >
            <span className="sr-only">Gå til første side</span>
            <ChevronsLeft />
          </Button>
          <Button
            variant="outline"
            className="h-8 w-8 p-0"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
          >
            <span className="sr-only">Gå til forrige side</span>
            <ChevronLeft />
          </Button>
          <Button
            variant="outline"
            className="h-8 w-8 p-0"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
          >
            <span className="sr-only">Gå til næste side</span>
            <ChevronRight />
          </Button>
          <Button
            variant="outline"
            className="hidden h-8 w-8 p-0 lg:flex"
            onClick={() => table.setPageIndex(table.getPageCount() - 1)}
            disabled={!table.getCanNextPage()}
          >
            <span className="sr-only">Gå til sidste side</span>
            <ChevronsRight />
          </Button>
        </div>
      </div>
    </div>
  );
}
