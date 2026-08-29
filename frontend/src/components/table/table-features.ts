import {
  columnFilteringFeature,
  columnVisibilityFeature,
  createFilteredRowModel,
  createPaginatedRowModel,
  createSortedRowModel,
  filterFns,
  globalFilteringFeature,
  rowPaginationFeature,
  rowSortingFeature,
  tableFeatures,
} from '@tanstack/react-table';

/**
 * Shared TanStack Table v9 feature set for the app's data tables.
 *
 * v9 is modular: features, row models, and filter-fn registries are opted into
 * explicitly here instead of the v8 `getCoreRowModel()`/`getSortedRowModel()`
 * table options. Registering `filterFns` keeps the v8 string identifiers such
 * as `'includesString'` working for the global filter.
 */
export const dataTableFeatures = tableFeatures({
  rowSortingFeature,
  rowPaginationFeature,
  columnVisibilityFeature,
  columnFilteringFeature,
  globalFilteringFeature,
  sortedRowModel: createSortedRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  filteredRowModel: createFilteredRowModel(),
  filterFns,
});

export type DataTableFeatures = typeof dataTableFeatures;
