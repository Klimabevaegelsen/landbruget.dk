/**
 * Utility functions for chart components
 */

import { PageBuilderItem, IteratedSection } from '@/services/supabase/types';

export type MissingDataType =
  | 'nitrate'
  | 'carbon'
  | 'subsidies'
  | 'worker_welfare';

/**
 * Determines if a chart should show a placeholder based on its _key
 * Returns the data type if it should be a placeholder, null otherwise
 */
export function shouldShowPlaceholder(
  chartKey: string
): MissingDataType | null {
  // Specific chart keys that need placeholders for missing data

  // Subsidies charts
  if (chartKey === 'subsidies-history-stacked') {
    return 'subsidies';
  }

  // Nitrogen/Nitrate charts
  if (
    chartKey === 'environment-nitrogen-leaching' ||
    chartKey === 'environment-nitrogen-per-field'
  ) {
    return 'nitrate';
  }

  // Carbon accounting charts
  if (
    chartKey === 'carbon-accounting-kpis' ||
    chartKey === 'carbon-accounting-history' ||
    chartKey === 'carbon-accounting-details'
  ) {
    return 'carbon';
  }

  // Worker welfare charts
  if (chartKey === 'worker-welfare-injuries') {
    return 'worker_welfare';
  }

  return null;
}

/**
 * Checks if a PageBuilderItem has actual data
 */
function hasData(item: PageBuilderItem): boolean {
  switch (item._type) {
    case 'barChart':
    case 'stackedBarChart':
    case 'horizontalStackedBarChart':
      return (
        item.data?.series?.length > 0 &&
        item.data?.xAxis?.values?.length > 0 &&
        item.data.series.some((series) => series.data?.length > 0)
      );

    case 'comboChart':
      return (
        item.data?.series?.length > 0 &&
        item.data?.xAxis?.values?.length > 0 &&
        item.data.series.some((series) => series.data?.length > 0)
      );

    case 'dataGrid':
      if (!item.rows || item.rows.length === 0) {
        return false;
      }
      // Check if all rows have only N/A values (excluding keys like 'year', 'id', etc.)
      return item.rows.some((row) => {
        return Object.entries(row).some(([key, value]) => {
          // Skip non-data columns like year, id, etc.
          if (
            key === 'year' ||
            key === 'id' ||
            key.toLowerCase().includes('year')
          ) {
            return false;
          }
          // Consider row has data if any value is not N/A, null, undefined, or empty string
          return (
            value !== 'N/A' &&
            value !== null &&
            value !== undefined &&
            value !== ''
          );
        });
      });

    case 'kpiGroup':
      return item.kpis?.length > 0;

    case 'mapChart':
      return (
        item.data?.layers?.some(
          (layer) =>
            layer.data &&
            typeof layer.data === 'object' &&
            'features' in layer.data &&
            Array.isArray(layer.data.features) &&
            layer.data.features.length > 0
        ) ?? false
      );

    case 'timeline':
      return item.events?.length > 0;

    case 'iteratedSection':
      return hasCategoryData(item);

    case 'infoCard':
      // Info cards are always considered to have data as they're informational
      return true;
    default:
      return true;
  }
}

/**
 * Checks if an entire category (IteratedSection) has any data across all its sections
 */
export function hasCategoryData(category: IteratedSection): boolean {
  if (!category.sections || category.sections.length === 0) {
    return false;
  }

  // Check if any section has any content with real data OR predefined placeholders
  return category.sections.some((section) =>
    section.content?.some((item) => {
      // Consider both real data and predefined placeholders as "having content"
      if (shouldShowPlaceholder(item._key)) {
        return true;
      }
      return hasData(item);
    })
  );
}

/**
 * Checks if a category has only real data (excluding predefined placeholders)
 * This is used to determine if individual empty charts should be hidden
 */
export function hasRealDataOnly(category: IteratedSection): boolean {
  if (!category.sections || category.sections.length === 0) {
    return false;
  }

  // Check if any section has real data (not just predefined placeholders)
  return category.sections.some((section) =>
    section.content?.some((item) => {
      // Skip predefined placeholders - we only care about real data
      if (shouldShowPlaceholder(item._key)) {
        return false;
      }
      return hasData(item);
    })
  );
}

/**
 * Checks if a specific section within a category has data
 */
export function hasSectionData(sectionContent: PageBuilderItem[]): boolean {
  if (!sectionContent || sectionContent.length === 0) {
    return false;
  }

  return sectionContent.some((item) => {
    // Skip items that should show predefined placeholders - they're not "no data"
    if (shouldShowPlaceholder(item._key)) {
      return true;
    }
    return hasData(item);
  });
}

/**
 * Determines if an individual chart should show a no-data placeholder
 * when it's within a category context
 */
export function shouldShowNoDataPlaceholder(
  item: PageBuilderItem,
  isInCategoryWithData: boolean = false
): boolean {
  // If we're in a category that has data somewhere, don't show individual no-data placeholders
  if (isInCategoryWithData) {
    return false;
  }

  // Otherwise, check if this individual item has data
  return !hasData(item);
}
