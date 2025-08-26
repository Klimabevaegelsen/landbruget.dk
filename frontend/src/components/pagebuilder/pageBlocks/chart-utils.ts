/**
 * Utility functions for chart components
 */

export type MissingDataType = "nitrate" | "carbon" | "subsidies";

/**
 * Determines if a chart should show a placeholder based on its _key
 * Returns the data type if it should be a placeholder, null otherwise
 */
export function shouldShowPlaceholder(chartKey: string): MissingDataType | null {
  // Specific chart keys that need placeholders for missing data

  // Subsidies charts
  if (chartKey === "subsidies-history-stacked") {
    return "subsidies";
  }

  // Nitrogen/Nitrate charts
  if (chartKey === "environment-nitrogen-leaching" ||
      chartKey === "environment-nitrogen-per-field") {
    return "nitrate";
  }

  // Carbon accounting charts
  if (chartKey === "carbon-accounting-kpis" ||
      chartKey === "carbon-accounting-history" ||
      chartKey === "carbon-accounting-details") {
    return "carbon";
  }

  return null;
}
