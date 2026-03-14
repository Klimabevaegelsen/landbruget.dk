/**
 * Shared number formatting utilities using Danish locale.
 *
 * Three variants exist because call sites have different null-handling needs:
 * - formatNumber: returns null for missing/near-zero (field detail panels)
 * - formatNumberOrZero: always returns a string (visualization labels)
 * - formatNumberFixed: fixed decimal padding, no null handling (KPI displays)
 */

/**
 * Format number with Danish locale. Returns null for missing/near-zero values.
 * Use in field detail panels where null means "hide this row".
 */
export function formatNumber(
  num: number | undefined | null,
  decimals: number = 2
): string | null {
  if (num == null || isNaN(num)) return null;
  if (num < 0.001) return null;

  if (num < 1 && decimals === 0) {
    return num.toLocaleString('da-DK', { maximumFractionDigits: 3 });
  }

  return num.toLocaleString('da-DK', { maximumFractionDigits: decimals });
}

/**
 * Format number with Danish locale. Always returns a string (defaults to '0').
 * Use in visualization labels where a value must always be shown.
 */
export function formatNumberOrZero(
  num: number | undefined | null,
  decimals: number = 2
): string {
  if (num == null || isNaN(num)) return '0';
  return num.toLocaleString('da-DK', { maximumFractionDigits: decimals });
}

/**
 * Format number with Danish locale using fixed decimal places (padded).
 * Use for KPI/stat displays where consistent decimal alignment is needed.
 */
export function formatNumberFixed(value: number, decimals: number = 0): string {
  return value.toLocaleString('da-DK', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}
