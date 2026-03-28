export function formatTooltipValue(value: unknown, unit?: string): string {
  if (value == null || (typeof value === 'number' && isNaN(value))) {
    return '0';
  }
  if (typeof value !== 'number') {
    return String(value ?? '');
  }

  let formatted: string;
  if (unit === 'dyr' || (typeof unit === 'string' && unit.includes('dyr'))) {
    formatted = Math.round(value).toLocaleString('da-DK');
  } else if (unit === 'ha' || unit === 'hektar') {
    formatted = value.toLocaleString('da-DK', {
      minimumFractionDigits: value < 1 ? 2 : 1,
      maximumFractionDigits: value < 1 ? 2 : 1,
    });
  } else if (unit === '%' || (typeof unit === 'string' && unit.includes('%'))) {
    formatted = value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  } else if (value >= 1000) {
    formatted = Math.round(value).toLocaleString('da-DK');
  } else {
    formatted = value.toLocaleString('da-DK', {
      maximumFractionDigits: value < 1 ? 2 : 1,
    });
  }

  return unit ? `${formatted} ${unit}` : formatted;
}
