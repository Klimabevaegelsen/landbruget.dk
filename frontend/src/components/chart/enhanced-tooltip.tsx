'use client';

import { TooltipProps } from 'recharts';
import {
  NameType,
  ValueType,
} from 'recharts/types/component/DefaultTooltipContent';
import { StatusIndicator } from './animated-number';

interface EnhancedTooltipProps extends TooltipProps<ValueType, NameType> {
  unit?: string;
  chartType?: string;
  showComparison?: boolean;
}

export function EnhancedTooltip({
  active,
  payload,
  label,
  unit,
  chartType = 'Chart',
  showComparison = false,
}: EnhancedTooltipProps) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  // Calculate percentage change if we have comparison data
  const getPercentageChange = (current: number, previous?: number) => {
    if (!previous || previous === 0) return null;
    return ((current - previous) / previous) * 100;
  };

  return (
    <div className="bg-background w-[240px] overflow-hidden rounded-lg border shadow-sm">
      {/* Header section with chart type and status */}
      <div className="flex items-center justify-between border-b px-4 py-2">
        <p className="text-foreground text-sm font-medium">{chartType}</p>
        <div className="flex items-center space-x-2">
          <div className="h-2 w-2 rounded-full bg-green-500" />
          <span className="text-muted-foreground text-xs">Live</span>
        </div>
      </div>

      {/* Content section */}
      <div className="p-4">
        {/* Label with timestamp */}
        <div className="mb-3 flex items-center justify-between">
          <p className="text-foreground text-sm font-medium">{label}</p>
          <span className="text-muted-foreground text-xs">
            {new Date().toLocaleDateString('da-DK')}
          </span>
        </div>

        {/* Data entries */}
        <div className="space-y-2">
          {payload.map((entry, index) => {
            const value = entry.value as number;
            const previousValue = entry.payload?.previousValue as
              | number
              | undefined;
            const change = getPercentageChange(value, previousValue);

            return (
              <div
                key={`item-${index}`}
                className="flex items-center justify-between"
              >
                <div className="flex items-center space-x-2">
                  <div
                    className="h-2 w-2 shrink-0 rounded-full"
                    style={{ backgroundColor: entry.color }}
                  />
                  <span className="text-muted-foreground truncate text-xs">
                    {entry.name}
                  </span>
                </div>

                <div className="flex items-center space-x-2">
                  <span className="text-foreground font-mono text-sm font-medium">
                    {typeof value === 'number'
                      ? value.toLocaleString('da-DK')
                      : value}
                    {unit && ` ${unit}`}
                  </span>

                  {showComparison && change !== null && (
                    <StatusIndicator value={change / 100} showArrow={true} />
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* Summary row for multiple series */}
        {payload.length > 1 && (
          <>
            <div className="mt-3 border-t pt-3">
              <div className="flex items-center justify-between">
                <span className="text-foreground text-sm font-medium">
                  Total
                </span>
                <span className="text-foreground font-mono text-sm font-medium">
                  {payload
                    .reduce((sum, entry) => {
                      const value = entry.value as number;
                      return sum + (typeof value === 'number' ? value : 0);
                    }, 0)
                    .toLocaleString('da-DK')}
                  {unit && ` ${unit}`}
                </span>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// Compact version for smaller charts
export function CompactTooltip({
  active,
  payload,
  label,
  unit,
}: EnhancedTooltipProps) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="bg-background min-w-[160px] rounded-lg border px-3 py-2 shadow-sm">
      <div className="text-foreground mb-1 text-xs font-medium">{label}</div>
      <div className="space-y-1">
        {payload.map((entry, index) => (
          <div
            key={`item-${index}`}
            className="flex items-center justify-between"
          >
            <div className="flex items-center space-x-1.5">
              <div
                className="h-1.5 w-1.5 rounded-full"
                style={{ backgroundColor: entry.color }}
              />
              <span className="text-muted-foreground text-xs">
                {entry.name}
              </span>
            </div>
            <span className="text-foreground font-mono text-xs font-medium">
              {typeof entry.value === 'number'
                ? entry.value.toLocaleString('da-DK')
                : entry.value}
              {unit && ` ${unit}`}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
