'use client';

import { TooltipProps } from 'recharts';
import type {
  NameType,
  Payload as TooltipPayload,
  ValueType,
} from 'recharts/types/component/DefaultTooltipContent';
import { StatusIndicator } from './animated-number';

interface EnhancedTooltipProps extends TooltipProps<ValueType, NameType> {
  active?: boolean;
  payload?: TooltipPayload<ValueType, NameType>[];
  label?: string | number;
  unit?: string;
  chartType?: string;
  showComparison?: boolean;
}

type TooltipEntry = TooltipPayload<ValueType, NameType>;

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
      {/* Header section with chart type */}
      <div className="border-b px-4 py-2">
        <p className="text-foreground text-sm font-medium">{chartType}</p>
      </div>

      {/* Content section */}
      <div className="p-4">
        {/* Label */}
        <div className="mb-3">
          <p className="text-foreground text-sm font-medium">{label}</p>
        </div>

        {/* Data entries */}
        <div className="space-y-2">
          {payload.map((entry: TooltipEntry, index: number) => {
            const value = entry.value as number;
            const previousValue = entry.payload?.previousValue as
              | number
              | undefined;
            const change = getPercentageChange(value, previousValue);
            const dotStyle = { backgroundColor: entry.color };

            return (
              <div
                key={`item-${index}`}
                className="flex items-center justify-between"
              >
                <div className="flex items-center space-x-2">
                  <div
                    className="h-2 w-2 shrink-0 rounded-full"
                    style={dotStyle}
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
                  </span>
                  {unit && (
                    <span className="text-muted-foreground bg-muted rounded px-1.5 py-0.5 text-xs font-medium">
                      {unit}
                    </span>
                  )}

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
                <div className="flex items-center space-x-2">
                  <span className="text-foreground font-mono text-sm font-medium">
                    {payload
                      .reduce((sum: number, entry: TooltipEntry) => {
                        const value = entry.value as number;
                        return sum + (typeof value === 'number' ? value : 0);
                      }, 0)
                      .toLocaleString('da-DK')}
                  </span>
                  {unit && (
                    <span className="text-muted-foreground bg-muted rounded px-1.5 py-0.5 text-xs font-medium">
                      {unit}
                    </span>
                  )}
                </div>
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
        {payload.map((entry: TooltipEntry, index: number) => {
          const dotStyle = { backgroundColor: entry.color };
          return (
            <div
              key={`item-${index}`}
              className="flex items-center justify-between"
            >
              <div className="flex items-center space-x-1.5">
                <div className="h-1.5 w-1.5 rounded-full" style={dotStyle} />
                <span className="text-muted-foreground text-xs">
                  {entry.name}
                </span>
              </div>
              <div className="flex items-center space-x-1">
                <span className="text-foreground font-mono text-xs font-medium">
                  {typeof entry.value === 'number'
                    ? entry.value.toLocaleString('da-DK')
                    : entry.value}
                </span>
                {unit && (
                  <span className="text-muted-foreground bg-muted rounded px-1 py-0.5 text-[10px] font-medium">
                    {unit}
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
