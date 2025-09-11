'use client';

import NumberFlow from '@number-flow/react';

interface AnimatedNumberProps {
  value: number;
  unit?: string;
  currency?: string;
  className?: string;
  format?: {
    style?: 'decimal' | 'currency' | 'percent';
    minimumFractionDigits?: number;
    maximumFractionDigits?: number;
  };
  locale?: string;
}

export function AnimatedNumber({
  value,
  unit,
  currency,
  className = '',
  format,
  locale = 'da-DK',
}: AnimatedNumberProps) {
  const formatOptions = format || {
    style: currency ? 'currency' : 'decimal',
    currency: currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  };

  return (
    <div className={`font-mono font-medium tabular-nums ${className}`}>
      <NumberFlow
        value={value}
        format={formatOptions}
        locales={locale}
        willChange
      />
      {unit && !currency && (
        <span className="text-muted-foreground ml-2">{unit}</span>
      )}
    </div>
  );
}

// Status indicator component for percentage changes
interface StatusIndicatorProps {
  value?: number;
  showArrow?: boolean;
  className?: string;
}

export function StatusIndicator({
  value,
  showArrow = true,
  className = '',
}: StatusIndicatorProps) {
  if (value === undefined || value === null) return null;

  const isPositive = value > 0;
  const isNegative = value < 0;
  const isNeutral = value === 0;

  const colorClass = isPositive
    ? 'text-green-600 dark:text-green-500'
    : isNegative
      ? 'text-destructive dark:text-destructive'
      : 'text-muted-foreground';

  const bgClass = isPositive
    ? 'bg-organic/10 dark:bg-organic/5'
    : isNegative
      ? 'bg-destructive/10 dark:bg-destructive/5'
      : 'bg-muted';

  return (
    <div
      className={`inline-flex items-center space-x-1 rounded-full px-2 py-1 text-xs ${bgClass} ${colorClass} ${className}`}
    >
      {showArrow && !isNeutral && (
        <span className="text-xs">{isPositive ? '↗' : '↘'}</span>
      )}
      <AnimatedNumber
        value={Math.abs(value)}
        format={{
          style: 'percent',
          minimumFractionDigits: 1,
          maximumFractionDigits: 1,
        }}
        className="text-xs"
      />
    </div>
  );
}
