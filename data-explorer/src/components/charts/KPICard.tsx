interface KPICardProps {
  label: string;
  value: string;
  format?: 'number' | 'percent' | 'hectares' | 'text' | null;
  description?: string | null;
}

export function KPICard({ label, value, format, description }: KPICardProps) {
  const formattedValue = formatValue(value, format);

  return (
    <div className="bg-card border-l-primary rounded-r-md border-y border-r border-l-[3px] py-5 pr-6 pl-5">
      <p className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
        {label}
      </p>
      <p className="text-foreground mt-1.5 text-2xl font-semibold tabular-nums">
        {formattedValue}
      </p>
      {description && (
        <p className="text-muted-foreground mt-1 text-xs">{description}</p>
      )}
    </div>
  );
}

function formatValue(value: string, format?: string | null): string {
  if (!format || format === 'text') return value;

  const num = parseFloat(value.replace(/[^\d.,-]/g, ''));
  if (isNaN(num)) return value;

  switch (format) {
    case 'number':
      return new Intl.NumberFormat('da-DK').format(num);
    case 'percent':
      return `${new Intl.NumberFormat('da-DK', { maximumFractionDigits: 1 }).format(num)}%`;
    case 'hectares':
      return `${new Intl.NumberFormat('da-DK', { maximumFractionDigits: 1 }).format(num)} ha`;
    default:
      return value;
  }
}
