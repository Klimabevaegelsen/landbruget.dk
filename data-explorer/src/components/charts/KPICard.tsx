interface KPICardProps {
  label: string;
  value: string;
  format?: "number" | "percent" | "hectares" | "text" | null;
  description?: string | null;
}

export function KPICard({ label, value, format, description }: KPICardProps) {
  const formattedValue = formatValue(value, format);

  return (
    <div className="border-t pt-4 pb-2" style={{ borderColor: "var(--border)" }}>
      <p
        className="text-[10px] font-semibold tracking-[0.2em] uppercase"
        style={{ color: "var(--muted-foreground)" }}
      >
        {label}
      </p>
      <p
        className="mt-2 text-3xl leading-none font-semibold tabular-nums"
        style={{
          fontFamily: "var(--font-geist-mono)",
          color: "var(--foreground)",
        }}
      >
        {formattedValue}
      </p>
      {description && (
        <p className="mt-1.5 text-[11px]" style={{ color: "var(--muted-foreground)" }}>
          {description}
        </p>
      )}
    </div>
  );
}

function formatValue(value: string, format?: string | null): string {
  if (!format || format === "text") return value;

  const num = parseFloat(value.replace(/[^\d.,-]/g, ""));
  if (isNaN(num)) return value;

  switch (format) {
    case "number":
      return new Intl.NumberFormat("da-DK").format(num);
    case "percent":
      return `${new Intl.NumberFormat("da-DK", { maximumFractionDigits: 1 }).format(num)}%`;
    case "hectares":
      return `${new Intl.NumberFormat("da-DK", { maximumFractionDigits: 1 }).format(num)} ha`;
    default:
      return value;
  }
}
