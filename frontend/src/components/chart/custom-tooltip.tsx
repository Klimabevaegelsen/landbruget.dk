import { TooltipProps } from 'recharts';
import type {
  NameType,
  Payload as TooltipPayload,
  ValueType,
} from 'recharts/types/component/DefaultTooltipContent';

interface CustomTooltipProps extends TooltipProps<ValueType, NameType> {
  active?: boolean;
  payload?: TooltipPayload<ValueType, NameType>[];
  label?: string | number;
  unit?: string;
}

type TooltipEntry = TooltipPayload<ValueType, NameType>;

export function CustomTooltip({
  active,
  payload,
  label,
  unit,
}: CustomTooltipProps) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="bg-background border-border rounded-lg border p-4 shadow-md">
      <p className="text-base font-semibold">{label}</p>
      {payload.map((entry: TooltipEntry, index: number) => {
        const colorStyle = { color: entry.color };
        return (
          <p
            key={`item-${index}`}
            style={colorStyle}
            className="mt-1 text-sm font-medium"
          >
            <span>
              {entry.name}: {entry.value?.toLocaleString('da-DK')}
              {unit && (
                <span className="text-muted-foreground bg-muted ml-2 rounded px-1 py-0.5 text-xs font-medium">
                  {unit}
                </span>
              )}
            </span>
          </p>
        );
      })}
    </div>
  );
}
