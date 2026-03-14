'use client';

import {
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

// Brand-aligned palette — forest greens, earthy tones
const COLORS = [
  'var(--chart-1)',
  'var(--chart-4)',
  'var(--chart-2)',
  'var(--chart-5)',
  'var(--chart-3)',
  '#6b9e7a',
  '#a07c4e',
  '#8fb89e',
  '#c4a06a',
  '#4d7a61',
];

interface PieChartViewProps {
  data: Record<string, unknown>[];
  nameKey: string;
  valueKey: string;
  title?: string | null;
  height?: number | null;
}

export function PieChartView({
  data,
  nameKey,
  valueKey,
  title,
  height = 350,
}: PieChartViewProps) {
  return (
    <div>
      {title && (
        <h3 className="text-foreground mb-3 text-sm font-semibold">{title}</h3>
      )}
      <ResponsiveContainer width="100%" height={height ?? 350}>
        <RechartsPieChart>
          <Pie
            data={data}
            dataKey={valueKey}
            nameKey={nameKey}
            cx="50%"
            cy="50%"
            outerRadius={120}
            innerRadius={60}
            paddingAngle={2}
            label={({ name, percent }: { name?: string; percent?: number }) =>
              `${name ?? ''} (${((percent ?? 0) * 100).toFixed(0)}%)`
            }
            labelLine={{ strokeWidth: 1 }}
          >
            {data.map((_, index) => (
              <Cell key={index} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              backgroundColor: 'var(--background)',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              fontSize: '12px',
            }}
          />
          <Legend />
        </RechartsPieChart>
      </ResponsiveContainer>
    </div>
  );
}
