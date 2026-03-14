'use client';

import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

const COLORS = [
  'var(--chart-1)',
  'var(--chart-4)',
  'var(--chart-2)',
  'var(--chart-5)',
  'var(--chart-3)',
  '#6b9e7a',
];

interface LineChartViewProps {
  data: Record<string, unknown>[];
  xKey: string;
  yKeys: string[];
  title?: string | null;
  height?: number | null;
}

export function LineChartView({
  data,
  xKey,
  yKeys,
  title,
  height = 350,
}: LineChartViewProps) {
  return (
    <div>
      {title && (
        <h3 className="text-foreground mb-3 text-sm font-semibold">{title}</h3>
      )}
      <ResponsiveContainer width="100%" height={height ?? 350}>
        <RechartsLineChart
          data={data}
          margin={{ top: 5, right: 20, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
          <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip
            contentStyle={{
              backgroundColor: 'var(--background)',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              fontSize: '12px',
            }}
          />
          {yKeys.length > 1 && <Legend />}
          {yKeys.map((key, i) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[i % COLORS.length]}
              strokeWidth={2}
              dot={{ r: 3 }}
              activeDot={{ r: 5 }}
            />
          ))}
        </RechartsLineChart>
      </ResponsiveContainer>
    </div>
  );
}
