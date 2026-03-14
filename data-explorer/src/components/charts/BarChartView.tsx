'use client';

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

interface BarChartViewProps {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  title?: string | null;
  color?: string | null;
  height?: number | null;
}

export function BarChartView({
  data,
  xKey,
  yKey,
  title,
  color = 'var(--chart-1)',
  height = 350,
}: BarChartViewProps) {
  return (
    <div>
      {title && (
        <h3 className="text-foreground mb-3 text-sm font-semibold">{title}</h3>
      )}
      <ResponsiveContainer width="100%" height={height ?? 350}>
        <RechartsBarChart
          data={data}
          margin={{ top: 5, right: 20, left: 20, bottom: 60 }}
        >
          <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
          <XAxis
            dataKey={xKey}
            tick={{ fontSize: 12 }}
            angle={-45}
            textAnchor="end"
            height={80}
          />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip
            contentStyle={{
              backgroundColor: 'var(--background)',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              fontSize: '12px',
            }}
          />
          <Bar
            dataKey={yKey}
            fill={color ?? 'var(--chart-1)'}
            radius={[4, 4, 0, 0]}
          />
        </RechartsBarChart>
      </ResponsiveContainer>
    </div>
  );
}
