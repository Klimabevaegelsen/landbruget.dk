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
        <p
          className="mb-4 text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: 'var(--muted-foreground)' }}
        >
          {title}
        </p>
      )}
      <ResponsiveContainer width="100%" height={height ?? 350}>
        <RechartsBarChart
          data={data}
          margin={{ top: 5, right: 20, left: 20, bottom: 60 }}
        >
          <CartesianGrid strokeDasharray="2 4" stroke="var(--border)" />
          <XAxis
            dataKey={xKey}
            tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
            angle={-45}
            textAnchor="end"
            height={80}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: 'var(--card)',
              border: '1px solid var(--border)',
              borderRadius: '0',
              fontSize: '11px',
              fontFamily: 'var(--font-geist-mono)',
            }}
            cursor={{
              fill: 'color-mix(in oklch, var(--primary) 8%, transparent)',
            }}
          />
          <Bar dataKey={yKey} fill={color ?? 'var(--chart-1)'} radius={0} />
        </RechartsBarChart>
      </ResponsiveContainer>
    </div>
  );
}
