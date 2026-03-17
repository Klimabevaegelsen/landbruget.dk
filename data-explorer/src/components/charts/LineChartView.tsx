"use client";

import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

const COLORS = [
  "var(--chart-1)",
  "var(--chart-4)",
  "var(--chart-2)",
  "var(--chart-5)",
  "var(--chart-3)",
  "#6b9e7a",
];

interface LineChartViewProps {
  data: Record<string, unknown>[];
  xKey: string;
  yKeys: string[];
  title?: string | null;
  height?: number | null;
}

export function LineChartView({ data, xKey, yKeys, title, height = 350 }: LineChartViewProps) {
  return (
    <div>
      {title && (
        <p
          className="mb-4 text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          {title}
        </p>
      )}
      <ResponsiveContainer width="100%" height={height ?? 350}>
        <RechartsLineChart data={data} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="var(--border)" />
          <XAxis
            dataKey={xKey}
            tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
            axisLine={{ stroke: "var(--border)" }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "var(--card)",
              border: "1px solid var(--border)",
              borderRadius: "0",
              fontSize: "11px",
              fontFamily: "var(--font-geist-mono)",
            }}
          />
          {yKeys.length > 1 && <Legend wrapperStyle={{ fontSize: "11px" }} />}
          {yKeys.map((key, i) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[i % COLORS.length]}
              strokeWidth={1.5}
              dot={{ r: 2.5 }}
              activeDot={{ r: 4 }}
            />
          ))}
        </RechartsLineChart>
      </ResponsiveContainer>
    </div>
  );
}
