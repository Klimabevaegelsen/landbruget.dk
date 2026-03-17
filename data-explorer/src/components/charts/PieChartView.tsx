"use client";

import {
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

// Brand-aligned palette — forest greens, earthy tones
const COLORS = [
  "var(--chart-1)",
  "var(--chart-4)",
  "var(--chart-2)",
  "var(--chart-5)",
  "var(--chart-3)",
  "#6b9e7a",
  "#a07c4e",
  "#8fb89e",
  "#c4a06a",
  "#4d7a61",
];

interface PieChartViewProps {
  data: Record<string, unknown>[];
  nameKey: string;
  valueKey: string;
  title?: string | null;
  height?: number | null;
}

export function PieChartView({ data, nameKey, valueKey, title, height = 350 }: PieChartViewProps) {
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
        <RechartsPieChart>
          <Pie
            data={data}
            dataKey={valueKey}
            nameKey={nameKey}
            cx="50%"
            cy="50%"
            outerRadius={120}
            innerRadius={60}
            paddingAngle={1}
            label={({ name, percent }: { name?: string; percent?: number }) =>
              `${name ?? ""} (${((percent ?? 0) * 100).toFixed(0)}%)`
            }
            labelLine={{ strokeWidth: 0.5, stroke: "var(--muted-foreground)" }}
          >
            {data.map((_, index) => (
              <Cell key={index} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              backgroundColor: "var(--card)",
              border: "1px solid var(--border)",
              borderRadius: "0",
              fontSize: "11px",
              fontFamily: "var(--font-geist-mono)",
            }}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
        </RechartsPieChart>
      </ResponsiveContainer>
    </div>
  );
}
