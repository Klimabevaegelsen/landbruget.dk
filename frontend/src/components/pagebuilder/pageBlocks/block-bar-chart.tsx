"use client";

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { BarChart as BarChartType, ChartData } from "@/services/supabase/types";

// Helper function to transform your data into the format Recharts expects
const transformDataForRecharts = (chartData: ChartData) => {
  const { xAxis, series } = chartData;
  if (!xAxis || !xAxis.values || !series) {
    return [];
  }

  return xAxis.values.map((value, index) => {
    const dataPoint: { [key: string]: string | number } = {
      name: String(value), // X-axis value
    };
    series.forEach((s) => {
      dataPoint[s.name] = s.data[index];
    });
    return dataPoint;
  });
};

export function BlockBarChart({ chart }: { chart: BarChartType }) {
  const transformedData = transformDataForRecharts(chart.data);

  if (!transformedData.length) {
    return <div>No data available for chart.</div>;
  }

  // Assuming a simple case with a few predefined colors.
  // You might want to make this more dynamic or configurable.
  const barColors = [
    "#8884d8",
    "#82ca9d",
    "#ffc658",
    "#ff7300",
    "#0088FE",
    "#00C49F",
    "#FFBB28",
    "#FF8042",
  ];

  return (
    <div style={{ width: "100%", height: 400 }}>
      <ResponsiveContainer>
        <RechartsBarChart
          data={transformedData}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="name" />
          <YAxis
            label={{
              value: chart.data.yAxis?.label,
              angle: -90,
              position: "insideLeft",
            }}
          />
          <Tooltip />
          <Legend />
          {chart.data.series.map((s, index) => (
            <Bar
              key={s.name}
              dataKey={s.name}
              fill={barColors[index % barColors.length]}
            />
          ))}
        </RechartsBarChart>
      </ResponsiveContainer>
    </div>
  );
}
