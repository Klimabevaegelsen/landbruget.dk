"use client";

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import { BarChart as BarChartType, ChartData } from "@/services/supabase/types";
import CustomTooltip from "@/components/chart/custom-tooltip";
import { useEffect, useState } from "react";
import CustomLegend from "@/components/chart/custom-legend";

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
  const [yWidth, setYWidth] = useState(60);

  // we have to calculate the width of the y-axis based on the longest tick. https://github.com/recharts/recharts/issues/1127
  useEffect(() => {
    const longestTick = transformedData.reduce((max, dataPoint) => {
      // Get all values from the dataPoint object except 'name'
      const allValues = Object.entries(dataPoint)
        .filter(([key]) => key !== "name")
        .map(([, value]) =>
          typeof value === "number"
            ? value.toLocaleString("da-DK")
            : String(value)
        );

      // Find the longest string among all values
      const currentMax = Math.max(...allValues.map((str) => str.length));
      return Math.max(max, currentMax);
    }, 0);

    // Add some padding to the width
    setYWidth(longestTick * 8 + 15);
  }, [transformedData]);

  if (!transformedData.length) {
    return <div>No data available for chart.</div>;
  }

  // Assuming a simple case with a few predefined colors.
  // You might want to make this more dynamic or configurable.
  const barColors = [
    "#4F5D75",
    "#C67750",
    "#467968",
    "#775120",
    "#7F2E39",
    "#2D673D",
    "#503955",
    "#5F318B",
  ];

  return (
    <div style={{ width: "100%", height: 400 }} className="mt-4">
      <ResponsiveContainer>
        <RechartsBarChart
          data={transformedData}
          {...{
            overflow: "visible",
          }}
        >
          <CartesianGrid vertical={false} />
          <XAxis dataKey="name" tickLine={true} axisLine={true} />
          <YAxis
            axisLine={false}
            tickLine={false}
            tickFormatter={(tick) => {
              return tick.toLocaleString("DA-dk");
            }}
            width={yWidth}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: "#E0F5E8" }} />
          <Legend content={<CustomLegend />} />
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
