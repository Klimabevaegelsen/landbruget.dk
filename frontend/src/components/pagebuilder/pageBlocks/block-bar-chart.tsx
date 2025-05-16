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
import {
  BarChart as BarChartType,
  ChartData,
  HorizontalStackedBarChart,
  StackedBarChart,
} from "@/services/supabase/types";
import CustomTooltip from "@/components/chart/custom-tooltip";
import { useEffect, useState } from "react";
import CustomLegend from "@/components/chart/custom-legend";

// Helper function to transform your data into the format Recharts expects
const transformDataForRecharts = (chartData: ChartData, chartType: string) => {
  // For horizontal charts, we use yAxis.values as our categories
  if (chartType === "horizontalStackedBarChart") {
    const { yAxis, series } = chartData;
    if (!yAxis?.values || !series) return [];

    return yAxis.values.map((category, index) => {
      const dataPoint: { [key: string]: string | number } = {
        category: String(category), // Using 'category' instead of 'name' for clarity
      };
      series.forEach((s) => {
        dataPoint[s.name] = s.data[index];
      });
      return dataPoint;
    });
  }

  // Original logic for vertical charts
  const { xAxis, series } = chartData;
  if (!xAxis?.values || !series) return [];

  return xAxis.values.map((value, index) => {
    const dataPoint: { [key: string]: string | number } = {
      name: String(value),
    };
    series.forEach((s) => {
      dataPoint[s.name] = s.data[index];
    });
    return dataPoint;
  });
};

export function BlockBarChart({
  chart,
}: {
  chart: BarChartType | StackedBarChart | HorizontalStackedBarChart;
}) {
  const transformedData = transformDataForRecharts(chart.data, chart._type);
  const [yWidth, setYWidth] = useState(60);
  const isHorizontal = chart._type === "horizontalStackedBarChart";

  // Calculate y-axis width based on the longest value
  useEffect(() => {
    const longestTick = transformedData.reduce((max, dataPoint) => {
      const values = Object.entries(dataPoint)
        .filter(([key]) => key !== (isHorizontal ? "category" : "name"))
        .map(([, value]) =>
          typeof value === "number"
            ? value.toLocaleString("da-DK")
            : String(value)
        );
      const currentMax = Math.max(...values.map((str) => str.length));
      return Math.max(max, currentMax);
    }, 0);
    setYWidth(longestTick * 8 + 15);
  }, [transformedData, isHorizontal]);

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
    <div>
      <div style={{ width: "100%", height: 400 }} className="mt-4">
        <ResponsiveContainer>
          <RechartsBarChart
            data={transformedData}
            layout={isHorizontal ? "vertical" : "horizontal"}
            {...{ overflow: "visible" }}
          >
            <CartesianGrid vertical={false} />
            {isHorizontal ? (
              <XAxis
                type="number"
                tickFormatter={(tick) => tick.toLocaleString("da-DK")}
                tickLine={true}
                axisLine={true}
              />
            ) : (
              <XAxis dataKey="name" tickLine={true} axisLine={true} />
            )}
            {isHorizontal ? (
              <YAxis
                dataKey="category"
                type="category"
                axisLine={false}
                tickLine={false}
                width={yWidth}
              />
            ) : (
              <YAxis
                axisLine={false}
                tickLine={false}
                tickFormatter={(tick) => {
                  return tick.toLocaleString("DA-dk");
                }}
                width={yWidth}
              />
            )}
            <Tooltip content={<CustomTooltip />} cursor={{ fill: "#E0F5E8" }} />
            <Legend content={<CustomLegend />} />
            {chart.data.series.map((s, index) => (
              <Bar
                key={s.name}
                dataKey={s.name}
                fill={barColors[index % barColors.length]}
                stackId={
                  chart._type === "stackedBarChart" || isHorizontal
                    ? "stack"
                    : undefined
                }
              />
            ))}
          </RechartsBarChart>
        </ResponsiveContainer>
      </div>
      {/* <JsonRender
        json={JSON.parse(JSON.stringify(transformedData))}
        title={`Component ${chart._type} placeholder (data)`}
      /> */}
    </div>
  );
}
