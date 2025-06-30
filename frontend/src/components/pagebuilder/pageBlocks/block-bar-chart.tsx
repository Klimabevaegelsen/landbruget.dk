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
  XAxisProps,
  YAxisProps,
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
import { VizColors } from "@/lib/utils";

export const xAxisDefaultProps: XAxisProps = {
  tickLine: true,
  axisLine: true,
  tickMargin: 8,
  height: 38,
};

export const yAxisDefaultProps: YAxisProps = {
  tickLine: false,
  axisLine: false,
  tickMargin: 6,
};

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
      if (isHorizontal) {
        // For horizontal charts, we need to consider the category name length
        const categoryLength = String(dataPoint.category).length;
        const valueLengths = Object.entries(dataPoint)
          .filter(([key]) => key !== "category")
          .map(([, value]) =>
            typeof value === "number"
              ? value.toLocaleString("da-DK").length
              : String(value).length
          );
        return Math.max(max, categoryLength, ...valueLengths);
      } else {
        // For vertical charts, we only need to consider the value lengths
        const valueLengths = Object.entries(dataPoint)
          .filter(([key]) => key !== "name")
          .map(([, value]) =>
            typeof value === "number"
              ? value.toLocaleString("da-DK").length
              : String(value).length
          );
        return Math.max(max, ...valueLengths);
      }
    }, 0);

    // Add some padding to the width
    setYWidth(longestTick * 8 + 20);
  }, [transformedData, isHorizontal]);

  if (!transformedData.length) {
    return <div>No data available for chart.</div>;
  }

  // Assuming a simple case with a few predefined colors.
  // You might want to make this more dynamic or configurable.
  const barColors = VizColors;

  return (
    <div>
      <div
        style={{ width: "100%", height: 400, minHeight: 400, minWidth: 100 }}
        className="mt-4"
      >
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
                {...xAxisDefaultProps}
              />
            ) : (
              <XAxis dataKey="name" {...xAxisDefaultProps} />
            )}
            {isHorizontal ? (
              <YAxis
                dataKey="category"
                type="category"
                {...yAxisDefaultProps}
                width={yWidth}
              />
            ) : (
              <YAxis
                tickFormatter={(tick) => {
                  return tick.toLocaleString("DA-dk");
                }}
                {...yAxisDefaultProps}
                width={yWidth}
              />
            )}
            <Tooltip content={<CustomTooltip />} cursor={{ fill: "#eef8f2" }} />
            <Legend content={<CustomLegend />} />
            {chart.data.series.map((s, index) => (
              <Bar
                key={s.name}
                dataKey={s.name}
                fill={barColors[index % barColors.length]}
                stackId={
                  chart._type === "stackedBarChart" ? "stack" : undefined
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
