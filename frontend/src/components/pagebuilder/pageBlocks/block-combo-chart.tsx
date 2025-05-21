"use client";

import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import {
  ComboChart as ComboChartType,
  ChartData,
} from "@/services/supabase/types";
import CustomTooltip from "@/components/chart/custom-tooltip";
import { useEffect, useState } from "react";
import CustomLegend from "@/components/chart/custom-legend";
import { VizColors } from "@/lib/utils";

// We can reuse the existing transformDataForRecharts function since it already handles our data structure
const transformDataForRecharts = (chartData: ChartData) => {
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

export function BlockComboChart({ chart }: { chart: ComboChartType }) {
  const transformedData = transformDataForRecharts(chart.data);
  const [yWidth, setYWidth] = useState(60);

  // Calculate y-axis width based on the longest value
  useEffect(() => {
    const longestTick = transformedData.reduce((max, dataPoint) => {
      const valueLengths = Object.entries(dataPoint)
        .filter(([key]) => key !== "name")
        .map(([, value]) =>
          typeof value === "number"
            ? value.toLocaleString("da-DK").length
            : String(value).length
        );
      return Math.max(max, ...valueLengths);
    }, 0);

    // Add some padding to the width
    setYWidth(longestTick * 8 + 15);
  }, [transformedData]);

  if (!transformedData.length) {
    return <div>No data available for chart.</div>;
  }

  // Separate series by type and yAxis
  const barSeries = chart.data.series.filter((s) => s.type === "bar");
  const lineSeries = chart.data.series.filter((s) => s.type === "line");

  return (
    <div>
      <div style={{ width: "100%", height: 400 }} className="mt-4">
        <ResponsiveContainer>
          <ComposedChart data={transformedData}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="name" tickLine={true} axisLine={true} />

            {/* Left Y-axis for bar series */}
            <YAxis
              yAxisId="left"
              orientation="left"
              axisLine={false}
              tickLine={false}
              tickFormatter={(tick) => tick.toLocaleString("da-DK")}
              width={yWidth}
            />

            {/* Right Y-axis for line series */}
            <YAxis
              yAxisId="right"
              orientation="right"
              axisLine={false}
              tickLine={false}
              tickFormatter={(tick) => tick.toLocaleString("da-DK")}
              width={yWidth}
            />

            <Tooltip content={<CustomTooltip />} cursor={{ fill: "#eef8f2" }} />
            <Legend content={<CustomLegend />} />

            {/* Render bar series */}
            {barSeries.map((s, index) => (
              <Bar
                key={s.name}
                dataKey={s.name}
                fill={VizColors[index % VizColors.length]}
                yAxisId="left"
              />
            ))}

            {/* Render line series */}
            {lineSeries.map((s, index) => (
              <Line
                key={s.name}
                type="monotone"
                dataKey={s.name}
                stroke={
                  VizColors[(barSeries.length + index) % VizColors.length]
                }
                yAxisId="right"
                dot={{
                  fill: VizColors[
                    (barSeries.length + index) % VizColors.length
                  ],
                }}
              />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
