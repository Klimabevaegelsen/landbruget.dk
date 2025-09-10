'use client';

import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
} from 'recharts';
import {
  ComboChart as ComboChartType,
  ChartData,
} from '@/services/supabase/types';
import { useEffect, useState } from 'react';
import CustomLegend from '@/components/chart/custom-legend';
import { DocumentationAccordion } from '@/components/chart/documentation-accordion';
import {
  chartColors,
  chartGridStyles,
  generateChartConfig,
} from '@/lib/chart-colors';
import { xAxisDefaultProps } from './block-bar-chart';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';
import {
  ChartContainer,
  ChartTooltip,
} from '@/components/chart/chart-container';
import { AnimatedNumber } from '@/components/chart/animated-number';
import { EnhancedTooltip } from '@/components/chart/enhanced-tooltip';
import { CSVDownloadButton } from '@/components/chart/csv-download-button';

// Helper function to calculate total value for large display number
const calculateTotalValue = (chartData: ChartData) => {
  if (!chartData.series || chartData.series.length === 0) return 0;

  // Sum all values from all series
  return chartData.series.reduce((total, series) => {
    return (
      total +
      series.data.reduce(
        (seriesSum, value) =>
          seriesSum + (typeof value === 'number' ? value : 0),
        0
      )
    );
  }, 0);
};

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
  // Always call hooks first
  const transformedData = transformDataForRecharts(chart.data);
  const [yWidth, setYWidth] = useState(60);
  const { isInCategoryWithData } = useCategoryDataContext();

  // Calculate y-axis width based on the longest value
  useEffect(() => {
    const longestTick = transformedData.reduce((max, dataPoint) => {
      const valueLengths = Object.entries(dataPoint)
        .filter(([key]) => key !== 'name')
        .map(([, value]) =>
          typeof value === 'number'
            ? value.toLocaleString('da-DK').length
            : String(value).length
        );
      return Math.max(max, ...valueLengths);
    }, 0);

    // Add some padding to the width
    setYWidth(longestTick * 8 + 0);
  }, [transformedData]);

  // Check if this chart should show a placeholder
  const placeholderDataType = shouldShowPlaceholder(chart._key);
  if (placeholderDataType) {
    return <PlaceholderChart dataType={placeholderDataType} />;
  }

  // Only show individual no-data placeholder if not in a category with data
  if (!transformedData.length && !isInCategoryWithData) {
    return <NoDataPlaceholder />;
  }

  // If we have no data but are in a category with data, render empty div
  if (!transformedData.length && isInCategoryWithData) {
    return (
      <div className="py-8 text-center text-gray-500">
        Ingen data tilgængelig for dette diagram
      </div>
    );
  }

  // Separate series by type and yAxis
  const barSeries = chart.data.series.filter((s) => s.type === 'bar');
  const lineSeries = chart.data.series.filter((s) => s.type === 'line');

  // Generate chart colors based on series
  const seriesNames = chart.data.series.map((s) => s.name);
  const chartConfig = generateChartConfig(seriesNames);
  const totalValue = calculateTotalValue(chart.data);
  const barColors = chartColors.data;

  // Get the colors for each axis
  const leftAxisColor = barSeries.length > 0 ? barColors[0] : undefined;
  const rightAxisColor =
    lineSeries.length > 0 ? barColors[barSeries.length] : undefined;

  return (
    <div className="space-y-6">
      {/* Header with download button */}
      <div className="flex items-start justify-between">
        <div className="flex-1">
          {/* Large display number - Midday style */}
          {totalValue > 0 && (
            <div className="space-y-2">
              <AnimatedNumber
                value={totalValue}
                unit={chart.unit}
                className="text-4xl"
              />
              <div className="text-muted-foreground flex items-center space-x-2 text-sm">
                <p>Total på tværs af alle kategorier</p>
              </div>
            </div>
          )}
        </div>
        <CSVDownloadButton
          chartData={chart.data}
          chartTitle={chart.title}
          chartKey={chart._key}
          className="ml-4"
        />
      </div>

      {/* Chart container with Midday styling */}
      <ChartContainer config={chartConfig} className="h-[400px] w-full">
        <ComposedChart data={transformedData}>
          <CartesianGrid {...chartGridStyles} />
          <XAxis dataKey="name" {...xAxisDefaultProps} />

          {/* Left Y-axis for bar series */}
          <YAxis
            yAxisId="left"
            orientation="left"
            axisLine={{ stroke: leftAxisColor }}
            tickLine={{ stroke: leftAxisColor }}
            tick={{ fill: leftAxisColor }}
            tickFormatter={(tick) => {
              const formattedTick = tick.toLocaleString('da-DK');
              return chart.unit
                ? `${formattedTick} ${chart.unit}`
                : formattedTick;
            }}
            width={yWidth}
          />

          {/* Right Y-axis for line series */}
          <YAxis
            yAxisId="right"
            orientation="right"
            axisLine={{ stroke: rightAxisColor }}
            tickLine={{ stroke: rightAxisColor }}
            tick={{ fill: rightAxisColor }}
            tickFormatter={(tick) => {
              const formattedTick = tick.toLocaleString('da-DK');
              return chart.unit
                ? `${formattedTick} ${chart.unit}`
                : formattedTick;
            }}
            width={yWidth}
          />

          <ChartTooltip
            content={
              <EnhancedTooltip
                unit={chart.unit}
                chartType="Combo Chart"
                showComparison={false}
              />
            }
            cursor={{ fill: 'hsl(var(--muted))', opacity: 0.1 }}
          />
          <Legend content={<CustomLegend />} />

          {/* Render bar series */}
          {barSeries.map((s, index) => (
            <Bar
              key={s.name}
              dataKey={s.name}
              fill={barColors[index % barColors.length]}
              yAxisId="left"
              radius={[2, 2, 0, 0]}
            />
          ))}

          {/* Render line series */}
          {lineSeries.map((s, index) => (
            <Line
              key={s.name}
              type="monotone"
              dataKey={s.name}
              stroke={barColors[(barSeries.length + index) % barColors.length]}
              yAxisId="right"
              strokeWidth={2}
              dot={{
                fill: barColors[(barSeries.length + index) % barColors.length],
                strokeWidth: 2,
                r: 4,
              }}
              activeDot={{
                r: 6,
                strokeWidth: 2,
              }}
            />
          ))}
        </ComposedChart>
      </ChartContainer>

      {/* Documentation accordion */}
      {chart.documentation && (
        <DocumentationAccordion documentation={chart.documentation} />
      )}
    </div>
  );
}
