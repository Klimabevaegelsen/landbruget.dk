'use client';

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
  XAxisProps,
  YAxisProps,
} from 'recharts';
import {
  BarChart as BarChartType,
  ChartData,
  HorizontalStackedBarChart,
  StackedBarChart,
} from '@/services/supabase/types';
import { useEffect, useState } from 'react';
import CustomLegend from '@/components/chart/custom-legend';
import { DocumentationAccordion } from '@/components/chart/documentation-accordion';
import {
  chartColors,
  chartAxisStyles,
  chartGridStyles,
  generateChartConfig,
} from '@/lib/chart-colors';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';
import { translateDestinationType } from '@/lib/translations/animal-transportation';
import {
  ChartContainer,
  ChartTooltip,
} from '@/components/chart/chart-container';
import { AnimatedNumber } from '@/components/chart/animated-number';
import { EnhancedTooltip } from '@/components/chart/enhanced-tooltip';
import { CSVDownloadButton } from '@/components/chart/csv-download-button';

export const xAxisDefaultProps: XAxisProps = {
  ...chartAxisStyles.x,
  height: 38,
};

export const yAxisDefaultProps: YAxisProps = {
  ...chartAxisStyles.y,
};

// Helper function to check if a value looks like a destination type
const isDestinationType = (value: string): boolean => {
  const destinationTypes = [
    'Slaughterhouse',
    'Rendering Plant',
    'Collection Center',
    'Collection Point',
    'Cooling Facility',
    'Production Farm',
    'Breeding Farm',
    'Piglet Farm',
    'Free-range Pig Farm',
    'Organic Pig Farm',
    'Dairy Farm',
    'Beef Farm',
    'Heifer Hotel',
    'Veal Farm',
    'Quarantine Facility',
    'Research Facility',
    'AI Station',
    'Market/Trading',
    'Hobby Farm',
    'Boarding/Riding School',
    'Stud Farm',
    'Racing/Training',
    'Other Livestock',
    'Livestock Farm',
    'Seasonal Grazing',
    'Nature Management',
    'Livestock Show',
    'Zoo',
    'International Export',
    'Other Commercial',
    'Unknown',
  ];
  return destinationTypes.includes(value);
};

// Helper function to translate category values if they are destination types
const translateCategoryValue = (value: string | number): string => {
  const strValue = String(value);
  return isDestinationType(strValue)
    ? translateDestinationType(strValue)
    : strValue;
};

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

// Helper function to transform your data into the format Recharts expects
const transformDataForRecharts = (chartData: ChartData, chartType: string) => {
  // For horizontal charts, we use yAxis.values as our categories
  if (chartType === 'horizontalStackedBarChart') {
    const { yAxis, series } = chartData;
    if (!yAxis?.values || !series) return [];

    return yAxis.values.map((category, index) => {
      const dataPoint: { [key: string]: string | number } = {
        category: translateCategoryValue(category), // Translate destination types
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
      name: translateCategoryValue(value), // Translate destination types
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
  // Always call hooks first
  const transformedData = transformDataForRecharts(chart.data, chart._type);
  const [yWidth, setYWidth] = useState(60);
  const isHorizontal = chart._type === 'horizontalStackedBarChart';
  const { isInCategoryWithData } = useCategoryDataContext();

  // Calculate y-axis width based on the longest value
  useEffect(() => {
    const longestTick = transformedData.reduce((max, dataPoint) => {
      if (isHorizontal) {
        // For horizontal charts, we need to consider the category name length
        const categoryLength = String(dataPoint.category).length;
        const valueLengths = Object.entries(dataPoint)
          .filter(([key]) => key !== 'category')
          .map(([, value]) =>
            typeof value === 'number'
              ? value.toLocaleString('da-DK').length
              : String(value).length
          );
        return Math.max(max, categoryLength, ...valueLengths);
      } else {
        // For vertical charts, we only need to consider the value lengths
        const valueLengths = Object.entries(dataPoint)
          .filter(([key]) => key !== 'name')
          .map(([, value]) =>
            typeof value === 'number'
              ? value.toLocaleString('da-DK').length
              : String(value).length
          );
        return Math.max(max, ...valueLengths);
      }
    }, 0);

    // Add some padding to the width
    setYWidth(longestTick * 8 + 20);
  }, [transformedData, isHorizontal]);

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

  // Generate chart colors based on series
  const seriesNames = chart.data.series.map((s) => s.name);
  const chartConfig = generateChartConfig(seriesNames);
  const barColors = chartColors.data;
  const totalValue = calculateTotalValue(chart.data);

  return (
    <div className="space-y-6">
      {/* Header with download button */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
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
          className="self-start sm:ml-4"
        />
      </div>

      {/* Chart container with Midday styling */}
      <ChartContainer config={chartConfig} className="h-[400px] w-full">
        <RechartsBarChart
          data={transformedData}
          layout={isHorizontal ? 'vertical' : 'horizontal'}
          margin={{
            top: 20,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid {...chartGridStyles} />
          {isHorizontal ? (
            <XAxis
              type="number"
              tickFormatter={(tick) => {
                const formattedTick = tick.toLocaleString('da-DK');
                return chart.unit
                  ? `${formattedTick} ${chart.unit}`
                  : formattedTick;
              }}
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
                const formattedTick = tick.toLocaleString('da-DK');
                return chart.unit
                  ? `${formattedTick} ${chart.unit}`
                  : formattedTick;
              }}
              {...yAxisDefaultProps}
              width={yWidth}
            />
          )}
          <ChartTooltip
            content={
              <EnhancedTooltip
                unit={chart.unit}
                chartType="Bar Chart"
                showComparison={false}
              />
            }
            cursor={{ fill: 'hsl(var(--muted))', opacity: 0.1 }}
          />
          <Legend content={<CustomLegend />} />
          {chart.data.series.map((s, index) => (
            <Bar
              key={s.name}
              dataKey={s.name}
              fill={barColors[index % barColors.length]}
              stackId={chart._type === 'stackedBarChart' ? 'stack' : undefined}
              radius={[2, 2, 0, 0]}
            />
          ))}
        </RechartsBarChart>
      </ChartContainer>

      {/* <JsonRender
        json={JSON.parse(JSON.stringify(transformedData))}
        title={`Component ${chart._type} placeholder (data)`}
      /> */}

      {/* Documentation accordion */}
      {chart.documentation && (
        <DocumentationAccordion documentation={chart.documentation} />
      )}
    </div>
  );
}
