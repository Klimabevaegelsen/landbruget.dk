'use client';

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
} from 'recharts';
import {
  BarChart as BarChartType,
  ChartData,
  HorizontalStackedBarChart,
  StackedBarChart,
} from '@/services/supabase/types';
import CustomTooltip from '@/components/chart/custom-tooltip';
import { useEffect, useState } from 'react';
import CustomLegend from '@/components/chart/custom-legend';
import { VizColors } from '@/lib/utils';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';
import { translateDestinationType } from '@/lib/translations/animal-transportation';

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

  // Assuming a simple case with a few predefined colors.
  // You might want to make this more dynamic or configurable.
  const barColors = VizColors;

  return (
    <div>
      <div
        style={{ width: '100%', height: 400, minHeight: 400, minWidth: 100 }}
        className="mt-4"
      >
        <ResponsiveContainer>
          <RechartsBarChart
            data={transformedData}
            layout={isHorizontal ? 'vertical' : 'horizontal'}
            {...{ overflow: 'visible' }}
          >
            <CartesianGrid vertical={false} />
            {isHorizontal ? (
              <XAxis
                type="number"
                tickFormatter={(tick) => tick.toLocaleString('da-DK')}
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
                  return tick.toLocaleString('DA-dk');
                }}
                {...yAxisDefaultProps}
                width={yWidth}
              />
            )}
            <Tooltip content={<CustomTooltip />} cursor={{ fill: '#eef8f2' }} />
            <Legend content={<CustomLegend />} />
            {chart.data.series.map((s, index) => (
              <Bar
                key={s.name}
                dataKey={s.name}
                fill={barColors[index % barColors.length]}
                stackId={
                  chart._type === 'stackedBarChart' ? 'stack' : undefined
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
