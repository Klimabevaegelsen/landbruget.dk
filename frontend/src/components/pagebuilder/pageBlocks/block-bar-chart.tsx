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
import { CustomLegend, DocumentationAccordion } from '@/components/chart';
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
  AnimatedNumber,
  EnhancedTooltip,
  CSVDownloadButton,
} from '@/components/chart';

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

// Helper function to detect if data is temporal (per time period)
const isTemporalData = (
  xAxisLabel?: string,
  xAxisValues?: (string | number)[]
) => {
  const axisText = xAxisLabel?.toLowerCase() || '';

  // Check axis label for temporal indicators
  const temporalIndicators = [
    'år',
    'year',
    'måned',
    'month',
    'dag',
    'day',
    'kvartal',
    'quarter',
    'uge',
    'week',
  ];
  if (temporalIndicators.some((indicator) => axisText.includes(indicator))) {
    return true;
  }

  // Check if x-axis values look like years/dates
  if (xAxisValues && xAxisValues.length > 0) {
    const firstValue = String(xAxisValues[0]);
    // Check for year patterns (2020, 2021, etc.) or date patterns
    if (/^\d{4}$/.test(firstValue) || /^\d{4}-\d{2}/.test(firstValue)) {
      return true;
    }
  }

  return false;
};

// Helper function to detect if metric should be summed or averaged
const shouldUseAverage = (unit?: string, xAxisLabel?: string) => {
  const unitLower = unit?.toLowerCase() || '';
  const contextText = `${unitLower} ${xAxisLabel?.toLowerCase() || ''}`;

  // If it's "per" something (per year, per month, etc.), always average
  const isPerSomething = ['per ', 'pr. ', 'pr ', '/ '].some((indicator) =>
    contextText.includes(indicator)
  );

  if (isPerSomething) {
    return true; // Always average rates/ratios
  }

  // Only sum for very specific cases that are explicitly cumulative AND not "per" something
  const explicitlyCumulativeIndicators = [
    'total',
    'sum',
    'accumulated',
    'akkumuleret',
  ];

  const shouldSum = explicitlyCumulativeIndicators.some(
    (indicator) =>
      unitLower.includes(indicator) ||
      (xAxisLabel?.toLowerCase() || '').includes(indicator)
  );

  // Default to average for everything else - much safer!
  return !shouldSum;
};

// Helper function to calculate appropriate display value for bar charts
const calculateDisplayValue = (chartData: ChartData, unit?: string) => {
  if (!chartData.series || chartData.series.length === 0)
    return {
      value: 0,
      label: 'Ingen data tilgængelig',
      showMetric: false,
    };

  const shouldAverage = shouldUseAverage(unit, chartData.xAxis?.label);
  const allValues = chartData.series.flatMap((series) =>
    series.data.filter((value) => typeof value === 'number' && value > 0)
  );

  if (allValues.length === 0) {
    return { value: 0, label: 'Ingen data tilgængelig', showMetric: false };
  }

  if (shouldAverage) {
    const average =
      allValues.reduce((sum, val) => sum + val, 0) / allValues.length;
    const isTemporal = isTemporalData(
      chartData.xAxis?.label,
      chartData.xAxis?.values
    );

    return {
      value: average,
      label: isTemporal
        ? 'Gennemsnit på tværs af perioder'
        : 'Gennemsnit på tværs af kategorier',
      showMetric: true,
    };
  } else {
    const total = allValues.reduce((sum, val) => sum + val, 0);
    return {
      value: total,
      label: 'Total på tværs af kategorier',
      showMetric: true,
    };
  }
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

  // If we have no data but are in a category with data, render nothing
  if (!transformedData.length && isInCategoryWithData) {
    return null;
  }

  // Generate chart colors based on series (Tailwind v4 compatible)
  const seriesNames = chart.data.series.map((s) => s.name);
  const chartConfig = generateChartConfig(seriesNames);
  const barColors = chartColors.recharts; // Uses CSS custom properties that auto-switch themes
  const displayMetric = calculateDisplayValue(chart.data, chart.unit);

  return (
    <div className="space-y-6">
      {/* Header with download button */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex-1">
          {/* Large display number - Midday style */}
          {displayMetric.showMetric && (
            <div className="space-y-2">
              <AnimatedNumber
                value={displayMetric.value}
                unit={chart.unit}
                className="text-4xl"
              />
              <div className="text-muted-foreground flex items-center space-x-2 text-sm">
                <p>{displayMetric.label}</p>
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
              label={{
                value: chart.unit || '',
                position: 'bottom',
                offset: -5,
                style: {
                  textAnchor: 'middle',
                  fill: 'oklch(var(--color-muted-foreground))',
                  fontSize: '12px',
                  fontWeight: '500',
                },
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
              label={{
                value: chart.unit || '',
                angle: -90,
                position: 'insideLeft',
                style: {
                  textAnchor: 'middle',
                  fill: 'oklch(var(--color-muted-foreground))',
                  fontSize: '12px',
                  fontWeight: '500',
                },
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
