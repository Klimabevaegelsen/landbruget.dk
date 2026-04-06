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
import { ComboChart as ComboChartType, ChartData } from '@/services/data/types';
import { useEffect, useState } from 'react';
import { CustomLegend, DocumentationAccordion } from '@/components/chart';
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
import { translateDestinationType } from '@/lib/translations/animal-transportation';
import {
  ChartContainer,
  ChartTooltip,
  AnimatedNumber,
  EnhancedTooltip,
  CSVDownloadButton,
} from '@/components/chart';

// Helper function to detect if data is temporal (reused from bar chart)
const isTemporalData = (
  xAxisLabel?: string,
  xAxisValues?: (string | number)[]
) => {
  const axisText = xAxisLabel?.toLowerCase() || '';

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

  if (xAxisValues && xAxisValues.length > 0) {
    const firstValue = String(xAxisValues[0]);
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

// Helper function to calculate appropriate display value for combo charts
const calculateComboDisplayValue = (chartData: ChartData, unit?: string) => {
  if (!chartData.series || chartData.series.length === 0)
    return {
      value: 0,
      label: 'Ingen data tilgængelig',
      showMetric: false,
    };

  const barSeries = chartData.series.filter((s) => s.type === 'bar');
  const lineSeries = chartData.series.filter((s) => s.type === 'line');

  // For mixed series types, focus on bar data with temporal awareness
  if (barSeries.length > 0 && lineSeries.length > 0) {
    const barValues = barSeries.flatMap((series) =>
      series.data.filter((value) => typeof value === 'number' && value > 0)
    );

    if (barValues.length === 0) {
      return { value: 0, label: 'Ingen data tilgængelig', showMetric: false };
    }

    const shouldAverage = shouldUseAverage(unit, chartData.xAxis?.label);
    const barSeriesNames = barSeries.map((s) => s.name);

    if (shouldAverage) {
      const average =
        barValues.reduce((sum, val) => sum + val, 0) / barValues.length;
      const isTemporal = isTemporalData(
        chartData.xAxis?.label,
        chartData.xAxis?.values
      );

      return {
        value: average,
        label: isTemporal
          ? `Gennemsnit for ${barSeriesNames.join(', ')} på tværs af perioder`
          : `Gennemsnit for ${barSeriesNames.join(', ')}`,
        showMetric: true,
      };
    } else {
      const total = barValues.reduce((sum, val) => sum + val, 0);
      return {
        value: total,
        label: `Total for ${barSeriesNames.join(', ')}`,
        showMetric: true,
      };
    }
  }

  // For single-type charts, use same logic as bar charts
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
        : 'Gennemsnit på tværs af serier',
      showMetric: true,
    };
  } else {
    const total = allValues.reduce((sum, val) => sum + val, 0);
    return {
      value: total,
      label: 'Total på tværs af serier',
      showMetric: true,
    };
  }
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

// We can reuse the existing transformDataForRecharts function since it already handles our data structure
const transformDataForRecharts = (chartData: ChartData) => {
  const { xAxis, series } = chartData;
  if (!xAxis?.values || !series) return [];

  return xAxis.values.map((value, index) => {
    const dataPoint: { [key: string]: string | number } = {
      name: translateCategoryValue(value), // Translate destination types
    };
    series.forEach((s) => {
      // Use translated series name as key for consistent legend display
      const translatedSeriesName = translateCategoryValue(s.name);
      dataPoint[translatedSeriesName] = s.data[index];
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

  // If we have no data but are in a category with data, render nothing
  if (!transformedData.length && isInCategoryWithData) {
    return null;
  }

  // Separate series by type and yAxis
  const barSeries = chart.data.series.filter((s) => s.type === 'bar');
  const lineSeries = chart.data.series.filter((s) => s.type === 'line');

  // Generate chart colors based on series
  const seriesNames = chart.data.series.map((s) => s.name);
  const chartConfig = generateChartConfig(seriesNames);
  const displayMetric = calculateComboDisplayValue(chart.data, chart.unit);
  const barColors = chartColors.recharts; // Tailwind v4 compatible CSS custom properties

  // Get the colors for each axis
  const leftAxisColor = barSeries.length > 0 ? barColors[0] : undefined;
  const rightAxisColor =
    lineSeries.length > 0 ? barColors[barSeries.length] : undefined;

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
            label={{
              value: chart.unit || '',
              angle: -90,
              position: 'insideLeft',
              style: {
                textAnchor: 'middle',
                fill: leftAxisColor || 'oklch(var(--color-muted-foreground))',
                fontSize: '12px',
                fontWeight: '500',
              },
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
            label={{
              value: chart.unit || '',
              angle: 90,
              position: 'insideRight',
              style: {
                textAnchor: 'middle',
                fill: rightAxisColor || 'oklch(var(--color-muted-foreground))',
                fontSize: '12px',
                fontWeight: '500',
              },
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
          {barSeries.map((s, index) => {
            const translatedSeriesName = translateCategoryValue(s.name);
            return (
              <Bar
                key={s.name}
                dataKey={translatedSeriesName}
                fill={barColors[index % barColors.length]}
                yAxisId="left"
                radius={[2, 2, 0, 0]}
              />
            );
          })}

          {/* Render line series */}
          {lineSeries.map((s, index) => {
            const translatedSeriesName = translateCategoryValue(s.name);
            return (
              <Line
                key={s.name}
                type="monotone"
                dataKey={translatedSeriesName}
                stroke={
                  barColors[(barSeries.length + index) % barColors.length]
                }
                yAxisId="right"
                strokeWidth={2}
                dot={{
                  fill: barColors[
                    (barSeries.length + index) % barColors.length
                  ],
                  strokeWidth: 2,
                  r: 4,
                }}
                activeDot={{
                  r: 6,
                  strokeWidth: 2,
                }}
              />
            );
          })}
        </ComposedChart>
      </ChartContainer>

      {/* Documentation accordion */}
      {chart.documentation && (
        <DocumentationAccordion documentation={chart.documentation} />
      )}
    </div>
  );
}
