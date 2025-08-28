import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ComposedChart,
  Line,
} from 'recharts';
import { EnvironmentalComplianceOverview } from '../../environmental/EnvironmentalStatusIndicator';

interface ChartData {
  series: Array<{
    name: string;
    data: Array<{
      name: string;
      value: number;
    }>;
  }>;
  categories?: string[];
}

interface EnvironmentalChartProps {
  chart: {
    _key: string;
    _type: string;
    title: string;
    data: ChartData;
  };
}

// Transform environmental data for better visualization
const transformEnvironmentalData = (data: ChartData) => {
  if (!data || !data.series) return [];

  // For environmental charts, we want to show Danish labels and proper color coding
  return (
    data.series[0]?.data?.map(
      (item: { name: string; value: number }, index: number) => {
        const dataPoint: Record<string, string | number> = { name: item.name };

        data.series.forEach(
          (series: {
            name: string;
            data: Array<{ name: string; value: number }>;
          }) => {
            const value = series.data[index]?.value || 0;
            dataPoint[series.name] = value;
          }
        );

        return dataPoint;
      }
    ) || []
  );
};

// Enhanced tooltip for environmental data
function EnvironmentalTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{
    name: string;
    value: number;
    color: string;
  }>;
  label?: string;
}) {
  if (!active || !payload || !payload.length) {
    return null;
  }

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-md">
      <p className="mb-2 text-base font-semibold">{label}</p>
      {payload.map((entry, index) => (
        <div
          key={`item-${index}`}
          className="mb-1 flex items-center justify-between"
        >
          <div className="flex items-center">
            <div
              className="mr-2 h-3 w-3 rounded"
              style={{ backgroundColor: entry.color }}
            />
            <span className="text-sm">{entry.name}:</span>
          </div>
          <span className="ml-4 text-sm font-medium">
            {typeof entry.value === 'number'
              ? entry.value.toLocaleString('da-DK', {
                  maximumFractionDigits: 1,
                })
              : entry.value}
            {entry.name.includes('%') ? '%' : ' ha'}
          </span>
        </div>
      ))}
    </div>
  );
}

export function BlockEnvironmentalChart({ chart }: EnvironmentalChartProps) {
  const transformedData = transformEnvironmentalData(chart.data);

  if (!transformedData.length) {
    return (
      <div className="flex h-64 items-center justify-center rounded-lg bg-gray-50">
        <div className="text-center">
          <p className="mb-2 text-gray-500">Ingen miljødata tilgængelig</p>
          <p className="text-sm text-gray-400">
            Miljødata vil blive vist når den er tilgængelig
          </p>
        </div>
      </div>
    );
  }

  // Enhanced colors for environmental data
  const environmentalColors = [
    '#ef4444', // Red for action required
    '#22c55e', // Green for completed
    '#3b82f6', // Blue for water covered
    '#f59e0b', // Amber for compliance rate
    '#8b5cf6', // Purple for additional metrics
    '#ec4899', // Pink for rankings
  ];

  // Special handling for environmental compliance overview
  if (chart._key === 'environmental-compliance-overview') {
    // Extract KPI data for the overview component
    const overviewData = {
      totalProblematicHa:
        (transformedData[0]?.['Problematiske Områder (Ha)'] as number) || 0,
      totalDealtWithHa:
        (transformedData[0]?.['Håndterede Områder (Ha)'] as number) || 0,
      compliancePercentage:
        (transformedData[0]?.['Overholdelsesgrad'] as number) || 0,
      waterCoveragePercentage:
        (transformedData[0]?.[
          'Areal til Klima- eller Miljøprojekter'
        ] as number) || 0,
    };

    return (
      <div className="space-y-6">
        <h3 className="text-lg font-semibold text-gray-900">{chart.title}</h3>
        <EnvironmentalComplianceOverview data={overviewData} />
      </div>
    );
  }

  // Determine if this is a combo chart (has both bars and lines)
  const isComboChart = chart._type === 'comboChart';
  const barSeries = chart.data.series.filter((s) => !s.name.includes('%'));
  const lineSeries = chart.data.series.filter((s) => s.name.includes('%'));

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-gray-900">{chart.title}</h3>

      <div style={{ width: '100%', height: 400 }} className="mt-4">
        <ResponsiveContainer>
          {isComboChart ? (
            <ComposedChart data={transformedData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 12, fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
              />

              {/* Left Y-axis for hectares */}
              <YAxis
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 12, fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
                tickFormatter={(value: number) =>
                  `${value.toLocaleString('da-DK')} ha`
                }
              />

              {/* Right Y-axis for percentages */}
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 12, fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
                tickFormatter={(value: number) => `${value}%`}
              />

              <Tooltip content={<EnvironmentalTooltip />} />
              <Legend />

              {/* Render bars for hectare values */}
              {barSeries.map((series, index) => (
                <Bar
                  key={series.name}
                  dataKey={series.name}
                  fill={environmentalColors[index % environmentalColors.length]}
                  yAxisId="left"
                />
              ))}

              {/* Render lines for percentage values */}
              {lineSeries.map((series, index) => (
                <Line
                  key={series.name}
                  type="monotone"
                  dataKey={series.name}
                  stroke={
                    environmentalColors[
                      (barSeries.length + index) % environmentalColors.length
                    ]
                  }
                  strokeWidth={3}
                  dot={{ r: 4 }}
                  yAxisId="right"
                />
              ))}
            </ComposedChart>
          ) : (
            <BarChart data={transformedData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 12, fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
              />
              <YAxis
                tick={{ fontSize: 12, fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
                tickFormatter={(value: number) => value.toLocaleString('da-DK')}
              />
              <Tooltip content={<EnvironmentalTooltip />} />
              <Legend />

              {chart.data.series.map((series, index) => (
                <Bar
                  key={series.name}
                  dataKey={series.name}
                  fill={environmentalColors[index % environmentalColors.length]}
                  radius={[2, 2, 0, 0]}
                />
              ))}
            </BarChart>
          )}
        </ResponsiveContainer>
      </div>

      {/* Environmental status legend */}
      <div className="grid grid-cols-1 gap-2 rounded-lg bg-gray-50 p-3 text-xs text-gray-600 md:grid-cols-3">
        <div className="flex items-center">
          <div className="mr-2 h-3 w-3 rounded bg-red-500" />
          <span>Kræver Handling</span>
        </div>
        <div className="flex items-center">
          <div className="mr-2 h-3 w-3 rounded bg-green-500" />
          <span>Gennemført</span>
        </div>
        <div className="flex items-center">
          <div className="mr-2 h-3 w-3 rounded bg-blue-500" />
          <span>Areal til Klima- eller Miljøprojekter</span>
        </div>
      </div>
    </div>
  );
}

export default BlockEnvironmentalChart;
