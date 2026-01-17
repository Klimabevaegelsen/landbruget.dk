'use client';

import { ClimateEmission } from '@/services/supabase/climate';
import { HorizontalStackedBarChart } from '@/services/supabase/types';
import { BlockBarChart } from '@/components/pagebuilder/pageBlocks/block-bar-chart';

interface CarbonAccountingBreakdownProps {
  emissions: ClimateEmission[];
}

export function CarbonAccountingBreakdown({
  emissions,
}: CarbonAccountingBreakdownProps) {
  // Transform emissions data into chart format
  const chartData: HorizontalStackedBarChart = {
    _key: 'climate-breakdown',
    _type: 'horizontalStackedBarChart',
    title: 'CO₂-udledning fordelt på kategorier',
    data: {
      xAxis: {
        label: 'CO₂e (kg)',
        values: [],
      },
      yAxis: {
        label: 'År',
        values: emissions.map((e) => e.year),
      },
      series: [],
    },
  };

  // Extract all unique categories from emissions
  const categorySet = new Set<string>();
  emissions.forEach((emission) => {
    if (emission.emissions_by_category) {
      Object.keys(emission.emissions_by_category).forEach((cat) =>
        categorySet.add(cat)
      );
    }
  });

  const categories = Array.from(categorySet).sort();

  // Create a series for each category
  chartData.data.series = categories.map((category) => ({
    name: getCategoryLabel(category),
    data: emissions.map((emission) => {
      return emission.emissions_by_category?.[category] || 0;
    }),
  }));

  return <BlockBarChart chart={chartData} />;
}

// Helper function to convert category keys to readable labels
function getCategoryLabel(category: string): string {
  const labels: Record<string, string> = {
    enteric_fermentation: 'Fordøjelse (mave)',
    manure_management: 'Gødningshåndtering',
    fertilizer: 'Gødning',
    crop_residues: 'Afgrøderester',
    fuel_machinery: 'Brændstof & maskiner',
    electricity: 'Elektricitet',
    transport: 'Transport',
    purchased_feed: 'Indkøbt foder',
    land_use_change: 'Arealanvendelsesændringer',
    other: 'Andet',
  };

  return labels[category] || category;
}
