'use client';

import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Legend } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { TrendingUp } from 'lucide-react';
import { FertilizerData } from '../livestock-analysis/types';
import { 
  ChartContainer, 
  ChartTooltip, 
  EnhancedTooltip
} from '@/components/chart';
import { generateChartConfig, chartColors } from '@/lib/chart-colors';

interface FertilizerTrendChartProps {
  companyData: FertilizerData[];
  companyName: string;
}

export function FertilizerTrendChart({ companyData, companyName }: FertilizerTrendChartProps) {
  // Sort data by year
  const sortedData = [...companyData].sort((a, b) => (a.year || 0) - (b.year || 0));

  // Transform data for the chart
  const chartData = sortedData.map(company => ({
    year: company.year,
    nitrogen_production: (company.f_303_1_normproduktion_kg_n_ghi_beregnet || 0) / 1000, // Convert to tons
    phosphorus_production: (company.f_303_3_normproduktion_kg_p_ghi_beregnet || 0) / 1000, // Convert to tons
    commercial_fertilizer: (company.f_706_1_samlet_forbrug_af_handelsgoedning_kg_n || 0) / 1000, // Convert to tons
    pig_manure: (company.f_601_2_svinegylle_kg_n || 0) / 1000,
    cattle_manure: (company.f_602_2_kvaeggylle_kg_n || 0) / 1000,
    poultry_manure: (company.f_614_2_fjerkregylle_kg_n || 0) / 1000,
    solid_manure: (company.f_604_2_fast_goedning_kg_n || 0) / 1000,
    nutrient_balance: company.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof || 0,
    area: company.f_106_1_harmoniareal_ha_ghi_beregnet || 0,
    animal_count: company.c_2006_antal_prod_dyr_aarsdyr || 0
  }));

  // Generate chart configurations for different series
  const productionConfig = generateChartConfig(['nitrogen_production', 'phosphorus_production', 'commercial_fertilizer']);
  const manureConfig = generateChartConfig(['pig_manure', 'cattle_manure', 'poultry_manure', 'solid_manure']);
  const operationsConfig = generateChartConfig(['area', 'animal_count']);
  const balanceConfig = generateChartConfig(['nutrient_balance']);


  if (chartData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Historisk Udvikling
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-center py-8">
            Ingen historiske data tilgængelig for denne virksomhed
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Production Trends */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Produktionsudvikling
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ChartContainer config={productionConfig} className="h-80">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="year" 
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                label={{ value: 'Tons N/P', angle: -90, position: 'insideLeft' }}
                tickLine={false}
                axisLine={false}
              />
              <ChartTooltip content={<EnhancedTooltip />} />
              <Legend />
              <Line 
                type="monotone" 
                dataKey="nitrogen_production" 
                stroke={chartColors.recharts[0]}
                strokeWidth={2}
                name="Kvælstofproduktion"
                dot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="phosphorus_production" 
                stroke={chartColors.recharts[1]}
                strokeWidth={2}
                name="Fosforproduktion"
                dot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="commercial_fertilizer" 
                stroke={chartColors.recharts[2]}
                strokeWidth={2}
                name="Handelsgødning"
                dot={{ r: 4 }}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>

      {/* Manure Types Trends */}
      <Card>
        <CardHeader>
          <CardTitle>Gødningstyper</CardTitle>
        </CardHeader>
        <CardContent>
          <ChartContainer config={manureConfig} className="h-80">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="year" 
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                label={{ value: 'Tons N', angle: -90, position: 'insideLeft' }}
                tickLine={false}
                axisLine={false}
              />
              <ChartTooltip content={<EnhancedTooltip />} />
              <Legend />
              <Line 
                type="monotone" 
                dataKey="pig_manure" 
                stroke={chartColors.recharts[0]}
                strokeWidth={2}
                name="Svinegylle"
                dot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="cattle_manure" 
                stroke={chartColors.recharts[1]}
                strokeWidth={2}
                name="Kvæggylle"
                dot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="poultry_manure" 
                stroke={chartColors.recharts[2]}
                strokeWidth={2}
                name="Fjerkrægylle"
                dot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="solid_manure" 
                stroke={chartColors.recharts[3]}
                strokeWidth={2}
                name="Fast gødning"
                dot={{ r: 4 }}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>

      {/* Farm Scale */}
      <Card>
        <CardHeader>
          <CardTitle>Driftsomfang</CardTitle>
        </CardHeader>
        <CardContent>
          <ChartContainer config={operationsConfig} className="h-80">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="year" 
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                yAxisId="left" 
                label={{ value: 'Hektar', angle: -90, position: 'insideLeft' }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                yAxisId="right" 
                orientation="right" 
                label={{ value: 'Antal dyr', angle: 90, position: 'insideRight' }}
                tickLine={false}
                axisLine={false}
              />
              <ChartTooltip content={<EnhancedTooltip />} />
              <Legend />
              <Line 
                yAxisId="left"
                type="monotone" 
                dataKey="area" 
                stroke={chartColors.recharts[0]}
                strokeWidth={2}
                name="Harmoniareal"
                dot={{ r: 4 }}
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="animal_count" 
                stroke={chartColors.recharts[1]}
                strokeWidth={2}
                name="Antal dyr"
                dot={{ r: 4 }}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>

      {/* Nutrient Balance */}
      <Card>
        <CardHeader>
          <CardTitle>Næringsstofbalance</CardTitle>
        </CardHeader>
        <CardContent>
          <ChartContainer config={balanceConfig} className="h-80">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="year" 
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                label={{ value: 'kg N', angle: -90, position: 'insideLeft' }}
                tickLine={false}
                axisLine={false}
              />
              <ChartTooltip content={<EnhancedTooltip />} />
              <Legend />
              <Line 
                type="monotone" 
                dataKey="nutrient_balance" 
                stroke={chartColors.recharts[0]}
                strokeWidth={2}
                name="Næringsstofbalance"
                dot={{ r: 4 }}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}

export default FertilizerTrendChart;
