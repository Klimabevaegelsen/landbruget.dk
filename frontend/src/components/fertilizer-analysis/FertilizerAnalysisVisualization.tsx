'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { Loader2, Download, BarChart3, Map, AlertTriangle } from 'lucide-react';
import dynamic from 'next/dynamic';
import { FertilizerMapFilters, FertilizerData, FertilizerAnalysisResponse } from '../livestock-analysis/types';
import { FertilizerFilterPanel } from './FertilizerFilterPanel';
import FertilizerTrendPopover from './FertilizerTrendPopover';
import FertilizerStatsModal from './FertilizerStatsModal';
// import { useToast } from '@/hooks/use-toast';

// Dynamically import the map to avoid SSR issues
const FertilizerMap = dynamic(() => import('./FertilizerMap'), {
  ssr: false,
  loading: () => (
    <div className="flex h-96 items-center justify-center">
      <Loader2 className="h-6 w-6 animate-spin" />
      <span className="text-muted-foreground ml-2 text-sm">
        Indlæser gødningskort...
      </span>
    </div>
  ),
});

export default function FertilizerAnalysisVisualization() {
  const [data, setData] = useState<FertilizerAnalysisResponse | null>(null);
  const [allData, setAllData] = useState<FertilizerData[]>([]); // Store unfiltered data for historical analysis
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedCompany, setSelectedCompany] = useState<FertilizerData | null>(null);
  const [selectedCompanyHistory, setSelectedCompanyHistory] = useState<FertilizerData[]>([]);
  const [isPopoverOpen, setIsPopoverOpen] = useState(false);
  const [isStatsModalOpen, setIsStatsModalOpen] = useState(false);
  // const { addToast, removeToast } = useToast();

  // Filters state
  const [filters, setFilters] = useState<FertilizerMapFilters>({
    visualizationMode: 'nitrogen_production',
    year: 2023,
  });

  // Fetch data from local JSON file
  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Fetch from local JSON file
      const response = await fetch('/sample-fertilizer-data.json');
      
      if (!response.ok) {
        throw new Error(`Failed to fetch data: ${response.status}`);
      }

      const jsonData: FertilizerAnalysisResponse = await response.json();
      
      // Store all unfiltered data for historical analysis
      setAllData(jsonData.data);
      
      // Apply filters to the data for map display
      let filteredData = jsonData.data;

      // Filter by year if specified
      if (filters.year) {
        filteredData = filteredData.filter(company => company.year === filters.year);
      }

      // Filter by municipality if specified
      if (filters.municipality) {
        filteredData = filteredData.filter(company => company.municipality === filters.municipality);
      }

      // Filter by CVR if specified (this would come from a CVR search)
      // This is a placeholder for future CVR search functionality

      // Update the response with filtered data
      const filteredResponse: FertilizerAnalysisResponse = {
        ...jsonData,
        data: filteredData,
        summary: {
          ...jsonData.summary,
          total_companies: filteredData.length,
        }
      };

      setData(filteredResponse);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
      console.error('Fetch Error:', err);
    } finally {
      setLoading(false);
    }
  }, [filters]);


  // Fetch data when filters change
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Update filters
  const updateFilters = (newFilters: Partial<FertilizerMapFilters>) => {
    setFilters(prev => ({ ...prev, ...newFilters }));
  };

  // Handle company selection and fetch historical data
  const handleCompanySelect = useCallback((company: FertilizerData | null) => {
    setSelectedCompany(company);
    
    if (company && allData.length > 0) {
      // Find all historical data for this company from unfiltered data
      const companyHistory = allData.filter(c => c.cvr_number === company.cvr_number);
      setSelectedCompanyHistory(companyHistory);
      setIsPopoverOpen(true); // Open the popover when a company is selected
    } else {
      setSelectedCompanyHistory([]);
      setIsPopoverOpen(false);
    }
  }, [allData]);

  // Handle popover close
  const handlePopoverClose = useCallback(() => {
    setIsPopoverOpen(false);
    setSelectedCompany(null);
    setSelectedCompanyHistory([]);
  }, []);

  // Handle data export
  const handleExportData = useCallback(() => {
    if (!data) return;

    const exportData = {
      metadata: {
        exportDate: new Date().toISOString(),
        filters: filters,
        totalRecords: data.data.length,
        description: 'Gødning og næringsstof data eksporteret fra landbruget.dk'
      },
      summary: data.summary,
      data: data.data.map(company => ({
        cvr_number: company.cvr_number,
        company_name: company.company_name,
        municipality: company.municipality,
        year: company.year,
        latitude: company.address_latitude,
        longitude: company.address_longitude,
        nitrogen_production_kg: company.f_303_1_normproduktion_kg_n_ghi_beregnet,
        phosphorus_production_kg: company.f_303_3_normproduktion_kg_p_ghi_beregnet,
        commercial_fertilizer_kg_n: company.f_706_1_samlet_forbrug_af_handelsgoedning_kg_n,
        pig_manure_kg_n: company.f_601_2_svinegylle_kg_n,
        cattle_manure_kg_n: company.f_602_2_kvaeggylle_kg_n,
        poultry_manure_kg_n: company.f_614_2_fjerkregylle_kg_n,
        solid_manure_kg_n: company.f_604_2_fast_goedning_kg_n,
        nutrient_balance: company.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof,
        agricultural_area_ha: company.f_106_1_harmoniareal_ha_ghi_beregnet,
        animal_count: company.c_2006_antal_prod_dyr_aarsdyr,
        biogas_production_kg: company.f_901_virksomhedens_samlede_forbrug_af_kvaelstof_kg_n
      }))
    };

    const blob = new Blob([JSON.stringify(exportData, null, 2)], {
      type: 'application/json',
    });

    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `goedning-data-${filters.year}-${Date.now()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [data, filters]);

  // Calculate data ranges for filter sliders
  const dataRanges = React.useMemo(() => {
    if (!data?.data) return undefined;

    const processedData = data.data.map(company => {
      const totalNitrogen = (
        (company.f_601_2_svinegylle_kg_n || 0) +
        (company.f_602_2_kvaeggylle_kg_n || 0) +
        (company.f_614_2_fjerkregylle_kg_n || 0) +
        (company.f_604_2_fast_goedning_kg_n || 0) +
        (company.f_303_1_normproduktion_kg_n_ghi_beregnet || 0)
      );
      
      const totalPhosphorus = (
        (company.f_303_3_normproduktion_kg_p_ghi_beregnet || 0) +
        (company.c_2021_normproduktion_fosfor_ghi_beregnet || 0)
      );
      
      const commercial = (
        (company.f_703_1_indkoebt_kunstgoedning_fratrukket_solgt_kunstgoedning_kg_n || 0) +
        (company.f_706_1_samlet_forbrug_af_handelsgoedning_kg_n || 0)
      );
      
      // Estimate biogas from manure data since c_607_2_afgasset_biomasse_kg_n doesn't exist
      const biogas = ((company.f_601_2_svinegylle_kg_n || 0) + (company.f_602_2_kvaeggylle_kg_n || 0)) * 0.1;

      return { totalNitrogen, totalPhosphorus, commercial, biogas };
    });

    const nitrogenValues = processedData.map(d => d.totalNitrogen).filter(v => v > 0);
    const phosphorusValues = processedData.map(d => d.totalPhosphorus).filter(v => v > 0);
    const commercialValues = processedData.map(d => d.commercial).filter(v => v > 0);
    const biogasValues = processedData.map(d => d.biogas).filter(v => v > 0);

    return {
      nitrogen: nitrogenValues.length > 0 ? [Math.min(...nitrogenValues), Math.max(...nitrogenValues)] as [number, number] : [0, 1] as [number, number],
      phosphorus: phosphorusValues.length > 0 ? [Math.min(...phosphorusValues), Math.max(...phosphorusValues)] as [number, number] : [0, 1] as [number, number],
      commercial: commercialValues.length > 0 ? [Math.min(...commercialValues), Math.max(...commercialValues)] as [number, number] : [0, 1] as [number, number],
      biogas: biogasValues.length > 0 ? [Math.min(...biogasValues), Math.max(...biogasValues)] as [number, number] : [0, 1] as [number, number],
    };
  }, [data]);

  if (loading) {
    return (
      <div className="flex h-96 items-center justify-center">
        <div className="text-center">
          <Loader2 className="mx-auto h-8 w-8 animate-spin" />
          <p className="mt-2 text-sm text-muted-foreground">
            Indlæser gødnings- og næringsstofdata...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          Fejl ved indlæsning af data: {error}
        </AlertDescription>
      </Alert>
    );
  }

  if (!data) {
    return (
      <Alert>
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          Ingen data tilgængelig
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Gødning & Næringsstoffer</h1>
          <p className="text-muted-foreground">
            Analyse af gødningsproduktion, handelsgødning og næringsstofanvendelse på CVR-niveau
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={handleExportData} disabled={!data}>
            <Download className="h-4 w-4 mr-2" />
            Eksporter Data
          </Button>
          <Button variant="outline" size="sm" onClick={() => setIsStatsModalOpen(true)} disabled={!data}>
            <BarChart3 className="h-4 w-4 mr-2" />
            Statistik
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Samlet Kvælstofproduktion</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-blue-600">
              {(data.summary.total_nitrogen_production_kg / 1000000).toFixed(1)}M
            </div>
            <p className="text-xs text-muted-foreground">kg N/år</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Samlet Fosforproduktion</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-orange-600">
              {(data.summary.total_phosphorus_production_kg / 1000000).toFixed(1)}M
            </div>
            <p className="text-xs text-muted-foreground">kg P/år</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Handelsgødning</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">
              {(data.summary.total_commercial_fertilizer_kg / 1000000).toFixed(1)}M
            </div>
            <p className="text-xs text-muted-foreground">kg N/år</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Biogasproduktion</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-purple-600">
              {(data.summary.total_biogas_production_kg / 1000000).toFixed(1)}M
            </div>
            <p className="text-xs text-muted-foreground">kg biomasse/år</p>
          </CardContent>
        </Card>
      </div>

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 h-[calc(100vh-12rem)]">
        {/* Filter Panel */}
        <div className="lg:col-span-1">
          <FertilizerFilterPanel
            filters={filters}
            onFiltersChange={updateFilters}
            availableFertilizerTypes={data.summary.fertilizer_types}
            availableMunicipalities={data.summary.municipalities}
            availableYears={data.filters.availableYears}
            dataRanges={dataRanges}
          />
        </div>

        {/* Map */}
        <div className="lg:col-span-3">
          <Card className="h-full">
            <CardHeader className="flex-shrink-0">
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Map className="h-5 w-5" />
                  Geografisk Oversigt
                </CardTitle>
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <span>{data.data.length} virksomheder</span>
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0 flex-1 h-full">
              <div className="h-[calc(100%-4rem)]">
                <FertilizerMap
                  data={data.data}
                  filters={filters}
                  onCompanySelect={handleCompanySelect}
                  className="h-full"
                />
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Trend Charts Popover */}
      <FertilizerTrendPopover
        isOpen={isPopoverOpen}
        onClose={handlePopoverClose}
        selectedCompany={selectedCompany}
        selectedCompanyHistory={selectedCompanyHistory}
      />

      {/* Statistics Modal */}
      {data && (
        <FertilizerStatsModal
          isOpen={isStatsModalOpen}
          onClose={() => setIsStatsModalOpen(false)}
          data={data}
        />
      )}
    </div>
  );
}
