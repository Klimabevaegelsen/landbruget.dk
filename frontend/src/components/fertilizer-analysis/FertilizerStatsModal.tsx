'use client';

import React, { useEffect } from 'react';
import { X, BarChart3, TrendingUp, Users, MapPin, Beaker } from 'lucide-react';
import { FertilizerData, FertilizerAnalysisResponse } from '../livestock-analysis/types';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

interface FertilizerStatsModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: FertilizerAnalysisResponse;
}

export function FertilizerStatsModal({ isOpen, onClose, data }: FertilizerStatsModalProps) {
  // Handle ESC key press
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && isOpen) {
        onClose();
      }
    };

    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
    }

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  // Calculate additional statistics
  const stats = {
    totalCompanies: data.data.length,
    averageNitrogenPerCompany: data.data.reduce((sum, c) => sum + (c.f_303_1_normproduktion_kg_n_ghi_beregnet || 0), 0) / data.data.length,
    averagePhosphorusPerCompany: data.data.reduce((sum, c) => sum + (c.f_303_3_normproduktion_kg_p_ghi_beregnet || 0), 0) / data.data.length,
    companiesWithBiogas: data.data.filter(c => (c.f_901_virksomhedens_samlede_forbrug_af_kvaelstof_kg_n || 0) > 0).length,
    totalAgriculturalArea: data.data.reduce((sum, c) => sum + (c.f_106_1_harmoniareal_ha_ghi_beregnet || 0), 0),
    totalAnimals: data.data.reduce((sum, c) => sum + (c.c_2006_antal_prod_dyr_aarsdyr || 0), 0),
    averageNutrientBalance: data.data.reduce((sum, c) => sum + (c.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof || 0), 0) / data.data.length,
    companiesWithSurplus: data.data.filter(c => (c.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof || 0) > 0).length,
    companiesWithDeficit: data.data.filter(c => (c.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof || 0) < 0).length,
    topMunicipalities: Object.entries(
      data.data.reduce((acc, c) => {
        acc[c.municipality] = (acc[c.municipality] || 0) + 1;
        return acc;
      }, {} as Record<string, number>)
    ).sort(([,a], [,b]) => b - a).slice(0, 5)
  };

  return (
    <>
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-black/20 backdrop-blur-sm z-40"
        onClick={onClose}
      />
      
      {/* Modal */}
      <div className="fixed inset-4 bg-white dark:bg-gray-900 rounded-lg shadow-2xl z-50 flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <BarChart3 className="h-6 w-6 text-blue-600" />
            <div>
              <h2 className="text-xl font-semibold text-foreground">
                Statistik - Gødning & Næringsstoffer
              </h2>
              <p className="text-sm text-muted-foreground">
                Detaljeret analyse af det aktuelle datasæt
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-full hover:bg-muted/50 transition-colors"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            
            {/* Overview Stats */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Users className="h-5 w-5" />
                  Virksomheder
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div>
                  <div className="text-2xl font-bold text-blue-600">{stats.totalCompanies}</div>
                  <div className="text-xs text-muted-foreground">Totale virksomheder</div>
                </div>
                <div>
                  <div className="text-lg font-semibold text-green-600">{stats.companiesWithBiogas}</div>
                  <div className="text-xs text-muted-foreground">Med biogasproduktion</div>
                </div>
              </CardContent>
            </Card>

            {/* Production Averages */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <TrendingUp className="h-5 w-5" />
                  Gennemsnitlig Produktion
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div>
                  <div className="text-lg font-bold text-blue-600">
                    {(stats.averageNitrogenPerCompany / 1000).toFixed(1)}k
                  </div>
                  <div className="text-xs text-muted-foreground">kg N per virksomhed</div>
                </div>
                <div>
                  <div className="text-lg font-bold text-orange-600">
                    {(stats.averagePhosphorusPerCompany / 1000).toFixed(1)}k
                  </div>
                  <div className="text-xs text-muted-foreground">kg P per virksomhed</div>
                </div>
              </CardContent>
            </Card>

            {/* Agricultural Area */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <MapPin className="h-5 w-5" />
                  Areal & Dyr
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div>
                  <div className="text-lg font-bold text-green-600">
                    {(stats.totalAgriculturalArea / 1000).toFixed(1)}k
                  </div>
                  <div className="text-xs text-muted-foreground">ha landbrug</div>
                </div>
                <div>
                  <div className="text-lg font-bold text-purple-600">
                    {(stats.totalAnimals / 1000).toFixed(1)}k
                  </div>
                  <div className="text-xs text-muted-foreground">produktionsdyr</div>
                </div>
              </CardContent>
            </Card>

            {/* Nutrient Balance */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Beaker className="h-5 w-5" />
                  Næringsstofbalance
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div>
                  <div className="text-lg font-bold text-emerald-600">{stats.companiesWithSurplus}</div>
                  <div className="text-xs text-muted-foreground">Virksomheder med overskud</div>
                </div>
                <div>
                  <div className="text-lg font-bold text-red-600">{stats.companiesWithDeficit}</div>
                  <div className="text-xs text-muted-foreground">Virksomheder med underskud</div>
                </div>
                <div>
                  <div className="text-sm font-medium">Gennemsnit: {stats.averageNutrientBalance.toFixed(0)} kg N</div>
                </div>
              </CardContent>
            </Card>

            {/* Top Municipalities */}
            <Card className="md:col-span-2">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <MapPin className="h-5 w-5" />
                  Top Kommuner
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {stats.topMunicipalities.map(([municipality, count], index) => (
                    <div key={municipality} className="flex justify-between items-center">
                      <span className="text-sm">{index + 1}. {municipality}</span>
                      <span className="text-sm font-medium">{count} virksomheder</span>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            {/* Summary Overview */}
            <Card className="md:col-span-3 lg:col-span-1">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <BarChart3 className="h-5 w-5" />
                  Datasæt Oversigt
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="text-xs text-muted-foreground">
                  <strong>Total N-produktion:</strong> {(data.summary.total_nitrogen_production_kg / 1000000).toFixed(1)}M kg
                </div>
                <div className="text-xs text-muted-foreground">
                  <strong>Total P-produktion:</strong> {(data.summary.total_phosphorus_production_kg / 1000000).toFixed(1)}M kg
                </div>
                <div className="text-xs text-muted-foreground">
                  <strong>Handelsgødning:</strong> {(data.summary.total_commercial_fertilizer_kg / 1000000).toFixed(1)}M kg
                </div>
                <div className="text-xs text-muted-foreground">
                  <strong>Biogasproduktion:</strong> {(data.summary.total_biogas_production_kg / 1000000).toFixed(1)}M kg
                </div>
                <div className="text-xs text-muted-foreground">
                  <strong>Kommuner:</strong> {data.summary.municipalities.length}
                </div>
                <div className="text-xs text-muted-foreground">
                  <strong>Gødningstyper:</strong> {data.summary.fertilizer_types.length}
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </>
  );
}

export default FertilizerStatsModal;
