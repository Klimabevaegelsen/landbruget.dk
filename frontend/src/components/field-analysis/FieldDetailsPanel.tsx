"use client";

import React from "react";
import { FieldAnalysisData } from "./types";

interface FieldDetailsPanelProps {
  fieldData: FieldAnalysisData;
  onClose: () => void;
}

export function FieldDetailsPanel({ fieldData, onClose }: FieldDetailsPanelProps) {
  const formatNumber = (num: number, decimals: number = 2): string => {
    return num.toLocaleString("da-DK", { maximumFractionDigits: decimals });
  };

  // Handle swipe gestures for mobile
  const handleTouchStart = (e: React.TouchEvent) => {
    const touch = e.touches[0];
    const startX = touch.clientX;

    const handleTouchMove = (moveEvent: TouchEvent) => {
      const currentTouch = moveEvent.touches[0];
      const deltaX = currentTouch.clientX - startX;

      // Swipe right to close (threshold: 100px)
      if (deltaX > 100) {
        onClose();
        document.removeEventListener('touchmove', handleTouchMove);
        document.removeEventListener('touchend', handleTouchEnd);
      }
    };

    const handleTouchEnd = () => {
      document.removeEventListener('touchmove', handleTouchMove);
      document.removeEventListener('touchend', handleTouchEnd);
    };

    document.addEventListener('touchmove', handleTouchMove);
    document.addEventListener('touchend', handleTouchEnd);
  };

  const getPesticideRiskLevel = (belastning: number): { level: string; color: string; description: string } => {
    if (belastning === 0) return { level: "Ingen", color: "text-green-600", description: "Ingen registreret pesticidanvendelse" };
    if (belastning < 10) return { level: "Lav", color: "text-yellow-600", description: "Lav pesticidbelastning" };
    if (belastning < 50) return { level: "Moderat", color: "text-orange-600", description: "Moderat pesticidbelastning" };
    return { level: "Høj", color: "text-red-600", description: "Høj pesticidbelastning" };
  };

  const riskLevel = getPesticideRiskLevel(fieldData.total_pesticide_belastning);

  return (
    <div className="p-4 lg:p-6 h-full overflow-y-auto" onTouchStart={handleTouchStart}>
      {/* Mobile swipe indicator */}
      <div className="lg:hidden w-12 h-1 bg-gray-300 rounded-full mx-auto mb-4"></div>

      {/* Header */}
      <div className="flex items-center justify-between mb-4 lg:mb-6">
        <h2 className="text-lg lg:text-xl font-bold text-gray-900">Markdetaljer</h2>
        <button
          onClick={onClose}
          className="p-2 hover:bg-gray-100 active:bg-gray-200 rounded-full transition-colors min-h-[44px] min-w-[44px] flex items-center justify-center"
          aria-label="Luk panel"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Basic Information */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Grundoplysninger</h3>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600">Mark ID:</span>
            <span className="font-mono text-xs">{fieldData.field_uuid}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Kommune:</span>
            <span className="font-medium">{fieldData.kommune}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">CVR nummer:</span>
            <span className="font-mono">{fieldData.cvr_number}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Areal:</span>
            <span className="font-medium">{formatNumber(fieldData.area_hectares)} ha</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Afgrøde:</span>
            <span className="font-medium">{fieldData.crop_name || "Ukendt"}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Økologisk:</span>
            <span className={`font-medium ${fieldData.is_organic ? "text-green-600" : "text-gray-500"}`}>
              {fieldData.is_organic ? "Ja" : "Nej"}
            </span>
          </div>
        </div>
      </div>

      {/* Pesticide Information */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Pesticidforbrug</h3>
        <div className="bg-gray-50 rounded-lg p-4 mb-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium">Samlet belastning</span>
            <span className={`font-bold text-lg ${riskLevel.color}`}>
              {formatNumber(fieldData.total_pesticide_belastning)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-xs text-gray-600">Risikoniveau</span>
            <span className={`text-xs font-medium ${riskLevel.color}`}>
              {riskLevel.level}
            </span>
          </div>
          <p className="text-xs text-gray-500 mt-2">{riskLevel.description}</p>
        </div>

        <div className="space-y-3">
          {/* PFAS Information */}
          {fieldData.pfas_applications && (
            <div className="bg-red-50 rounded-lg p-3">
              <div className="flex items-center mb-2">
                <span className="text-red-600 mr-2">🧪</span>
                <span className="font-medium text-red-800">PFAS Pesticider</span>
              </div>
              <div className="space-y-1 text-sm text-red-700">
                {/* Show actual dosage units used */}
                {fieldData.total_dosage_liters && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_liters, 1)} L</span>
                  </div>
                )}
                {fieldData.total_dosage_kg && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_kg, 2)} kg</span>
                  </div>
                )}
                {fieldData.total_dosage_grams && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_grams, 0)} g</span>
                  </div>
                )}
                {fieldData.total_dosage_tablets && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{fieldData.total_dosage_tablets} tabletter</span>
                  </div>
                )}
                {fieldData.total_pfas_active_ingredient_kg && (
                  <div className="flex justify-between">
                    <span>Aktivstof:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_pfas_active_ingredient_kg, 3)} kg</span>
                  </div>
                )}
                {fieldData.total_pfas_belastning && (
                  <div className="flex justify-between">
                    <span>Belastning:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_pfas_belastning)}</span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span>Applikationer:</span>
                  <span className="font-medium">{fieldData.pfas_applications}</span>
                </div>
              </div>
            </div>
          )}

          {/* Diquat Information */}
          {fieldData.diquat_applications && (
            <div className="bg-blue-50 rounded-lg p-3">
              <div className="flex items-center mb-2">
                <span className="text-blue-600 mr-2">💧</span>
                <span className="font-medium text-blue-800">Diquat Pesticider</span>
              </div>
              <div className="space-y-1 text-sm text-blue-700">
                {/* Show actual dosage units used */}
                {fieldData.total_dosage_liters && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_liters, 1)} L</span>
                  </div>
                )}
                {fieldData.total_dosage_kg && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_kg, 2)} kg</span>
                  </div>
                )}
                {fieldData.total_dosage_grams && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_grams, 0)} g</span>
                  </div>
                )}
                {fieldData.total_dosage_tablets && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{fieldData.total_dosage_tablets} tabletter</span>
                  </div>
                )}
                {fieldData.total_diquat_belastning && (
                  <div className="flex justify-between">
                    <span>Belastning:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_diquat_belastning)}</span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span>Applikationer:</span>
                  <span className="font-medium">{fieldData.diquat_applications}</span>
                </div>
              </div>
            </div>
          )}

          {/* Glyphosate Information */}
          {fieldData.glyphosate_applications && (
            <div className="bg-green-50 rounded-lg p-3">
              <div className="flex items-center mb-2">
                <span className="text-green-600 mr-2">🌿</span>
                <span className="font-medium text-green-800">Glyphosate Pesticider</span>
              </div>
              <div className="space-y-1 text-sm text-green-700">
                {/* Show actual dosage units used */}
                {fieldData.total_dosage_liters && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_liters, 1)} L</span>
                  </div>
                )}
                {fieldData.total_dosage_kg && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_kg, 2)} kg</span>
                  </div>
                )}
                {fieldData.total_dosage_grams && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_dosage_grams, 0)} g</span>
                  </div>
                )}
                {fieldData.total_dosage_tablets && (
                  <div className="flex justify-between">
                    <span>Dosering:</span>
                    <span className="font-medium">{fieldData.total_dosage_tablets} tabletter</span>
                  </div>
                )}
                {fieldData.total_glyphosate_active_ingredient_kg && (
                  <div className="flex justify-between">
                    <span>Aktivstof:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_glyphosate_active_ingredient_kg, 3)} kg</span>
                  </div>
                )}
                {fieldData.total_glyphosate_belastning && (
                  <div className="flex justify-between">
                    <span>Belastning:</span>
                    <span className="font-medium">{formatNumber(fieldData.total_glyphosate_belastning)}</span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span>Applikationer:</span>
                  <span className="font-medium">{fieldData.glyphosate_applications}</span>
                </div>
              </div>
            </div>
          )}

          {/* Additional pesticide info */}
          <div className="space-y-2 text-sm">
            {fieldData.total_pesticide_applications && (
              <div className="flex justify-between">
                <span className="text-gray-600">Total applikationer:</span>
                <span className="font-medium">{fieldData.total_pesticide_applications}</span>
              </div>
            )}
            {fieldData.unique_pesticide_products && (
              <div className="flex justify-between">
                <span className="text-gray-600">Unikke produkter:</span>
                <span className="font-medium">{fieldData.unique_pesticide_products}</span>
              </div>
            )}
            {fieldData.is_partial_coverage && (
              <div className="flex items-center space-x-2">
                <span className="text-orange-600">⚠️</span>
                <span className="text-sm text-orange-700">Delvis markdækning</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Environmental Areas */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Miljøområder</h3>
        <div className="space-y-3">
          {fieldData.bnbo_area_hectares > 0 && (
            <div className="bg-blue-50 rounded-lg p-3">
              <div className="flex items-center mb-1">
                <span className="text-blue-600 mr-2">💧</span>
                <span className="font-medium text-blue-800">BNBO Område</span>
              </div>
              <div className="text-sm text-blue-700">
                {formatNumber(fieldData.bnbo_area_hectares)} ha boringsnære beskyttelsesområder
              </div>
            </div>
          )}

          {fieldData.wetland_area_hectares > 0 && (
            <div className="bg-gray-50 rounded-lg p-3">
              <div className="flex items-center mb-1">
                <span className="text-gray-600 mr-2">💨</span>
                <span className="font-medium text-gray-800">Lavbundsområde</span>
              </div>
              <div className="text-sm text-gray-700">
                {formatNumber(fieldData.wetland_area_hectares)} ha lavbundsjorder
              </div>
            </div>
          )}

          {fieldData.bnbo_area_hectares === 0 && fieldData.wetland_area_hectares === 0 && (
            <div className="text-sm text-gray-500 italic">
              Ingen registrerede miljøområder på denne mark
            </div>
          )}
        </div>
      </div>

      {/* Proximity Information */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Nærhedsanalyse</h3>
        <div className="space-y-3">
          {fieldData.residential_buildings_proximity && (
            <div className="bg-gray-50 rounded-lg p-3">
              <div className="flex items-center mb-1">
                <span className="text-gray-600 mr-2">🏠</span>
                <span className="font-medium text-gray-800">Boliger i nærheden</span>
              </div>
              <div className="text-sm text-gray-700">
                {fieldData.residential_buildings_proximity}
              </div>
            </div>
          )}

          {fieldData.educational_facilities_proximity && (
            <div className="bg-blue-50 rounded-lg p-3">
              <div className="flex items-center mb-1">
                <span className="text-blue-600 mr-2">🏫</span>
                <span className="font-medium text-blue-800">Uddannelsesinstitutioner</span>
              </div>
              <div className="text-sm text-blue-700">
                {fieldData.educational_facilities_proximity}
              </div>
            </div>
          )}

          {fieldData.water_distance_proximity && (
            <div className="bg-teal-50 rounded-lg p-3">
              <div className="flex items-center mb-1">
                <span className="text-teal-600 mr-2">🌊</span>
                <span className="font-medium text-teal-800">Vandområder</span>
              </div>
              <div className="text-sm text-teal-700">
                {fieldData.water_distance_proximity}
              </div>
            </div>
          )}

          {!fieldData.residential_buildings_proximity &&
           !fieldData.educational_facilities_proximity &&
           !fieldData.water_distance_proximity && (
            <div className="text-sm text-gray-500 italic">
              Ingen nærhedsdata tilgængelig for denne mark
            </div>
          )}
        </div>
      </div>

      {/* Actions */}
      <div className="mt-8 pt-6 border-t">
        <div className="space-y-3">
          <button className="w-full px-4 py-3 lg:py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 active:bg-blue-800 transition-colors text-base lg:text-sm font-medium min-h-[44px]">
            Vis detaljeret rapport
          </button>
          <button className="w-full px-4 py-3 lg:py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 active:bg-gray-300 transition-colors text-base lg:text-sm font-medium min-h-[44px]">
            Eksporter data
          </button>
        </div>
      </div>
    </div>
  );
}
