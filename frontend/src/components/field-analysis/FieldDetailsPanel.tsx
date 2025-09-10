'use client';

import React, { useState } from 'react';
import { FieldAnalysisData } from './types';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  copyCoordinatesToClipboard,
} from './coordinateUtils';
import {
  MapPin,
  Copy,
  Check,
  Plane,
  Map,
  TestTube,
  Leaf,
  AlertTriangle,
  Home,
  School,
} from 'lucide-react';

interface FieldDetailsPanelProps {
  fieldData: FieldAnalysisData;
  onClose: () => void;
}

export function FieldDetailsPanel({
  fieldData,
  onClose,
}: FieldDetailsPanelProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  const formatNumber = (num: number, decimals: number = 2): string => {
    return num.toLocaleString('da-DK', { maximumFractionDigits: decimals });
  };

  // Handle coordinate copying
  const handleCopyCoordinates = async () => {
    if (!fieldData.click_coordinates) return;

    const success = await copyCoordinatesToClipboard(
      fieldData.click_coordinates.lat,
      fieldData.click_coordinates.lng
    );

    if (success) {
      setCopiedCoordinates(true);
      setTimeout(() => setCopiedCoordinates(false), 2000);
    }
  };

  // Parse pesticide detail strings (format: "ProductName:dosage; ProductName2:dosage2")
  const parsePesticideDetail = (
    detailString: string | undefined
  ): Array<{ name: string; dosage: number }> => {
    if (!detailString || detailString.trim() === '') return [];

    try {
      return detailString
        .split(';')
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => {
          const [name, dosageStr] = item.split(':');
          return {
            name: name?.trim() || 'Ukendt produkt',
            dosage: parseFloat(dosageStr?.trim() || '0'),
          };
        })
        .filter((item) => item.dosage > 0)
        .sort((a, b) => b.dosage - a.dosage); // Sort by dosage descending
    } catch (e) {
      console.warn('Error parsing pesticide detail:', detailString, e);
      return [];
    }
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

  const getPesticideRiskLevel = (
    belastning: number
  ): { level: string; color: string; description: string } => {
    if (belastning === 0)
      return {
        level: 'Ingen',
        color: 'text-green-600',
        description: 'Ingen registreret pesticidanvendelse',
      };
    if (belastning < 10)
      return {
        level: 'Lav',
        color: 'text-yellow-600',
        description: 'Lav pesticidbelastning',
      };
    if (belastning < 50)
      return {
        level: 'Moderat',
        color: 'text-orange-600',
        description: 'Moderat pesticidbelastning',
      };
    return {
      level: 'Høj',
      color: 'text-red-600',
      description: 'Høj pesticidbelastning',
    };
  };

  const riskLevel = getPesticideRiskLevel(fieldData.total_pesticide_belastning);

  return (
    <div
      className="h-full overflow-y-auto p-4 lg:p-6"
      onTouchStart={handleTouchStart}
    >
      {/* Mobile swipe indicator */}
      <div className="mx-auto mb-4 h-1 w-12 rounded-full bg-gray-300 lg:hidden"></div>

      {/* Header */}
      <div className="mb-4 flex items-center justify-between lg:mb-6">
        <h2 className="text-lg font-bold text-gray-900 lg:text-xl">
          Markdetaljer
        </h2>
        <button
          onClick={onClose}
          className="flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors hover:bg-gray-100 active:bg-gray-200"
          aria-label="Luk panel"
        >
          <svg
            className="h-5 w-5"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M6 18L18 6M6 6l12 12"
            />
          </svg>
        </button>
      </div>

      {/* Basic Information */}
      <div className="mb-4">
        <h3 className="mb-2 text-base font-semibold text-gray-900">
          Grundoplysninger
        </h3>
        <div className="space-y-1 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600">Kommune:</span>
            <span className="font-medium">{fieldData.kommune}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">CVR:</span>
            <span className="font-mono text-xs">{fieldData.cvr_number}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Areal:</span>
            <span className="font-medium">
              {formatNumber(fieldData.area_hectares)} ha
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Afgrøde:</span>
            <span className="font-medium">
              {fieldData.crop_name || 'Ukendt'}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Økologisk:</span>
            <span
              className={`font-medium ${fieldData.is_organic ? 'text-green-600' : 'text-gray-500'}`}
            >
              {fieldData.is_organic ? 'Ja' : 'Nej'}
            </span>
          </div>
        </div>
      </div>

      {/* GPS Coordinates and Skråfoto */}
      {fieldData.click_coordinates && (
        <div className="mb-4">
          <h3 className="mb-2 text-base font-semibold text-gray-900">
            Koordinater
          </h3>
          <div className="rounded-lg bg-blue-50 p-3">
            <div className="mb-2 flex items-center justify-between">
              <span className="flex items-center text-sm font-medium text-blue-800">
                <MapPin className="mr-1 h-4 w-4" />
                GPS Position
              </span>
              <button
                onClick={handleCopyCoordinates}
                className="flex min-h-[32px] items-center rounded bg-blue-100 px-2 py-1 text-xs text-blue-700 transition-colors hover:bg-blue-200 active:bg-blue-300"
                title="Kopier koordinater"
              >
                {copiedCoordinates ? (
                  <>
                    <Check className="mr-1 h-3 w-3" />
                    Kopieret!
                  </>
                ) : (
                  <>
                    <Copy className="mr-1 h-3 w-3" />
                    Kopier
                  </>
                )}
              </button>
            </div>
            <div className="mb-2 font-mono text-xs text-blue-700">
              {formatWgs84Coordinates(
                fieldData.click_coordinates.lat,
                fieldData.click_coordinates.lng
              )}
            </div>
            <div className="flex space-x-2">
              <a
                href={generateSkraafotoUrl(
                  fieldData.click_coordinates.lat,
                  fieldData.click_coordinates.lng
                )}
                target="_blank"
                rel="noopener noreferrer"
                className="flex min-h-[36px] flex-1 items-center justify-center rounded bg-blue-600 px-3 py-2 text-center text-xs font-medium text-white transition-colors hover:bg-blue-700 active:bg-blue-800"
              >
                <Plane className="mr-1 h-3 w-3" />
                Åbn i Skråfoto
              </a>
              <button
                onClick={() => {
                  const coords = fieldData.click_coordinates!;
                  const googleMapsUrl = `https://www.google.com/maps?q=${coords.lat},${coords.lng}`;
                  window.open(googleMapsUrl, '_blank');
                }}
                className="flex min-h-[36px] flex-1 items-center justify-center rounded bg-green-600 px-3 py-2 text-center text-xs font-medium text-white transition-colors hover:bg-green-700 active:bg-green-800"
              >
                <Map className="mr-1 h-3 w-3" />
                Google Maps
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Pesticide Information */}
      <div className="mb-4">
        <h3 className="mb-2 text-base font-semibold text-gray-900">
          Pesticidforbrug
        </h3>
        <div className="mb-2 rounded-lg bg-gray-50 p-3">
          <div className="mb-1 flex items-center justify-between">
            <span className="text-sm font-medium">Samlet belastning</span>
            <span className={`font-bold ${riskLevel.color}`}>
              {formatNumber(fieldData.total_pesticide_belastning)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-xs text-gray-600">Risikoniveau</span>
            <span className={`text-xs font-medium ${riskLevel.color}`}>
              {riskLevel.level}
            </span>
          </div>
        </div>

        {/* Pesticide Products Summary */}
        {fieldData.unique_pesticide_products &&
          fieldData.unique_pesticide_products > 0 && (
            <div className="mb-2 rounded-lg bg-blue-50 p-3">
              <div className="mb-1 flex items-center justify-between">
                <span className="text-sm font-medium text-blue-800">
                  Produkter anvendt
                </span>
                <span className="font-bold text-blue-800">
                  {fieldData.unique_pesticide_products}
                </span>
              </div>
              {fieldData.total_pesticide_applications &&
                fieldData.total_pesticide_applications > 0 && (
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-blue-600">
                      Total applikationer
                    </span>
                    <span className="text-xs font-medium text-blue-800">
                      {fieldData.total_pesticide_applications}
                    </span>
                  </div>
                )}
            </div>
          )}

        {/* Dosage Information - only show if there are any non-zero values */}
        {((fieldData.total_dosage_kg && fieldData.total_dosage_kg > 0) ||
          (fieldData.total_dosage_liters &&
            fieldData.total_dosage_liters > 0) ||
          (fieldData.total_dosage_grams && fieldData.total_dosage_grams > 0) ||
          (fieldData.total_dosage_ml && fieldData.total_dosage_ml > 0) ||
          (fieldData.total_dosage_tablets &&
            fieldData.total_dosage_tablets > 0)) && (
          <div className="space-y-2">
            {/* Show available dosage units */}
            {fieldData.total_dosage_kg && fieldData.total_dosage_kg > 0 && (
              <div className="flex items-center justify-between text-sm">
                <span className="text-gray-600">Total dosering (kg):</span>
                <span className="font-medium">
                  {formatNumber(fieldData.total_dosage_kg, 2)} kg
                </span>
              </div>
            )}
            {fieldData.total_dosage_liters &&
              fieldData.total_dosage_liters > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <span className="text-gray-600">Total dosering (L):</span>
                  <span className="font-medium">
                    {formatNumber(fieldData.total_dosage_liters, 1)} L
                  </span>
                </div>
              )}
            {fieldData.total_dosage_grams &&
              fieldData.total_dosage_grams > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <span className="text-gray-600">Total dosering (g):</span>
                  <span className="font-medium">
                    {formatNumber(fieldData.total_dosage_grams, 0)} g
                  </span>
                </div>
              )}
            {fieldData.total_dosage_ml && fieldData.total_dosage_ml > 0 && (
              <div className="flex items-center justify-between text-sm">
                <span className="text-gray-600">Total dosering (ml):</span>
                <span className="font-medium">
                  {formatNumber(fieldData.total_dosage_ml, 0)} ml
                </span>
              </div>
            )}
            {fieldData.total_dosage_tablets &&
              fieldData.total_dosage_tablets > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <span className="text-gray-600">Total dosering:</span>
                  <span className="font-medium">
                    {fieldData.total_dosage_tablets} tabletter
                  </span>
                </div>
              )}
          </div>
        )}

        {/* Detailed Pesticide Products */}
        {(fieldData.pesticides_kg_detail ||
          fieldData.pesticides_liters_detail ||
          fieldData.pesticides_grams_detail ||
          fieldData.pesticides_ml_detail ||
          fieldData.pesticides_tons_detail) && (
          <div className="mt-3">
            <h4 className="mb-2 text-sm font-medium text-gray-900">
              Anvendte produkter
            </h4>
            <div className="max-h-32 space-y-2 overflow-y-auto">
              {/* Kg products */}
              {parsePesticideDetail(fieldData.pesticides_kg_detail).map(
                (product, index) => (
                  <div
                    key={`kg-${index}`}
                    className="flex items-center justify-between rounded bg-gray-50 p-2 text-xs"
                  >
                    <span className="truncate font-medium text-gray-800">
                      {product.name}
                    </span>
                    <span className="ml-2 flex-shrink-0 text-gray-600">
                      {formatNumber(product.dosage, 2)} kg
                    </span>
                  </div>
                )
              )}

              {/* Liter products */}
              {parsePesticideDetail(fieldData.pesticides_liters_detail).map(
                (product, index) => (
                  <div
                    key={`l-${index}`}
                    className="flex items-center justify-between rounded bg-blue-50 p-2 text-xs"
                  >
                    <span className="truncate font-medium text-blue-800">
                      {product.name}
                    </span>
                    <span className="ml-2 flex-shrink-0 text-blue-600">
                      {formatNumber(product.dosage, 1)} L
                    </span>
                  </div>
                )
              )}

              {/* Gram products */}
              {parsePesticideDetail(fieldData.pesticides_grams_detail).map(
                (product, index) => (
                  <div
                    key={`g-${index}`}
                    className="flex items-center justify-between rounded bg-green-50 p-2 text-xs"
                  >
                    <span className="truncate font-medium text-green-800">
                      {product.name}
                    </span>
                    <span className="ml-2 flex-shrink-0 text-green-600">
                      {formatNumber(product.dosage, 0)} g
                    </span>
                  </div>
                )
              )}

              {/* ML products */}
              {parsePesticideDetail(fieldData.pesticides_ml_detail).map(
                (product, index) => (
                  <div
                    key={`ml-${index}`}
                    className="flex items-center justify-between rounded bg-purple-50 p-2 text-xs"
                  >
                    <span className="truncate font-medium text-purple-800">
                      {product.name}
                    </span>
                    <span className="ml-2 flex-shrink-0 text-purple-600">
                      {formatNumber(product.dosage, 0)} ml
                    </span>
                  </div>
                )
              )}

              {/* Tons products */}
              {parsePesticideDetail(fieldData.pesticides_tons_detail).map(
                (product, index) => (
                  <div
                    key={`t-${index}`}
                    className="flex items-center justify-between rounded bg-orange-50 p-2 text-xs"
                  >
                    <span className="truncate font-medium text-orange-800">
                      {product.name}
                    </span>
                    <span className="ml-2 flex-shrink-0 text-orange-600">
                      {formatNumber(product.dosage, 3)} t
                    </span>
                  </div>
                )
              )}
            </div>
          </div>
        )}

        {/* Chemical-specific information */}
        <div className="space-y-2">
          {/* PFAS Information */}
          {fieldData.pfas_applications && fieldData.pfas_applications > 0 && (
            <div className="rounded-lg bg-red-50 p-2">
              <div className="mb-1 flex items-center justify-between">
                <span className="flex items-center text-sm font-medium text-red-800">
                  <TestTube className="mr-1 h-4 w-4" />
                  PFAS
                </span>
                <span className="text-sm font-bold text-red-800">
                  {fieldData.pfas_applications} apps
                </span>
              </div>
              <div className="space-y-1 text-xs text-red-700">
                {fieldData.total_pfas_active_ingredient_kg &&
                  fieldData.total_pfas_active_ingredient_kg > 0 && (
                    <div className="flex justify-between">
                      <span>Aktivstof:</span>
                      <span className="font-medium">
                        {formatNumber(
                          fieldData.total_pfas_active_ingredient_kg,
                          3
                        )}{' '}
                        kg
                      </span>
                    </div>
                  )}
                {fieldData.total_pfas_belastning &&
                  fieldData.total_pfas_belastning > 0 && (
                    <div className="flex justify-between">
                      <span>Belastning:</span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_pfas_belastning)}
                      </span>
                    </div>
                  )}
              </div>
            </div>
          )}

          {/* Diquat Information */}
          {fieldData.diquat_applications &&
            fieldData.diquat_applications > 0 && (
              <div className="rounded-lg bg-blue-50 p-2">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-sm font-medium text-blue-800">
                    💧 Diquat
                  </span>
                  <span className="text-sm font-bold text-blue-800">
                    {fieldData.diquat_applications} apps
                  </span>
                </div>
                {fieldData.total_diquat_belastning &&
                  fieldData.total_diquat_belastning > 0 && (
                    <div className="flex justify-between text-xs text-blue-700">
                      <span>Belastning:</span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_diquat_belastning)}
                      </span>
                    </div>
                  )}
              </div>
            )}

          {/* Glyphosate Information */}
          {fieldData.glyphosate_applications &&
            fieldData.glyphosate_applications > 0 && (
              <div className="rounded-lg bg-green-50 p-2">
                <div className="mb-1 flex items-center justify-between">
                  <span className="flex items-center text-sm font-medium text-green-800">
                    <Leaf className="mr-1 h-4 w-4" />
                    Glyphosate
                  </span>
                  <span className="text-sm font-bold text-green-800">
                    {fieldData.glyphosate_applications} apps
                  </span>
                </div>
                <div className="space-y-1 text-xs text-green-700">
                  {fieldData.total_glyphosate_active_ingredient_kg &&
                    fieldData.total_glyphosate_active_ingredient_kg > 0 && (
                      <div className="flex justify-between">
                        <span>Aktivstof:</span>
                        <span className="font-medium">
                          {formatNumber(
                            fieldData.total_glyphosate_active_ingredient_kg,
                            3
                          )}{' '}
                          kg
                        </span>
                      </div>
                    )}
                  {fieldData.total_glyphosate_belastning &&
                    fieldData.total_glyphosate_belastning > 0 && (
                      <div className="flex justify-between">
                        <span>Belastning:</span>
                        <span className="font-medium">
                          {formatNumber(fieldData.total_glyphosate_belastning)}
                        </span>
                      </div>
                    )}
                </div>
              </div>
            )}

          {/* Partial coverage warning */}
          {fieldData.is_partial_coverage && (
            <div className="flex items-center space-x-2 rounded-lg bg-orange-50 p-2">
              <AlertTriangle className="h-4 w-4 text-orange-600" />
              <span className="text-xs text-orange-700">
                Delvis markdækning
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Environmental Areas */}
      <div className="mb-4">
        <h3 className="mb-2 text-base font-semibold text-gray-900">
          Miljøområder
        </h3>
        <div className="space-y-2">
          {fieldData.bnbo_area_hectares > 0 && (
            <div className="rounded-lg bg-blue-50 p-2">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-blue-800">
                  💧 BNBO
                </span>
                <span className="text-sm font-bold text-blue-800">
                  {formatNumber(fieldData.bnbo_area_hectares)} ha
                </span>
              </div>
            </div>
          )}

          {fieldData.wetland_area_hectares > 0 && (
            <div className="rounded-lg bg-gray-100 p-2">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-800">
                  💨 Lavbund
                </span>
                <span className="text-sm font-bold text-gray-800">
                  {formatNumber(fieldData.wetland_area_hectares)} ha
                </span>
              </div>
            </div>
          )}

          {fieldData.bnbo_area_hectares === 0 &&
            fieldData.wetland_area_hectares === 0 && (
              <div className="p-2 text-xs text-gray-500 italic">
                Ingen registrerede miljøområder
              </div>
            )}
        </div>
      </div>

      {/* Proximity Information */}
      <div className="mb-4">
        <h3 className="mb-2 text-base font-semibold text-gray-900">
          Nærhedsanalyse
        </h3>
        <div className="space-y-1 text-sm">
          {fieldData.residential_buildings_proximity && (
            <div className="flex justify-between">
              <span className="flex items-center text-gray-600">
                <Home className="mr-1 h-4 w-4" />
                Boliger:
              </span>
              <span className="text-xs font-medium">
                {fieldData.residential_buildings_proximity}
              </span>
            </div>
          )}

          {fieldData.educational_facilities_proximity && (
            <div className="flex justify-between">
              <span className="flex items-center text-gray-600">
                <School className="mr-1 h-4 w-4" />
                Skoler:
              </span>
              <span className="text-xs font-medium">
                {fieldData.educational_facilities_proximity}
              </span>
            </div>
          )}

          {fieldData.water_distance_proximity && (
            <div className="flex justify-between">
              <span className="text-gray-600">🌊 Vand:</span>
              <span className="text-xs font-medium">
                {fieldData.water_distance_proximity}
              </span>
            </div>
          )}

          {!fieldData.residential_buildings_proximity &&
            !fieldData.educational_facilities_proximity &&
            !fieldData.water_distance_proximity && (
              <div className="text-xs text-gray-500 italic">
                Ingen nærhedsdata tilgængelig
              </div>
            )}
        </div>
      </div>
    </div>
  );
}
