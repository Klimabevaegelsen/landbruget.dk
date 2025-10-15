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
import {
  GHS06_SkullCrossbones,
  GHS07_ExclamationMark,
  GHS08_HealthHazard,
  GHS09_Environmental,
  GHS05_Corrosive,
} from './GHSPictograms';

interface FieldDetailsPanelProps {
  fieldData?: FieldAnalysisData;
  coordinates?: { lat: number; lng: number };
  onClose: () => void;
}

export function FieldDetailsPanel({
  fieldData,
  coordinates,
  onClose,
}: FieldDetailsPanelProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  const formatNumber = (
    num: number | undefined | null,
    decimals: number = 2
  ): string | null => {
    // Handle null, undefined, or NaN values
    if (num == null || isNaN(num)) return null;
    // Don't display if the value is effectively zero
    if (num < 0.001) return null;

    // For very small values, show more precision
    if (num < 1 && decimals === 0) {
      return num.toLocaleString('da-DK', { maximumFractionDigits: 3 });
    }

    return num.toLocaleString('da-DK', { maximumFractionDigits: decimals });
  };

  // Handle coordinate copying
  const handleCopyCoordinates = async () => {
    const coords = fieldData?.click_coordinates || coordinates;
    if (!coords) return;

    const success = await copyCoordinatesToClipboard(coords.lat, coords.lng);

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

  // Parse enhanced pesticide detail strings with risk information (format: "ProductName:dosage:unit:health_risk:env_risk:signal_word")
  const parsePesticideDetailWithUnit = (
    detailString: string | undefined
  ): Array<{
    name: string;
    dosage: number;
    unit: string;
    healthRisk?: string;
    envRisk?: string;
    signalWord?: string;
  }> => {
    if (!detailString || detailString.trim() === '') return [];

    try {
      return detailString
        .split(';')
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => {
          const parts = item.split(':');
          const name = parts[0]?.trim() || 'Ukendt produkt';
          const dosage = parseFloat(parts[1]?.trim() || '0');
          const rawUnit = parts[2]?.trim() || 'ukendt';
          const healthRisk = parts[3]?.trim() || undefined;
          const envRisk = parts[4]?.trim() || undefined;
          const signalWord = parts[5]?.trim() || undefined;

          // Convert unit codes to user-friendly names
          const friendlyUnit =
            rawUnit === '2'
              ? 'kg'
              : rawUnit === '4'
                ? 'L'
                : rawUnit === '1'
                  ? 'g'
                  : rawUnit === '5'
                    ? 'ml'
                    : rawUnit === '3'
                      ? 'tabletter'
                      : rawUnit;

          return {
            name,
            dosage,
            unit: friendlyUnit,
            healthRisk:
              healthRisk && healthRisk !== '' ? healthRisk : undefined,
            envRisk: envRisk && envRisk !== '' ? envRisk : undefined,
            signalWord:
              signalWord && signalWord !== '' ? signalWord : undefined,
          };
        })
        .filter((item) => item.dosage > 0)
        .sort((a, b) => b.dosage - a.dosage); // Sort by dosage descending
    } catch (e) {
      console.warn('Error parsing enhanced pesticide detail:', detailString, e);
      return [];
    }
  };

  // Get proper GHS pictogram based on BMD risk classification
  const getRiskIcon = (
    healthRisk?: string,
    envRisk?: string,
    signalWord?: string
  ) => {
    // Most severe: Acute toxicity (GHS06) - Skull and Crossbones
    if (healthRisk?.includes('Meget giftig') || healthRisk?.includes('Tx')) {
      return {
        Icon: GHS06_SkullCrossbones,
        color: 'text-red-700',
        bgColor: 'bg-red-100',
        level: 'Meget giftig',
        ghs: 'GHS06',
      };
    }

    // Severe: Toxic (GHS06) - Skull and Crossbones
    if (healthRisk?.includes('Giftig') || healthRisk?.includes('T')) {
      return {
        Icon: GHS06_SkullCrossbones,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Giftig',
        ghs: 'GHS06',
      };
    }

    // Corrosive (GHS05) - Corrosion
    if (healthRisk?.includes('Ætsende') || healthRisk?.includes('C')) {
      return {
        Icon: GHS05_Corrosive,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Ætsende',
        ghs: 'GHS05',
      };
    }

    // Health hazard (GHS08) - Health Hazard
    if (
      healthRisk?.includes('Sundhedsskadelig') ||
      healthRisk?.includes('Xn')
    ) {
      return {
        Icon: GHS08_HealthHazard,
        color: 'text-orange-600',
        bgColor: 'bg-orange-50',
        level: 'Sundhedsskadelig',
        ghs: 'GHS08',
      };
    }

    // Irritant (GHS07) - Exclamation Mark
    if (
      healthRisk?.includes('Lokalirriterende') ||
      healthRisk?.includes('Xi')
    ) {
      return {
        Icon: GHS07_ExclamationMark,
        color: 'text-yellow-600',
        bgColor: 'bg-yellow-50',
        level: 'Lokalirriterende',
        ghs: 'GHS07',
      };
    }

    // Environmental hazard (GHS09) - Environment
    if (envRisk?.includes('Miljøfarlig') || envRisk?.includes('N')) {
      return {
        Icon: GHS09_Environmental,
        color: 'text-green-700',
        bgColor: 'bg-green-50',
        level: 'Miljøfarlig',
        ghs: 'GHS09',
      };
    }

    // Signal word fallbacks - use appropriate GHS pictograms
    if (signalWord === 'Fare') {
      return {
        Icon: GHS06_SkullCrossbones,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Fare',
        ghs: 'SIGNAL',
      };
    }
    if (signalWord === 'Advarsel') {
      return {
        Icon: GHS07_ExclamationMark,
        color: 'text-yellow-600',
        bgColor: 'bg-yellow-50',
        level: 'Advarsel',
        ghs: 'SIGNAL',
      };
    }

    return null;
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
        color: 'text-conventional',
        description: 'Lav pesticidbelastning',
      };
    if (belastning < 50)
      return {
        level: 'Moderat',
        color: 'text-conventional',
        description: 'Moderat pesticidbelastning',
      };
    return {
      level: 'Høj',
      color: 'text-destructive',
      description: 'Høj pesticidbelastning',
    };
  };

  const riskLevel = fieldData
    ? getPesticideRiskLevel(fieldData.total_pesticide_belastning)
    : { level: 'Ingen', color: 'text-green-600', description: 'Ingen data' };

  return (
    <div
      className="h-full overflow-y-auto p-4 lg:p-6"
      onTouchStart={handleTouchStart}
    >
      {/* Mobile swipe indicator */}
      <div className="bg-border mx-auto mb-4 h-1 w-12 rounded-full lg:hidden"></div>

      {/* Header */}
      <div className="mb-4 flex items-center justify-between lg:mb-6">
        <h2 className="text-foreground text-lg font-bold lg:text-xl">
          {fieldData ? 'Markdetaljer' : 'Koordinater'}
        </h2>
        <button
          onClick={onClose}
          className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
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

      {/* Basic Information - Only show for field data */}
      {fieldData && (
        <div className="mb-6">
          <h3 className="text-foreground mb-4 text-lg font-semibold">
            Grundoplysninger
          </h3>
          <div className="space-y-3 text-sm lg:space-y-2 lg:text-sm">
            <div className="flex items-center justify-between py-1">
              <span className="text-muted-foreground font-medium">
                Kommune:
              </span>
              <span className="text-base font-semibold lg:text-sm">
                {fieldData.kommune}
              </span>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-muted-foreground font-medium">CVR:</span>
              <span className="font-mono text-sm lg:text-xs">
                {fieldData.cvr_number}
              </span>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-muted-foreground font-medium">Areal:</span>
              <span className="text-base font-semibold lg:text-sm">
                {formatNumber(fieldData.area_hectares)} ha
              </span>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-muted-foreground font-medium">
                Afgrøde:
              </span>
              <span className="text-base font-semibold lg:text-sm">
                {fieldData.crop_name || 'Ukendt'}
              </span>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-muted-foreground font-medium">
                Økologisk:
              </span>
              <span
                className={`text-base font-semibold lg:text-sm ${fieldData.is_organic ? 'text-accent-foreground' : 'text-muted-foreground'}`}
              >
                {fieldData.is_organic ? 'Ja' : 'Nej'}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* GPS Coordinates and Skråfoto */}
      {(fieldData?.click_coordinates || coordinates) && (
        <div className="mb-6">
          {fieldData && (
            <h3 className="text-foreground mb-4 text-lg font-semibold">
              Koordinater
            </h3>
          )}
          <div className="bg-primary/10 rounded-lg p-4">
            <div className="mb-2 flex items-center justify-between">
              <span className="text-primary flex items-center text-sm font-medium">
                <MapPin className="mr-1 h-4 w-4" />
                GPS Position
              </span>
              <button
                onClick={handleCopyCoordinates}
                className="bg-primary/20 text-primary hover:bg-primary/30 active:bg-primary/40 flex min-h-[32px] items-center rounded px-2 py-1 text-xs transition-colors"
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
            <div className="text-primary/80 mb-2 font-mono text-xs">
              {(() => {
                const coords = fieldData?.click_coordinates || coordinates!;
                return formatWgs84Coordinates(coords.lat, coords.lng);
              })()}
            </div>
            <div className={fieldData ? 'flex space-x-2' : 'space-y-2'}>
              <a
                href={(() => {
                  const coords = fieldData?.click_coordinates || coordinates!;
                  return generateSkraafotoUrl(coords.lat, coords.lng);
                })()}
                target="_blank"
                rel="noopener noreferrer"
                className={`bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex min-h-[36px] items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors ${
                  fieldData ? 'flex-1' : 'block'
                }`}
              >
                <Plane className="mr-1 h-3 w-3" />
                Åbn i Skråfoto
              </a>
              <button
                onClick={() => {
                  const coords = fieldData?.click_coordinates || coordinates!;
                  const googleMapsUrl = `https://www.google.com/maps?q=${coords.lat},${coords.lng}`;
                  window.open(googleMapsUrl, '_blank');
                }}
                className={`bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex min-h-[36px] items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors ${
                  fieldData ? 'flex-1' : 'w-full'
                }`}
              >
                <Map className="mr-1 h-3 w-3" />
                Google Maps
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Field-specific content - Only show when fieldData is available */}
      {fieldData && (
        <>
          {/* Pesticide Information */}
          <div className="mb-4">
            <h3 className="text-foreground mb-2 text-base font-semibold">
              Pesticidforbrug
            </h3>
            <div className="bg-muted mb-2 rounded-lg p-3">
              <div className="mb-1 flex items-center justify-between">
                <span className="text-sm font-medium">Samlet belastning</span>
                <span className={`font-bold ${riskLevel.color}`}>
                  {formatNumber(fieldData.total_pesticide_belastning)}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground text-xs">
                  Risikoniveau
                </span>
                <span className={`text-xs font-medium ${riskLevel.color}`}>
                  {riskLevel.level}
                </span>
              </div>
            </div>

            {/* Pesticide Products Summary */}
            {fieldData.unique_pesticide_products &&
              fieldData.unique_pesticide_products > 0 && (
                <div className="bg-primary/10 mb-2 rounded-lg p-3">
                  <div className="mb-1 flex items-center justify-between">
                    <span className="text-primary text-sm font-medium">
                      Produkter anvendt
                    </span>
                    <span className="text-primary font-bold">
                      {fieldData.unique_pesticide_products}
                    </span>
                  </div>
                  {fieldData.total_pesticide_applications &&
                    fieldData.total_pesticide_applications > 0 && (
                      <div className="flex items-center justify-between">
                        <span className="text-primary/80 text-xs">
                          Total pesticider
                        </span>
                        <span className="text-primary text-xs font-medium">
                          {fieldData.total_pesticide_applications}
                        </span>
                      </div>
                    )}
                </div>
              )}

            {/* Dosage Information - only show if there are meaningful values */}
            {((fieldData.total_dosage_kg &&
              fieldData.total_dosage_kg > 0.001) ||
              (fieldData.total_dosage_liters &&
                fieldData.total_dosage_liters > 0.001) ||
              (fieldData.total_dosage_grams &&
                fieldData.total_dosage_grams > 0.001) ||
              (fieldData.total_dosage_ml &&
                fieldData.total_dosage_ml > 0.001) ||
              (fieldData.total_dosage_tablets &&
                fieldData.total_dosage_tablets > 0)) && (
              <div className="space-y-2">
                {/* Show available dosage units */}
                {fieldData.total_dosage_kg &&
                  fieldData.total_dosage_kg > 0.001 && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (kg):
                      </span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_dosage_kg, 2)} kg
                      </span>
                    </div>
                  )}
                {fieldData.total_dosage_liters &&
                  fieldData.total_dosage_liters > 0.001 && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (L):
                      </span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_dosage_liters, 1)} L
                      </span>
                    </div>
                  )}
                {fieldData.total_dosage_grams &&
                  fieldData.total_dosage_grams > 0.001 &&
                  formatNumber(fieldData.total_dosage_grams, 0) && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (g):
                      </span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_dosage_grams, 0)} g
                      </span>
                    </div>
                  )}
                {fieldData.total_dosage_ml &&
                  fieldData.total_dosage_ml > 0.001 &&
                  formatNumber(fieldData.total_dosage_ml, 0) && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (ml):
                      </span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_dosage_ml, 0)} ml
                      </span>
                    </div>
                  )}
                {fieldData.total_dosage_tablets &&
                  fieldData.total_dosage_tablets > 0 &&
                  formatNumber(fieldData.total_dosage_tablets, 0) && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering:
                      </span>
                      <span className="font-medium">
                        {formatNumber(fieldData.total_dosage_tablets, 0)}{' '}
                        tabletter
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
              fieldData.pesticides_tablets_detail) && (
              <div className="mt-3">
                <h4 className="text-foreground mb-2 text-sm font-medium">
                  Anvendte produkter
                </h4>
                <div className="max-h-32 space-y-2 overflow-y-auto">
                  {/* Kg products */}
                  {parsePesticideDetail(fieldData.pesticides_kg_detail).map(
                    (product, index) => (
                      <div
                        key={`kg-${index}`}
                        className="bg-muted flex items-center justify-between rounded p-2 text-xs"
                      >
                        <span className="text-foreground truncate font-medium">
                          {product.name}
                        </span>
                        <span className="text-muted-foreground ml-2 flex-shrink-0">
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
                        className="bg-primary/10 flex items-center justify-between rounded p-2 text-xs"
                      >
                        <span className="text-primary truncate font-medium">
                          {product.name}
                        </span>
                        <span className="text-primary/80 ml-2 flex-shrink-0">
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
                        className="bg-muted/50 flex items-center justify-between rounded p-2 text-xs"
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
                        className="bg-bnbo/10 flex items-center justify-between rounded p-2 text-xs"
                      >
                        <span className="text-bnbo truncate font-medium">
                          {product.name}
                        </span>
                        <span className="text-bnbo/80 ml-2 flex-shrink-0">
                          {formatNumber(product.dosage, 0)} ml
                        </span>
                      </div>
                    )
                  )}

                  {/* Tablet products */}
                  {parsePesticideDetail(
                    fieldData.pesticides_tablets_detail
                  ).map((product, index) => (
                    <div
                      key={`tablet-${index}`}
                      className="bg-conventional/10 flex items-center justify-between rounded p-2 text-xs"
                    >
                      <span className="text-conventional truncate font-medium">
                        {product.name}
                      </span>
                      <span className="text-conventional/80 ml-2 flex-shrink-0">
                        {formatNumber(product.dosage, 0)} tabletter
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Categorized Pesticide Products */}
            {(fieldData.pfas_products_detail ||
              fieldData.diquat_products_detail ||
              fieldData.glyphosate_products_detail ||
              fieldData.other_products_detail) && (
              <div className="mt-3">
                <h4 className="text-foreground mb-2 text-sm font-medium">
                  Anvendte pesticider (kategoriseret)
                </h4>
                <div className="max-h-48 space-y-2 overflow-y-auto">
                  {/* PFAS Products */}
                  {fieldData.pfas_products_detail && (
                    <div className="mb-2">
                      <div className="mb-1 text-xs font-medium text-orange-600">
                        🚨 PFAS-holdige produkter (
                        {fieldData.pfas_applications || 0})
                      </div>
                      {parsePesticideDetailWithUnit(
                        fieldData.pfas_products_detail
                      ).map((product, index) => {
                        const riskIcon = getRiskIcon(
                          product.healthRisk,
                          product.envRisk,
                          product.signalWord
                        );
                        return (
                          <div
                            key={`pfas-${index}`}
                            className="mb-1 rounded border-l-4 border-orange-400 bg-orange-50 p-2"
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex-1">
                                <div className="font-medium text-orange-800">
                                  {product.name}
                                </div>
                                <div className="text-sm text-orange-600">
                                  {formatNumber(product.dosage, 2)}{' '}
                                  {product.unit}
                                </div>
                              </div>
                              {riskIcon && (
                                <div
                                  className={`flex items-center gap-1 rounded px-2 py-1 ${riskIcon.bgColor}`}
                                  title={`${riskIcon.ghs} - ${riskIcon.level}`}
                                >
                                  <riskIcon.Icon
                                    className={`h-4 w-4 ${riskIcon.color}`}
                                  />
                                  <span
                                    className={`text-xs font-medium ${riskIcon.color}`}
                                  >
                                    {riskIcon.level}
                                  </span>
                                </div>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {/* Diquat Products */}
                  {fieldData.diquat_products_detail && (
                    <div className="mb-2">
                      <div className="mb-1 text-xs font-medium text-red-600">
                        ⚠️ Diquat-holdige produkter (
                        {fieldData.diquat_applications || 0})
                      </div>
                      {parsePesticideDetailWithUnit(
                        fieldData.diquat_products_detail
                      ).map((product, index) => (
                        <div
                          key={`diquat-${index}`}
                          className="mb-1 rounded border-l-4 border-red-400 bg-red-50 p-2"
                        >
                          <span className="font-medium text-red-800">
                            {product.name}
                          </span>
                          <span className="ml-2 text-red-600">
                            {formatNumber(product.dosage, 2)} {product.unit}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Glyphosate Products */}
                  {fieldData.glyphosate_products_detail && (
                    <div className="mb-2">
                      <div className="mb-1 text-xs font-medium text-yellow-600">
                        🌾 Glyphosat-holdige produkter (
                        {fieldData.glyphosate_applications || 0})
                      </div>
                      {parsePesticideDetailWithUnit(
                        fieldData.glyphosate_products_detail
                      ).map((product, index) => (
                        <div
                          key={`glyphosate-${index}`}
                          className="mb-1 rounded border-l-4 border-yellow-400 bg-yellow-50 p-2"
                        >
                          <span className="font-medium text-yellow-800">
                            {product.name}
                          </span>
                          <span className="ml-2 text-yellow-600">
                            {formatNumber(product.dosage, 2)} {product.unit}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Other Products */}
                  {fieldData.other_products_detail && (
                    <div className="mb-2">
                      <div className="mb-1 text-xs font-medium text-gray-600">
                        🧪 Øvrige produkter ({fieldData.other_applications || 0}
                        )
                      </div>
                      {parsePesticideDetailWithUnit(
                        fieldData.other_products_detail
                      ).map((product, index) => (
                        <div
                          key={`other-${index}`}
                          className="mb-1 rounded border-l-4 border-gray-400 bg-gray-50 p-2"
                        >
                          <span className="font-medium text-gray-800">
                            {product.name}
                          </span>
                          <span className="ml-2 text-gray-600">
                            {formatNumber(product.dosage, 2)} {product.unit}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Chemical-specific information */}
            <div className="space-y-2">
              {/* PFAS Information */}
              {fieldData.pfas_applications &&
                fieldData.pfas_applications > 0 && (
                  <div className="rounded-lg border border-amber-200 bg-amber-50 p-2 dark:border-amber-800 dark:bg-amber-950/20">
                    <div className="mb-1 flex items-center justify-between">
                      <span className="flex items-center text-sm font-medium text-amber-700 dark:text-amber-300">
                        <TestTube className="mr-1 h-4 w-4" />
                        PFAS
                      </span>
                      <span className="text-sm font-bold text-amber-800 dark:text-amber-200">
                        {fieldData.pfas_applications} apps
                      </span>
                    </div>
                    <div className="space-y-1 text-xs text-amber-700/80 dark:text-amber-300/80">
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
                  <div className="rounded-lg border border-purple-200 bg-purple-50 p-2 dark:border-purple-800 dark:bg-purple-950/20">
                    <div className="mb-1 flex items-center justify-between">
                      <span className="text-sm font-medium text-purple-700 dark:text-purple-300">
                        💧 Diquat
                      </span>
                      <span className="text-sm font-bold text-purple-800 dark:text-purple-200">
                        {fieldData.diquat_applications} apps
                      </span>
                    </div>
                    {fieldData.total_diquat_belastning &&
                      fieldData.total_diquat_belastning > 0 && (
                        <div className="flex justify-between text-xs text-purple-700/80 dark:text-purple-300/80">
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
                  <div className="bg-muted/50 rounded-lg p-2">
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
                              {formatNumber(
                                fieldData.total_glyphosate_belastning
                              )}
                            </span>
                          </div>
                        )}
                    </div>
                  </div>
                )}

              {/* Partial coverage warning */}
              {fieldData.is_partial_coverage && (
                <div className="bg-conventional/10 flex items-center space-x-2 rounded-lg p-2">
                  <AlertTriangle className="text-conventional h-4 w-4" />
                  <span className="text-conventional/80 text-xs">
                    Delvis markdækning
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Environmental Areas */}
          <div className="mb-4">
            <h3 className="text-foreground mb-2 text-base font-semibold">
              Miljøområder
            </h3>
            <div className="space-y-2">
              {(fieldData.bnbo_area_hectares ?? 0) > 0 && (
                <div className="bg-primary/10 rounded-lg p-2">
                  <div className="flex items-center justify-between">
                    <span className="text-primary text-sm font-medium">
                      💧 BNBO
                    </span>
                    <span className="text-primary text-sm font-bold">
                      {formatNumber(fieldData.bnbo_area_hectares)} ha
                    </span>
                  </div>
                </div>
              )}

              {(fieldData.wetland_area_hectares ?? 0) > 0 && (
                <div className="bg-muted rounded-lg p-2">
                  <div className="flex items-center justify-between">
                    <span className="text-foreground text-sm font-medium">
                      💨 Lavbund
                    </span>
                    <span className="text-foreground text-sm font-bold">
                      {formatNumber(fieldData.wetland_area_hectares)} ha
                    </span>
                  </div>
                </div>
              )}

              {(fieldData.bnbo_area_hectares ?? 0) === 0 &&
                (fieldData.wetland_area_hectares ?? 0) === 0 && (
                  <div className="text-muted-foreground p-2 text-xs italic">
                    Ingen registrerede miljøområder
                  </div>
                )}
            </div>
          </div>

          {/* Proximity Information */}
          <div className="mb-4">
            <h3 className="text-foreground mb-2 text-base font-semibold">
              Nærhedsanalyse
            </h3>
            <div className="space-y-1 text-sm">
              {fieldData.residential_buildings_proximity && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground flex items-center">
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
                  <span className="text-muted-foreground flex items-center">
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
                  <span className="text-muted-foreground">🌊 Vand:</span>
                  <span className="text-xs font-medium">
                    {fieldData.water_distance_proximity}
                  </span>
                </div>
              )}

              {!fieldData.residential_buildings_proximity &&
                !fieldData.educational_facilities_proximity &&
                !fieldData.water_distance_proximity && (
                  <div className="text-muted-foreground text-xs italic">
                    Ingen nærhedsdata tilgængelig
                  </div>
                )}
            </div>
          </div>
        </>
      )}

      {/* Message for coordinates-only mode */}
      {!fieldData && coordinates && (
        <div className="text-muted-foreground text-xs italic">
          Klik på en landbrugsmark for at se detaljerede oplysninger.
        </div>
      )}
    </div>
  );
}
