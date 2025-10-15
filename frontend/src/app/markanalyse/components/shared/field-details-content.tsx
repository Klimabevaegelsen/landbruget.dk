'use client';

import React, { useState } from 'react';
import {
  MapPin,
  Leaf,
  AlertTriangle,
  Copy,
  Check,
  Plane,
  Map,
  TestTube,
  Home,
  School,
  Skull,
  ShieldAlert,
  Trees,
  OctagonAlert,
  TriangleAlert,
} from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  copyCoordinatesToClipboard,
} from '@/components/field-analysis/coordinateUtils';

interface FieldDetailsContentProps {
  field: FieldAnalysisData;
}

export function FieldDetailsContent({ field }: FieldDetailsContentProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  // DEBUG: Log all detail fields to find "00" or "000"
  console.log('🔍 DEBUG Field Details:', {
    pfas_products_detail: field.pfas_products_detail,
    diquat_products_detail: field.diquat_products_detail,
    glyphosate_products_detail: field.glyphosate_products_detail,
    other_products_detail: field.other_products_detail,
    pesticides_kg_detail: field.pesticides_kg_detail,
    pesticides_liters_detail: field.pesticides_liters_detail,
    pesticides_grams_detail: field.pesticides_grams_detail,
    pesticides_ml_detail: field.pesticides_ml_detail,
    pesticides_tablets_detail: field.pesticides_tablets_detail,
  });

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
    if (!field.click_coordinates) return;

    const success = await copyCoordinatesToClipboard(
      field.click_coordinates.lat,
      field.click_coordinates.lng
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

    // DEBUG: Check if detailString is literally "00" or "000"
    if (detailString === '00' || detailString === '000') {
      console.error(`🚨 FOUND LITERAL "${detailString}" IN parsePesticideDetail!`);
    }

    try {
      const items = detailString
        .split(';')
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => {
          const [name, dosageStr] = item.split(':');
          const parsedItem = {
            name: name?.trim() || 'Ukendt produkt',
            dosage: parseFloat(dosageStr?.trim() || '0'),
          };
          // DEBUG: Log malformed items
          if (!name || name.trim() === '') {
            console.warn('⚠️  Malformed pesticide item (empty name):', item, parsedItem);
          }
          return parsedItem;
        })
        .filter((item) => item.dosage > 0)
        .sort((a, b) => b.dosage - a.dosage); // Sort by dosage descending
      
      // DEBUG: Log if we filtered everything out but had input
      if (items.length === 0 && detailString.trim() !== '') {
        console.warn('⚠️  All items filtered out from:', detailString);
      }
      
      return items;
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

    // DEBUG: Check if detailString is literally "00" or "000"
    if (detailString === '00' || detailString === '000') {
      console.error(`🚨 FOUND LITERAL "${detailString}" IN parsePesticideDetailWithUnit!`);
    }

    try {
      const items = detailString
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

          // DEBUG: Log malformed items
          if (!name || name.trim() === '') {
            console.warn('⚠️  Malformed enhanced pesticide item (empty name):', item, parts);
          }

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
      
      // DEBUG: Log if we filtered everything out but had input
      if (items.length === 0 && detailString.trim() !== '') {
        console.warn('⚠️  All enhanced items filtered out from:', detailString);
      }
      
      return items;
    } catch (e) {
      console.warn('Error parsing enhanced pesticide detail:', detailString, e);
      return [];
    }
  };

  // Get proper hazard icon based on BMD risk classification (matching GHS pictograms)
  const getRiskIcon = (
    healthRisk?: string,
    envRisk?: string,
    signalWord?: string
  ) => {
    // Most severe: Acute toxicity (GHS06)
    if (healthRisk?.includes('Meget giftig') || healthRisk?.includes('Tx')) {
      return {
        Icon: Skull,
        color: 'text-red-700',
        bgColor: 'bg-red-100',
        level: 'Meget giftig',
        ghs: 'GHS06',
      };
    }

    // Severe: Toxic (GHS06)
    if (healthRisk?.includes('Giftig') || healthRisk?.includes('T')) {
      return {
        Icon: Skull,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Giftig',
        ghs: 'GHS06',
      };
    }

    // Corrosive (GHS05)
    if (healthRisk?.includes('Ætsende') || healthRisk?.includes('C')) {
      return {
        Icon: ShieldAlert,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Ætsende',
        ghs: 'GHS05',
      };
    }

    // Health hazard (GHS08)
    if (
      healthRisk?.includes('Sundhedsskadelig') ||
      healthRisk?.includes('Xn')
    ) {
      return {
        Icon: AlertTriangle,
        color: 'text-orange-600',
        bgColor: 'bg-orange-50',
        level: 'Sundhedsskadelig',
        ghs: 'GHS08',
      };
    }

    // Irritant (GHS07)
    if (
      healthRisk?.includes('Lokalirriterende') ||
      healthRisk?.includes('Xi')
    ) {
      return {
        Icon: TriangleAlert,
        color: 'text-yellow-600',
        bgColor: 'bg-yellow-50',
        level: 'Lokalirriterende',
        ghs: 'GHS07',
      };
    }

    // Environmental hazard (GHS09)
    if (envRisk?.includes('Miljøfarlig') || envRisk?.includes('N')) {
      return {
        Icon: Trees,
        color: 'text-green-700',
        bgColor: 'bg-green-50',
        level: 'Miljøfarlig',
        ghs: 'GHS09',
      };
    }

    // Signal word fallbacks
    if (signalWord === 'Fare') {
      return {
        Icon: OctagonAlert,
        color: 'text-red-600',
        bgColor: 'bg-red-50',
        level: 'Fare',
        ghs: 'SIGNAL',
      };
    }
    if (signalWord === 'Advarsel') {
      return {
        Icon: TriangleAlert,
        color: 'text-yellow-600',
        bgColor: 'bg-yellow-50',
        level: 'Advarsel',
        ghs: 'SIGNAL',
      };
    }

    return null;
  };

  const getPesticideRiskLevel = (
    belastning: number
  ): {
    level: string;
    color: string;
    description: string;
    variant: 'default' | 'secondary' | 'destructive';
  } => {
    if (belastning === 0)
      return {
        level: 'Ingen',
        color: 'text-green-600',
        description: 'Ingen registreret pesticidanvendelse',
        variant: 'secondary',
      };
    if (belastning < 10)
      return {
        level: 'Lav',
        color: 'text-conventional',
        description: 'Lav pesticidbelastning',
        variant: 'default',
      };
    if (belastning < 50)
      return {
        level: 'Moderat',
        color: 'text-conventional',
        description: 'Moderat pesticidbelastning',
        variant: 'secondary',
      };
    return {
      level: 'Høj',
      color: 'text-destructive',
      description: 'Høj pesticidbelastning',
      variant: 'destructive',
    };
  };

  const riskLevel = getPesticideRiskLevel(field.total_pesticide_belastning);

  // Check if there's any meaningful pesticide data
  const hasPesticideData =
    (field.total_pesticide_belastning &&
      field.total_pesticide_belastning > 0.001) ||
    (field.unique_pesticide_products && field.unique_pesticide_products > 0) ||
    (field.total_pesticide_applications &&
      field.total_pesticide_applications > 0) ||
    field.pesticides_kg_detail ||
    field.pesticides_liters_detail ||
    field.pesticides_grams_detail ||
    field.pesticides_ml_detail ||
    field.pesticides_tablets_detail ||
    field.pfas_products_detail ||
    field.diquat_products_detail ||
    field.glyphosate_products_detail ||
    field.other_products_detail;

  // Check if there's any meaningful environmental data
  const hasEnvironmentalData =
    (field.bnbo_area_hectares && field.bnbo_area_hectares > 0.001) ||
    (field.wetland_area_hectares && field.wetland_area_hectares > 0.001);

  return (
    <div className="space-y-4 lg:space-y-6">
      {/* Basic Information */}
      <Card className="p-4 lg:p-6">
        <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
          Grundoplysninger
        </h3>
        <div className="space-y-3 text-sm lg:text-base">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Kommune:</span>
            <span className="font-medium">{field.kommune}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">CVR:</span>
            <span className="font-mono text-xs lg:text-sm">
              {field.cvr_number}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Areal:</span>
            <span className="font-medium">
              {formatNumber(field.area_hectares) || '0'} ha
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Afgrøde:</span>
            <span className="font-medium">{field.crop_name || 'Ukendt'}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Økologisk:</span>
            <div className="flex items-center gap-2">
              {field.is_organic ? (
                <Badge variant="default" className="bg-organic text-white">
                  <Leaf className="mr-1 h-3 w-3" />
                  Ja
                </Badge>
              ) : (
                <span className="text-muted-foreground">Nej</span>
              )}
            </div>
          </div>
        </div>
      </Card>

      {/* GPS Coordinates and Skråfoto */}
      {field.click_coordinates && (
        <Card className="p-4 lg:p-6">
          <h3 className="text-foreground mb-3 flex items-center gap-2 text-base font-semibold lg:text-lg">
            <MapPin className="h-4 w-4 lg:h-5 lg:w-5" />
            Koordinater
          </h3>
          <div className="bg-primary/10 rounded-lg p-3 lg:p-4">
            <div className="mb-2 flex items-center justify-between">
              <span className="text-primary flex items-center text-sm font-medium lg:text-base">
                GPS Koordinater
              </span>
              <button
                onClick={handleCopyCoordinates}
                className="touch-target bg-primary/20 text-primary hover:bg-primary/30 active:bg-primary/40 flex items-center rounded px-2 py-1 text-xs transition-colors lg:px-3 lg:py-2 lg:text-sm"
                title="Kopier koordinater"
              >
                {copiedCoordinates ? (
                  <>
                    <Check className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                    Kopieret!
                  </>
                ) : (
                  <>
                    <Copy className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                    Kopier
                  </>
                )}
              </button>
            </div>
            <div className="text-primary/80 mb-3 font-mono text-xs lg:text-sm">
              {formatWgs84Coordinates(
                field.click_coordinates.lat,
                field.click_coordinates.lng
              )}
            </div>
            <div className="flex space-x-2">
              <a
                href={generateSkraafotoUrl(
                  field.click_coordinates.lat,
                  field.click_coordinates.lng
                )}
                target="_blank"
                rel="noopener noreferrer"
                className="touch-target bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex flex-1 items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors lg:text-sm"
              >
                <Plane className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                Skråfoto
              </a>
              <button
                onClick={() => {
                  const coords = field.click_coordinates!;
                  const googleMapsUrl = `https://www.google.com/maps?q=${coords.lat},${coords.lng}`;
                  window.open(googleMapsUrl, '_blank');
                }}
                className="touch-target bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex flex-1 items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors lg:text-sm"
              >
                <Map className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                Google Maps
              </button>
            </div>
          </div>
        </Card>
      )}

      {/* Pesticide Information */}
      {hasPesticideData && (
        <Card className="p-4 lg:p-6">
          <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
            Pesticidforbrug
          </h3>
          <div className="bg-muted mb-3 rounded-lg p-3 lg:p-4">
            <div className="mb-1 flex items-center justify-between">
              <span className="text-sm font-medium lg:text-base">
                Samlet belastning
              </span>
              <span
                className={`text-lg font-bold lg:text-xl ${riskLevel.color}`}
              >
                {formatNumber(field.total_pesticide_belastning) || '0'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground text-xs lg:text-sm">
                Risikoniveau
              </span>
              <Badge variant={riskLevel.variant} className="text-xs lg:text-sm">
                {riskLevel.level}
              </Badge>
            </div>
          </div>

          {/* Pesticide Products Summary */}
          {field.unique_pesticide_products &&
            field.unique_pesticide_products > 0 && (
              <div className="bg-primary/10 mb-3 rounded-lg p-3 lg:p-4">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-primary text-sm font-medium lg:text-base">
                    Produkter anvendt
                  </span>
                  <span className="text-primary text-lg font-bold lg:text-xl">
                    {field.unique_pesticide_products}
                  </span>
                </div>
                {field.total_pesticide_applications &&
                  field.total_pesticide_applications > 0 && (
                    <div className="flex items-center justify-between">
                      <span className="text-primary/80 text-xs lg:text-sm">
                        Total pesticider
                      </span>
                      <span className="text-primary text-xs font-medium lg:text-sm">
                        {field.total_pesticide_applications}
                      </span>
                    </div>
                  )}
              </div>
            )}

          {/* Dosage Information - only show if there are meaningful values */}
          {((field.total_dosage_kg && field.total_dosage_kg > 0.001) ||
            (field.total_dosage_liters && field.total_dosage_liters > 0.001) ||
            (field.total_dosage_grams && field.total_dosage_grams > 0.001) ||
            (field.total_dosage_ml && field.total_dosage_ml > 0.001) ||
            (field.total_dosage_tablets && field.total_dosage_tablets > 0)) && (
            <div className="mb-3 space-y-2 lg:space-y-3">
              {field.total_dosage_kg && field.total_dosage_kg > 0.001 && (
                <div className="flex items-center justify-between text-sm lg:text-base">
                  <span className="text-muted-foreground">
                    Total dosering (kg):
                  </span>
                  <span className="font-medium">
                    {formatNumber(field.total_dosage_kg, 2)} kg
                  </span>
                </div>
              )}
              {field.total_dosage_liters &&
                field.total_dosage_liters > 0.001 && (
                  <div className="flex items-center justify-between text-sm lg:text-base">
                    <span className="text-muted-foreground">
                      Total dosering (L):
                    </span>
                    <span className="font-medium">
                      {formatNumber(field.total_dosage_liters, 1)} L
                    </span>
                  </div>
                )}
              {field.total_dosage_grams && field.total_dosage_grams > 0.001 && (
                <div className="flex items-center justify-between text-sm lg:text-base">
                  <span className="text-muted-foreground">
                    Total dosering (g):
                  </span>
                  <span className="font-medium">
                    {formatNumber(field.total_dosage_grams, 0)} g
                  </span>
                </div>
              )}
              {field.total_dosage_ml && field.total_dosage_ml > 0.001 && (
                <div className="flex items-center justify-between text-sm lg:text-base">
                  <span className="text-muted-foreground">
                    Total dosering (ml):
                  </span>
                  <span className="font-medium">
                    {formatNumber(field.total_dosage_ml, 0)} ml
                  </span>
                </div>
              )}
              {field.total_dosage_tablets && field.total_dosage_tablets > 0 && (
                <div className="flex items-center justify-between text-sm lg:text-base">
                  <span className="text-muted-foreground">Total dosering:</span>
                  <span className="font-medium">
                    {field.total_dosage_tablets} tabletter
                  </span>
                </div>
              )}
            </div>
          )}

          {/* Detailed Pesticide Products */}
          {(field.pesticides_kg_detail ||
            field.pesticides_liters_detail ||
            field.pesticides_grams_detail ||
            field.pesticides_ml_detail ||
            field.pesticides_tablets_detail) && (
            <div className="mb-3">
              <h4 className="text-foreground mb-2 text-sm font-medium lg:text-base">
                Anvendte produkter
              </h4>
              <div className="max-h-48 space-y-2 overflow-y-auto lg:max-h-64">
                {/* Kg products */}
                {parsePesticideDetail(field.pesticides_kg_detail).map(
                  (product, index) => (
                    <div
                      key={`kg-${index}`}
                      className="bg-muted flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm"
                    >
                      <span className="truncate font-medium">
                        {product.name}
                      </span>
                      <span className="text-muted-foreground ml-2 flex-shrink-0">
                        {formatNumber(product.dosage, 2)} kg
                      </span>
                    </div>
                  )
                )}

                {/* Liter products */}
                {parsePesticideDetail(field.pesticides_liters_detail).map(
                  (product, index) => (
                    <div
                      key={`l-${index}`}
                      className="bg-primary/10 flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm"
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
                {parsePesticideDetail(field.pesticides_grams_detail).map(
                  (product, index) => (
                    <div
                      key={`g-${index}`}
                      className="bg-muted/50 flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm"
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
                {parsePesticideDetail(field.pesticides_ml_detail).map(
                  (product, index) => (
                    <div
                      key={`ml-${index}`}
                      className="bg-bnbo/10 flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm"
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
                {parsePesticideDetail(field.pesticides_tablets_detail).map(
                  (product, index) => (
                    <div
                      key={`tablet-${index}`}
                      className="bg-conventional/10 flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm"
                    >
                      <span className="text-conventional truncate font-medium">
                        {product.name}
                      </span>
                      <span className="text-conventional/80 ml-2 flex-shrink-0">
                        {formatNumber(product.dosage, 0)} tabletter
                      </span>
                    </div>
                  )
                )}
              </div>
            </div>
          )}

          {/* Categorized Pesticide Products */}
          {(field.pfas_products_detail ||
            field.diquat_products_detail ||
            field.glyphosate_products_detail ||
            field.other_products_detail) && (
            <Card className="p-4 lg:p-6">
              <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
                Anvendte pesticider (kategoriseret)
              </h3>
              <div className="max-h-64 space-y-3 overflow-y-auto lg:max-h-80">
                {/* PFAS Products */}
                {field.pfas_products_detail && (
                  <div className="rounded-lg border border-orange-200 bg-orange-50 p-3">
                    <div className="mb-2 text-sm font-medium text-orange-700">
                      🚨 PFAS-holdige produkter ({field.pfas_applications || 0})
                    </div>
                    <div className="space-y-2">
                      {parsePesticideDetailWithUnit(
                        field.pfas_products_detail
                      ).map((product, index) => {
                        const riskIcon = getRiskIcon(
                          product.healthRisk,
                          product.envRisk,
                          product.signalWord
                        );
                        return (
                          <div
                            key={`pfas-${index}`}
                            className="rounded border-l-4 border-orange-400 bg-orange-100 p-2"
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
                  </div>
                )}

                {/* Diquat Products */}
                {field.diquat_products_detail && (
                  <div className="rounded-lg border border-red-200 bg-red-50 p-3">
                    <div className="mb-2 text-sm font-medium text-red-700">
                      ⚠️ Diquat-holdige produkter (
                      {field.diquat_applications || 0})
                    </div>
                    <div className="space-y-2">
                      {parsePesticideDetailWithUnit(
                        field.diquat_products_detail
                      ).map((product, index) => {
                        const riskIcon = getRiskIcon(
                          product.healthRisk,
                          product.envRisk,
                          product.signalWord
                        );
                        return (
                          <div
                            key={`diquat-${index}`}
                            className="rounded border-l-4 border-red-400 bg-red-100 p-2"
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex-1">
                                <div className="font-medium text-red-800">
                                  {product.name}
                                </div>
                                <div className="text-sm text-red-600">
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
                  </div>
                )}

                {/* Glyphosate Products */}
                {field.glyphosate_products_detail && (
                  <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-3">
                    <div className="mb-2 text-sm font-medium text-yellow-700">
                      🌾 Glyphosat-holdige produkter (
                      {field.glyphosate_applications || 0})
                    </div>
                    <div className="space-y-2">
                      {parsePesticideDetailWithUnit(
                        field.glyphosate_products_detail
                      ).map((product, index) => {
                        const riskIcon = getRiskIcon(
                          product.healthRisk,
                          product.envRisk,
                          product.signalWord
                        );
                        return (
                          <div
                            key={`glyphosate-${index}`}
                            className="rounded border-l-4 border-yellow-400 bg-yellow-100 p-2"
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex-1">
                                <div className="font-medium text-yellow-800">
                                  {product.name}
                                </div>
                                <div className="text-sm text-yellow-600">
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
                  </div>
                )}

                {/* Other Products */}
                {field.other_products_detail && (
                  <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                    <div className="mb-2 text-sm font-medium text-gray-700">
                      🧪 Øvrige produkter ({field.other_applications || 0})
                    </div>
                    <div className="space-y-2">
                      {parsePesticideDetailWithUnit(
                        field.other_products_detail
                      ).map((product, index) => {
                        const riskIcon = getRiskIcon(
                          product.healthRisk,
                          product.envRisk,
                          product.signalWord
                        );
                        return (
                          <div
                            key={`other-${index}`}
                            className="rounded border-l-4 border-gray-400 bg-gray-100 p-2"
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex-1">
                                <div className="font-medium text-gray-800">
                                  {product.name}
                                </div>
                                <div className="text-sm text-gray-600">
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
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* Chemical-specific information */}
          <div className="space-y-2 lg:space-y-3">
            {/* PFAS Information */}
            {field.pfas_applications && field.pfas_applications > 0 && (
              <div className="rounded-lg border border-amber-200 bg-amber-50 p-2 lg:p-3 dark:border-amber-800 dark:bg-amber-950/20">
                <div className="mb-1 flex items-center justify-between">
                  <span className="flex items-center text-sm font-medium text-amber-700 lg:text-base dark:text-amber-300">
                    <TestTube className="mr-1 h-4 w-4" />
                    PFAS
                  </span>
                  <span className="text-sm font-bold text-amber-800 lg:text-base dark:text-amber-200">
                    {field.pfas_applications} produkter
                  </span>
                </div>
                <div className="space-y-1 text-xs text-amber-700/80 lg:text-sm dark:text-amber-300/80">
                  {field.total_pfas_active_ingredient_kg &&
                    field.total_pfas_active_ingredient_kg > 0 && (
                      <div className="flex justify-between">
                        <span>Aktivstof:</span>
                        <span className="font-medium">
                          {formatNumber(
                            field.total_pfas_active_ingredient_kg,
                            3
                          )}{' '}
                          kg
                        </span>
                      </div>
                    )}
                  {field.total_pfas_belastning &&
                    field.total_pfas_belastning > 0 && (
                      <div className="flex justify-between">
                        <span>Belastning:</span>
                        <span className="font-medium">
                          {formatNumber(field.total_pfas_belastning)}
                        </span>
                      </div>
                    )}
                </div>
              </div>
            )}

            {/* Diquat Information */}
            {field.diquat_applications && field.diquat_applications > 0 && (
              <div className="rounded-lg border border-purple-200 bg-purple-50 p-2 lg:p-3 dark:border-purple-800 dark:bg-purple-950/20">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-sm font-medium text-purple-700 lg:text-base dark:text-purple-300">
                    💧 Diquat
                  </span>
                  <span className="text-sm font-bold text-purple-800 lg:text-base dark:text-purple-200">
                    {field.diquat_applications} produkter
                  </span>
                </div>
                {field.total_diquat_belastning &&
                  field.total_diquat_belastning > 0 && (
                    <div className="flex justify-between text-xs text-purple-700/80 lg:text-sm dark:text-purple-300/80">
                      <span>Belastning:</span>
                      <span className="font-medium">
                        {formatNumber(field.total_diquat_belastning)}
                      </span>
                    </div>
                  )}
              </div>
            )}

            {/* Glyphosate Information */}
            {field.glyphosate_applications &&
              field.glyphosate_applications > 0 && (
                <div className="rounded-lg border border-blue-200 bg-blue-50 p-2 lg:p-3 dark:border-blue-800 dark:bg-blue-950/20">
                  <div className="mb-1 flex items-center justify-between">
                    <span className="flex items-center text-sm font-medium text-blue-700 lg:text-base dark:text-blue-300">
                      <Leaf className="mr-1 h-4 w-4" />
                      Glyphosate
                    </span>
                    <span className="text-sm font-bold text-blue-800 lg:text-base dark:text-blue-200">
                      {field.glyphosate_applications} produkter
                    </span>
                  </div>
                  <div className="space-y-1 text-xs text-blue-700/80 lg:text-sm dark:text-blue-300/80">
                    {field.total_glyphosate_active_ingredient_kg &&
                      field.total_glyphosate_active_ingredient_kg > 0 && (
                        <div className="flex justify-between">
                          <span>Aktivstof:</span>
                          <span className="font-medium">
                            {formatNumber(
                              field.total_glyphosate_active_ingredient_kg,
                              3
                            )}{' '}
                            kg
                          </span>
                        </div>
                      )}
                    {field.total_glyphosate_belastning &&
                      field.total_glyphosate_belastning > 0 && (
                        <div className="flex justify-between">
                          <span>Belastning:</span>
                          <span className="font-medium">
                            {formatNumber(field.total_glyphosate_belastning)}
                          </span>
                        </div>
                      )}
                  </div>
                </div>
              )}

            {/* Partial coverage warning */}
            {field.is_partial_coverage && (
              <div className="bg-conventional/10 flex items-center space-x-2 rounded-lg p-2 lg:p-3">
                <AlertTriangle className="text-conventional h-4 w-4" />
                <span className="text-conventional/80 text-xs lg:text-sm">
                  Delvis markdækning
                </span>
              </div>
            )}
          </div>
        </Card>
      )}

      {/* Environmental Areas */}
      {hasEnvironmentalData && (
        <Card className="p-4 lg:p-6">
          <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
            Miljøområder
          </h3>
          <div className="space-y-2 lg:space-y-3">
            {(field.bnbo_area_hectares ?? 0) > 0.001 && (
              <div className="bg-primary/10 rounded-lg p-2 lg:p-3">
                <div className="flex items-center justify-between">
                  <span className="text-primary text-sm font-medium lg:text-base">
                    💧 BNBO
                  </span>
                  <span className="text-primary text-sm font-bold lg:text-base">
                    {formatNumber(field.bnbo_area_hectares) || '0'} ha
                  </span>
                </div>
              </div>
            )}

            {(field.wetland_area_hectares ?? 0) > 0.001 && (
              <div className="bg-muted rounded-lg p-2 lg:p-3">
                <div className="flex items-center justify-between">
                  <span className="text-foreground text-sm font-medium lg:text-base">
                    💨 Lavbund
                  </span>
                  <span className="text-foreground text-sm font-bold lg:text-base">
                    {formatNumber(field.wetland_area_hectares) || '0'} ha
                  </span>
                </div>
              </div>
            )}
          </div>
        </Card>
      )}

      {/* Proximity Information */}
      <Card className="p-4 lg:p-6">
        <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
          Nærhedsanalyse
        </h3>
        <div className="space-y-2 text-sm lg:space-y-3 lg:text-base">
          {field.residential_buildings_proximity && (
            <div className="flex justify-between">
              <span className="text-muted-foreground flex items-center">
                <Home className="mr-1 h-4 w-4" />
                Boliger:
              </span>
              <span className="text-xs font-medium lg:text-sm">
                {field.residential_buildings_proximity}
              </span>
            </div>
          )}

          {field.educational_facilities_proximity && (
            <div className="flex justify-between">
              <span className="text-muted-foreground flex items-center">
                <School className="mr-1 h-4 w-4" />
                Skoler:
              </span>
              <span className="text-xs font-medium lg:text-sm">
                {field.educational_facilities_proximity}
              </span>
            </div>
          )}

          {field.water_distance_proximity && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">🌊 Vand:</span>
              <span className="text-xs font-medium lg:text-sm">
                {field.water_distance_proximity}
              </span>
            </div>
          )}

          {!field.residential_buildings_proximity &&
            !field.educational_facilities_proximity &&
            !field.water_distance_proximity && (
              <div className="text-muted-foreground text-xs italic lg:text-sm">
                Ingen nærhedsdata tilgængelig
              </div>
            )}
        </div>
      </Card>
    </div>
  );
}
