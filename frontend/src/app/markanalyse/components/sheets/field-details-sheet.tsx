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
} from 'lucide-react';
import { Sheet, SheetContent } from '@/components/ui/sheet';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  copyCoordinatesToClipboard,
} from '@/components/field-analysis/coordinateUtils';

interface FieldDetailsSheetProps {
  field: FieldAnalysisData | null;
  isOpen: boolean;
  onClose: () => void;
}

export function FieldDetailsSheet({
  field,
  isOpen,
  onClose,
}: FieldDetailsSheetProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  if (!field) return null;

  const formatNumber = (num: number, decimals: number = 2): string => {
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
        color: 'text-yellow-600',
        description: 'Lav pesticidbelastning',
        variant: 'default',
      };
    if (belastning < 50)
      return {
        level: 'Moderat',
        color: 'text-orange-600',
        description: 'Moderat pesticidbelastning',
        variant: 'secondary',
      };
    return {
      level: 'Høj',
      color: 'text-red-600',
      description: 'Høj pesticidbelastning',
      variant: 'destructive',
    };
  };

  const riskLevel = getPesticideRiskLevel(field.total_pesticide_belastning);

  return (
    <Sheet open={isOpen} onOpenChange={onClose}>
      <SheetContent
        side="bottom"
        title="Markdetaljer"
        description={field.crop_name || 'Ukendt afgrøde'}
        className="h-[85vh]"
      >
        <ScrollArea className="h-full">
          <div className="space-y-4">
            {/* Basic Information */}
            <Card className="p-4">
              <h3 className="text-foreground mb-3 text-base font-semibold">
                Grundoplysninger
              </h3>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Kommune:</span>
                  <span className="font-medium">{field.kommune}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">CVR:</span>
                  <span className="font-mono text-xs">{field.cvr_number}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Areal:</span>
                  <span className="font-medium">
                    {formatNumber(field.area_hectares)} ha
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Afgrøde:</span>
                  <span className="font-medium">
                    {field.crop_name || 'Ukendt'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Økologisk:</span>
                  <div className="flex items-center gap-2">
                    {field.is_organic ? (
                      <Badge
                        variant="default"
                        className="bg-organic text-white"
                      >
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
              <Card className="p-4">
                <h3 className="text-foreground mb-3 flex items-center gap-2 text-base font-semibold">
                  <MapPin className="h-4 w-4" />
                  Koordinater
                </h3>
                <div className="rounded-lg bg-blue-50 p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <span className="flex items-center text-sm font-medium text-blue-800">
                      GPS Position
                    </span>
                    <button
                      onClick={handleCopyCoordinates}
                      className="touch-target flex items-center rounded bg-blue-100 px-2 py-1 text-xs text-blue-700 transition-colors hover:bg-blue-200 active:bg-blue-300"
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
                      className="touch-target flex flex-1 items-center justify-center rounded bg-blue-600 px-3 py-2 text-center text-xs font-medium text-white transition-colors hover:bg-blue-700 active:bg-blue-800"
                    >
                      <Plane className="mr-1 h-3 w-3" />
                      Skråfoto
                    </a>
                    <button
                      onClick={() => {
                        const coords = field.click_coordinates!;
                        const googleMapsUrl = `https://www.google.com/maps?q=${coords.lat},${coords.lng}`;
                        window.open(googleMapsUrl, '_blank');
                      }}
                      className="touch-target flex flex-1 items-center justify-center rounded bg-green-600 px-3 py-2 text-center text-xs font-medium text-white transition-colors hover:bg-green-700 active:bg-green-800"
                    >
                      <Map className="mr-1 h-3 w-3" />
                      Google Maps
                    </button>
                  </div>
                </div>
              </Card>
            )}

            {/* Pesticide Information */}
            <Card className="p-4">
              <h3 className="text-foreground mb-3 text-base font-semibold">
                Pesticidforbrug
              </h3>
              <div className="bg-muted mb-3 rounded-lg p-3">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-sm font-medium">Samlet belastning</span>
                  <span className={`font-bold ${riskLevel.color}`}>
                    {formatNumber(field.total_pesticide_belastning)}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-muted-foreground text-xs">
                    Risikoniveau
                  </span>
                  <Badge variant={riskLevel.variant} className="text-xs">
                    {riskLevel.level}
                  </Badge>
                </div>
              </div>

              {/* Pesticide Products Summary */}
              {field.unique_pesticide_products &&
                field.unique_pesticide_products > 0 && (
                  <div className="mb-3 rounded-lg bg-blue-50 p-3">
                    <div className="mb-1 flex items-center justify-between">
                      <span className="text-sm font-medium text-blue-800">
                        Produkter anvendt
                      </span>
                      <span className="font-bold text-blue-800">
                        {field.unique_pesticide_products}
                      </span>
                    </div>
                    {field.total_pesticide_applications &&
                      field.total_pesticide_applications > 0 && (
                        <div className="flex items-center justify-between">
                          <span className="text-xs text-blue-600">
                            Total applikationer
                          </span>
                          <span className="text-xs font-medium text-blue-800">
                            {field.total_pesticide_applications}
                          </span>
                        </div>
                      )}
                  </div>
                )}

              {/* Dosage Information */}
              {((field.total_dosage_kg && field.total_dosage_kg > 0) ||
                (field.total_dosage_liters && field.total_dosage_liters > 0) ||
                (field.total_dosage_grams && field.total_dosage_grams > 0) ||
                (field.total_dosage_ml && field.total_dosage_ml > 0) ||
                (field.total_dosage_tablets &&
                  field.total_dosage_tablets > 0)) && (
                <div className="mb-3 space-y-2">
                  {field.total_dosage_kg && field.total_dosage_kg > 0 && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (kg):
                      </span>
                      <span className="font-medium">
                        {formatNumber(field.total_dosage_kg, 2)} kg
                      </span>
                    </div>
                  )}
                  {field.total_dosage_liters &&
                    field.total_dosage_liters > 0 && (
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">
                          Total dosering (L):
                        </span>
                        <span className="font-medium">
                          {formatNumber(field.total_dosage_liters, 1)} L
                        </span>
                      </div>
                    )}
                  {field.total_dosage_grams && field.total_dosage_grams > 0 && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (g):
                      </span>
                      <span className="font-medium">
                        {formatNumber(field.total_dosage_grams, 0)} g
                      </span>
                    </div>
                  )}
                  {field.total_dosage_ml && field.total_dosage_ml > 0 && (
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">
                        Total dosering (ml):
                      </span>
                      <span className="font-medium">
                        {formatNumber(field.total_dosage_ml, 0)} ml
                      </span>
                    </div>
                  )}
                  {field.total_dosage_tablets &&
                    field.total_dosage_tablets > 0 && (
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">
                          Total dosering:
                        </span>
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
                field.pesticides_tons_detail) && (
                <div className="mb-3">
                  <h4 className="text-foreground mb-2 text-sm font-medium">
                    Anvendte produkter
                  </h4>
                  <div className="max-h-32 space-y-2 overflow-y-auto">
                    {/* Kg products */}
                    {parsePesticideDetail(field.pesticides_kg_detail).map(
                      (product, index) => (
                        <div
                          key={`kg-${index}`}
                          className="bg-muted flex items-center justify-between rounded p-2 text-xs"
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
                    {parsePesticideDetail(field.pesticides_grams_detail).map(
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
                    {parsePesticideDetail(field.pesticides_ml_detail).map(
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
                    {parsePesticideDetail(field.pesticides_tons_detail).map(
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
                {field.pfas_applications && field.pfas_applications > 0 && (
                  <div className="rounded-lg bg-red-50 p-2">
                    <div className="mb-1 flex items-center justify-between">
                      <span className="flex items-center text-sm font-medium text-red-800">
                        <TestTube className="mr-1 h-4 w-4" />
                        PFAS
                      </span>
                      <span className="text-sm font-bold text-red-800">
                        {field.pfas_applications} apps
                      </span>
                    </div>
                    <div className="space-y-1 text-xs text-red-700">
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
                  <div className="rounded-lg bg-blue-50 p-2">
                    <div className="mb-1 flex items-center justify-between">
                      <span className="text-sm font-medium text-blue-800">
                        💧 Diquat
                      </span>
                      <span className="text-sm font-bold text-blue-800">
                        {field.diquat_applications} apps
                      </span>
                    </div>
                    {field.total_diquat_belastning &&
                      field.total_diquat_belastning > 0 && (
                        <div className="flex justify-between text-xs text-blue-700">
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
                    <div className="rounded-lg bg-green-50 p-2">
                      <div className="mb-1 flex items-center justify-between">
                        <span className="flex items-center text-sm font-medium text-green-800">
                          <Leaf className="mr-1 h-4 w-4" />
                          Glyphosate
                        </span>
                        <span className="text-sm font-bold text-green-800">
                          {field.glyphosate_applications} apps
                        </span>
                      </div>
                      <div className="space-y-1 text-xs text-green-700">
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
                                {formatNumber(
                                  field.total_glyphosate_belastning
                                )}
                              </span>
                            </div>
                          )}
                      </div>
                    </div>
                  )}

                {/* Partial coverage warning */}
                {field.is_partial_coverage && (
                  <div className="flex items-center space-x-2 rounded-lg bg-orange-50 p-2">
                    <AlertTriangle className="h-4 w-4 text-orange-600" />
                    <span className="text-xs text-orange-700">
                      Delvis markdækning
                    </span>
                  </div>
                )}
              </div>
            </Card>

            {/* Environmental Areas */}
            <Card className="p-4">
              <h3 className="text-foreground mb-3 text-base font-semibold">
                Miljøområder
              </h3>
              <div className="space-y-2">
                {field.bnbo_area_hectares > 0 && (
                  <div className="rounded-lg bg-blue-50 p-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-blue-800">
                        💧 BNBO
                      </span>
                      <span className="text-sm font-bold text-blue-800">
                        {formatNumber(field.bnbo_area_hectares)} ha
                      </span>
                    </div>
                  </div>
                )}

                {field.wetland_area_hectares > 0 && (
                  <div className="rounded-lg bg-gray-100 p-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-gray-800">
                        💨 Lavbund
                      </span>
                      <span className="text-sm font-bold text-gray-800">
                        {formatNumber(field.wetland_area_hectares)} ha
                      </span>
                    </div>
                  </div>
                )}

                {field.bnbo_area_hectares === 0 &&
                  field.wetland_area_hectares === 0 && (
                    <div className="text-muted-foreground p-2 text-xs italic">
                      Ingen registrerede miljøområder
                    </div>
                  )}
              </div>
            </Card>

            {/* Proximity Information */}
            <Card className="p-4">
              <h3 className="text-foreground mb-3 text-base font-semibold">
                Nærhedsanalyse
              </h3>
              <div className="space-y-2 text-sm">
                {field.residential_buildings_proximity && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground flex items-center">
                      <Home className="mr-1 h-4 w-4" />
                      Boliger:
                    </span>
                    <span className="text-xs font-medium">
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
                    <span className="text-xs font-medium">
                      {field.educational_facilities_proximity}
                    </span>
                  </div>
                )}

                {field.water_distance_proximity && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">🌊 Vand:</span>
                    <span className="text-xs font-medium">
                      {field.water_distance_proximity}
                    </span>
                  </div>
                )}

                {!field.residential_buildings_proximity &&
                  !field.educational_facilities_proximity &&
                  !field.water_distance_proximity && (
                    <div className="text-muted-foreground text-xs italic">
                      Ingen nærhedsdata tilgængelig
                    </div>
                  )}
              </div>
            </Card>
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  );
}
