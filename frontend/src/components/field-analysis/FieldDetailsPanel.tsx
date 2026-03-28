'use client';

import React from 'react';
import type { FieldAnalysisData } from './types';
import { getPesticideRiskLevel } from '@/app/markanalyse/components/shared/field-details/pesticide-utils';
import { FieldBasicInfo } from './FieldBasicInfo';
import { FieldCoordinates } from './FieldCoordinates';
import { FieldPesticideInfo } from './FieldPesticideInfo';
import { FieldEnvironmentalAreas } from './FieldEnvironmentalAreas';
import { FieldProximityInfo } from './FieldProximityInfo';

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
  const handleTouchStart = (e: React.TouchEvent) => {
    const touch = e.touches[0];
    const startX = touch.clientX;

    const handleTouchMove = (moveEvent: TouchEvent) => {
      const currentTouch = moveEvent.touches[0];
      if (currentTouch.clientX - startX > 100) {
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

  const riskLevel = fieldData
    ? getPesticideRiskLevel(fieldData.total_pesticide_belastning)
    : {
        level: 'Ingen',
        color: 'text-primary',
        description: 'Ingen data',
        variant: 'secondary' as const,
      };

  const coords = fieldData?.click_coordinates || coordinates;

  return (
    <div
      className="h-full overflow-y-auto p-4 lg:p-6"
      onTouchStart={handleTouchStart}
    >
      {/* Mobile swipe indicator */}
      <div className="bg-border mx-auto mb-4 h-1 w-12 rounded-full lg:hidden" />

      {/* Header */}
      <div className="mb-4 flex items-center justify-between lg:mb-6">
        <h2 className="text-foreground text-lg font-bold lg:text-xl">
          {fieldData ? 'Markdetaljer' : 'Koordinater'}
        </h2>
        <button
          onClick={onClose}
          data-testid="close-details-panel-button"
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

      {fieldData && <FieldBasicInfo fieldData={fieldData} />}

      {coords && (
        <FieldCoordinates
          coordinates={coords}
          showHeading={!!fieldData}
          inlineButtons={!!fieldData}
        />
      )}

      {fieldData && (
        <>
          <FieldPesticideInfo fieldData={fieldData} riskLevel={riskLevel} />
          <FieldEnvironmentalAreas fieldData={fieldData} />
          <FieldProximityInfo fieldData={fieldData} />
        </>
      )}

      {!fieldData && coordinates && (
        <div className="text-muted-foreground text-xs italic">
          Klik på en landbrugsmark for at se detaljerede oplysninger.
        </div>
      )}
    </div>
  );
}
