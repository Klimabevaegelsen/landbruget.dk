'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { WellLabel } from '@/components/methodology/well-label';
import { MapLegend } from '@/components/methodology/map-legend';
import {
  ESPE_WELL,
  type ScrollyStepId,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

const ESPE_CENTER = { lng: 10.46, lat: 55.202 };

const ESPE_LEGEND = [
  { color: '#3b82f6', label: 'Opland (Espe)' },
  { color: '#ef4444', label: 'Marker (bentazon)' },
  { color: '#ef4444', label: 'Boring', shape: 'circle' as const },
];

interface GwEspeAnnotationsProps {
  step: ScrollyStepId;
}

export function GwEspeAnnotations({ step }: GwEspeAnnotationsProps) {
  return (
    <>
      {step === 'espe' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat + 0.02}
          text="Espe Opland"
          subtext={'~8 km² indvindingsområde'}
        />
      )}
      {step === 'kilden' && (
        <MapLabel
          lng={ESPE_CENTER.lng + 0.015}
          lat={ESPE_CENTER.lat - 0.005}
          text="6 marker · 42,2 ha"
          subtext="Fighter 480 (bentazon)"
        />
      )}
      {step === 'rejsen' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat}
          text="~1,5 år til grundvand"
          variant="muted"
        />
      )}
      {step === 'fundet' && (
        <WellLabel
          lng={ESPE_WELL.lng}
          lat={ESPE_WELL.lat}
          dgu={ESPE_WELL.dgu}
          value={String(ESPE_WELL.detection.conc)}
          unit="µg/L bentazon"
          depth={`${ESPE_WELL.depthM} m`}
        />
      )}
      <MapLegend
        items={ESPE_LEGEND}
        visible={
          step === 'espe' ||
          step === 'kilden' ||
          step === 'rejsen' ||
          step === 'fundet'
        }
      />
    </>
  );
}
