'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { WellLabel } from '@/components/methodology/well-label';
import { MapLegend } from '@/components/methodology/map-legend';
import {
  VEJEN_WELL,
  type ScrollyStepId,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

const VEJEN_CENTER = { lng: 9.15, lat: 55.49 };

const VEJEN_LEGEND = [
  { color: '#8b5cf6', label: 'Opland (Vejen)' },
  { color: '#f59e0b', label: 'Marker (MCPA)' },
];

interface GwVejenAnnotationsProps {
  step: ScrollyStepId;
}

export function GwVejenAnnotations({ step }: GwVejenAnnotationsProps) {
  return (
    <>
      {step === 'mcpa' && (
        <MapLabel
          lng={VEJEN_CENTER.lng}
          lat={VEJEN_CENTER.lat + 0.02}
          text="Vejen Forsyning"
          subtext="MCPA → metabolit"
        />
      )}
      {step === 'fortidslevn' && (
        <>
          <MapLabel
            lng={VEJEN_CENTER.lng}
            lat={VEJEN_CENTER.lat + 0.02}
            text="Vejen Forsyning"
          />
          <WellLabel
            lng={VEJEN_WELL.lng}
            lat={VEJEN_WELL.lat}
            dgu={VEJEN_WELL.dgu}
            value={String(VEJEN_WELL.detection.conc)}
            unit="µg/L metabolit"
            depth={`${VEJEN_WELL.depthM} m`}
          />
        </>
      )}
      <MapLegend
        items={VEJEN_LEGEND}
        visible={step === 'mcpa' || step === 'fortidslevn'}
      />
    </>
  );
}
