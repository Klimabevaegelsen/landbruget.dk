'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { GwEspeAnnotations } from '@/components/methodology-groundwater/scrollytelling/gw-espe-annotations';
import { GwVejenAnnotations } from '@/components/methodology-groundwater/scrollytelling/gw-vejen-annotations';
import type { ScrollyStepId } from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

interface GwMapAnnotationsProps {
  step: ScrollyStepId;
}

export function GwMapAnnotations({ step }: GwMapAnnotationsProps) {
  return (
    <>
      {step === 'intro' && (
        <MapLabel
          lng={10.5}
          lat={56.0}
          text="Fra overflade til grundvand"
          subtext="To stoffer, to rejser"
        />
      )}
      {step === 'billede' && (
        <>
          <MapLabel lng={10.46} lat={55.2} text="Espe" />
          <MapLabel lng={9.15} lat={55.49} text="Vejen" />
        </>
      )}
      <div className="absolute top-3 left-3 z-10">
        <GwEspeAnnotations step={step} />
        <GwVejenAnnotations step={step} />
      </div>
    </>
  );
}
