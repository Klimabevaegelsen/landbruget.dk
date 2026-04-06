'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { WellLabel } from '@/components/methodology/well-label';
import { MapLegend } from '@/components/methodology/map-legend';
import { ESPE_WELL, VEJEN_WELL, type ScrollyStepId } from './scrolly-constants';

const ESPE_CENTER = { lng: 10.46, lat: 55.202 };
const VEJEN_CENTER = { lng: 9.15, lat: 55.49 };

const ESPE_LEGEND = [
  { color: '#3b82f6', label: 'Opland (Espe)' },
  { color: '#ef4444', label: 'Marker (bentazon)' },
  { color: '#ef4444', label: 'Boring', shape: 'circle' as const },
];
const VEJEN_LEGEND = [
  { color: '#8b5cf6', label: 'Opland (Vejen)' },
  { color: '#f59e0b', label: 'Marker (MCPA)' },
];

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
          text="Pesticider → grundvand"
          subtext="To eksempler fra Danmark"
        />
      )}
      {step === 'location' && (
        <MapLabel
          lng={10.46}
          lat={55.25}
          text="Espe Vandværk"
          subtext="Midtfyn"
        />
      )}
      {step === 'opland' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat + 0.02}
          text="Espe Opland"
          subtext="~8 km² indvindingsområde"
        />
      )}
      {step === 'fields' && (
        <>
          <MapLabel
            lng={ESPE_CENTER.lng}
            lat={ESPE_CENTER.lat + 0.012}
            text="Espe Vandværk"
          />
          <MapLabel
            lng={ESPE_CENTER.lng + 0.015}
            lat={ESPE_CENTER.lat - 0.005}
            text="6 marker · 42,2 ha"
            subtext="Silomajs + vårbyg"
          />
        </>
      )}
      {step === 'ingredient' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat}
          text="Fighter 480 → Bentazon"
        />
      )}
      {step === 'soil' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat}
          text="Koc ~34"
          subtext="Høj mobilitet"
          variant="muted"
        />
      )}
      {step === 'vadose' && (
        <MapLabel
          lng={ESPE_CENTER.lng}
          lat={ESPE_CENTER.lat}
          text="~1,5 år til grundvand"
          variant="muted"
        />
      )}
      {step === 'detection' && (
        <WellLabel
          lng={ESPE_WELL.lng}
          lat={ESPE_WELL.lat}
          dgu={ESPE_WELL.dgu}
          value={String(ESPE_WELL.detection.conc)}
          unit="µg/L bentazon"
          depth={`${ESPE_WELL.depthM} m`}
        />
      )}
      {step === 'doseresponse' && (
        <MapLabel
          lng={ESPE_WELL.lng}
          lat={ESPE_WELL.lat + 0.015}
          text="3.154 oplande"
          subtext="4,4× højere detektion (Q4/Q1)"
        />
      )}
      {step === 'transition' && (
        <>
          <MapLabel lng={10.46} lat={55.2} text="Espe" variant="muted" />
          <MapLabel lng={9.15} lat={55.49} text="Vejen" variant="muted" />
        </>
      )}
      {step === 'metabolite' && (
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
      {step === 'conclusion' && (
        <>
          <MapLabel lng={10.46} lat={55.2} text="Espe" />
          <MapLabel lng={9.15} lat={55.49} text="Vejen" />
        </>
      )}
      <div className="absolute top-3 left-3 z-10">
        <MapLegend
          items={ESPE_LEGEND}
          visible={
            step === 'opland' || step === 'fields' || step === 'detection'
          }
        />
        <MapLegend items={VEJEN_LEGEND} visible={step === 'metabolite'} />
      </div>
    </>
  );
}
