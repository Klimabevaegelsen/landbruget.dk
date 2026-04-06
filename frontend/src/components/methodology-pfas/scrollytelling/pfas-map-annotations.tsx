'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { WellLabel } from '@/components/methodology/well-label';
import { MapLegend } from '@/components/methodology/map-legend';
import {
  SKRYDSTRUP_WELL,
  EXAMPLE_FIELD,
  STATS,
  type PfasScrollyStepId,
} from './scrolly-constants';

const CATCHMENT_CENTER = { lng: 9.255, lat: 55.248 };
const SONDER_FELDING = { lng: 8.83, lat: 55.915 };

const LOCAL_LEGEND = [
  { color: '#8b5cf6', label: 'Opland (indvinding)' },
  { color: '#22c55e', label: 'Marker (sprøjtet)' },
  { color: '#ef4444', label: 'Boring', shape: 'circle' as const },
];
const NATIONAL_LEGEND = [
  { color: '#ef4444', label: 'Overvåget', shape: 'circle' as const },
  { color: '#94a3b8', label: 'Ikke overvåget', shape: 'circle' as const },
];

interface PfasMapAnnotationsProps {
  step: PfasScrollyStepId;
}

export function PfasMapAnnotations({ step }: PfasMapAnnotationsProps) {
  return (
    <>
      {step === 'skjult' && (
        <MapLabel
          lng={10.5}
          lat={56.0}
          text={`${STATS.totalCatchments.toLocaleString('da-DK')} oplande`}
          subtext="i hele Danmark"
        />
      )}
      {step === 'skrydstrup' && (
        <>
          <MapLabel
            lng={CATCHMENT_CENTER.lng}
            lat={CATCHMENT_CENTER.lat + 0.015}
            text="I/S Skrydstrup Vandværk"
            subtext="Haderslev, Sønderjylland"
          />
          <MapLabel
            lng={9.245}
            lat={55.253}
            text="5 marker med fluorpesticider"
          />
        </>
      )}
      {step === 'byggeklodser' &&
        EXAMPLE_FIELD.products.map((p, i) => (
          <MapLabel
            key={p.name}
            lng={[9.243, 9.255, 9.264][i]}
            lat={[55.252, 55.252, 55.248][i]}
            text={`${p.name}`}
            subtext={`CF₃ · ${p.ingredient.split(' + ')[0]}`}
            variant="alert"
          />
        ))}
      {step === 'evighed' && (
        <MapLabel
          lng={CATCHMENT_CENTER.lng}
          lat={CATCHMENT_CENTER.lat + 0.012}
          text="I/S Skrydstrup"
          subtext="TFA spreder sig"
        />
      )}
      {step === 'glasset' && (
        <WellLabel
          lng={SKRYDSTRUP_WELL.lng}
          lat={SKRYDSTRUP_WELL.lat}
          dgu={SKRYDSTRUP_WELL.dgu}
          value={String(SKRYDSTRUP_WELL.detection.conc)}
          unit="µg/L TFA"
          depth={`${SKRYDSTRUP_WELL.depthM} m`}
        />
      )}
      {step === 'overalt' && (
        <MapLabel
          lng={10.5}
          lat={56.3}
          text="2.113 overvågede oplande"
          subtext="TFA fundet i 100 %"
        />
      )}
      {step === 'blindvinkel' && (
        <>
          <MapLabel
            lng={SONDER_FELDING.lng}
            lat={SONDER_FELDING.lat + 0.025}
            text="Søndre-Felding Vandværk"
            subtext="Ingen PFAS-målinger"
            variant="muted"
          />
          <MapLabel
            lng={8.81}
            lat={55.925}
            text="Marker med fluorpesticider"
            subtext="Ingen overvågning"
          />
        </>
      )}
      <div className="absolute bottom-3 left-3 z-10">
        <MapLegend
          items={LOCAL_LEGEND}
          visible={
            step === 'skrydstrup' ||
            step === 'byggeklodser' ||
            step === 'glasset'
          }
        />
        <MapLegend
          items={NATIONAL_LEGEND}
          visible={step === 'overalt' || step === 'blindvinkel'}
        />
      </div>
    </>
  );
}
