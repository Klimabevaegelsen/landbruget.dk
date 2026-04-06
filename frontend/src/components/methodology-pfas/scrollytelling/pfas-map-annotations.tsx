'use client';

import { MapLabel } from '@/components/methodology/map-label';
import { WellLabel } from '@/components/methodology/well-label';
import { MapLegend } from '@/components/methodology/map-legend';
import {
  SKRYDSTRUP_WELL,
  STATS,
  type PfasScrollyStepId,
} from './scrolly-constants';

const CATCHMENT_CENTER = { lng: 9.251, lat: 55.248 };
const FIELD_CENTER = { lng: 9.254, lat: 55.246 };
const SONDER_FELDING = { lng: 8.82, lat: 55.91 };

const LOCAL_LEGEND = [
  { color: '#8b5cf6', label: 'Opland (indvinding)' },
  { color: '#22c55e', label: 'Marker' },
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
        <MapLabel
          lng={CATCHMENT_CENTER.lng}
          lat={CATCHMENT_CENTER.lat + 0.012}
          text="I/S Skrydstrup Vandværk"
          subtext="Haderslev, Sønderjylland"
        />
      )}
      {step === 'byggeklodser' && (
        <MapLabel
          lng={FIELD_CENTER.lng}
          lat={FIELD_CENTER.lat}
          text="3 fluorprodukter"
          subtext="CF₃-gruppe"
        />
      )}
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
        <MapLabel
          lng={SONDER_FELDING.lng}
          lat={SONDER_FELDING.lat + 0.03}
          text="Søndre-Felding Vandværk"
          subtext="Ingen PFAS-målinger"
          variant="muted"
        />
      )}
      <div className="absolute bottom-3 left-3 z-10">
        <MapLegend
          items={LOCAL_LEGEND}
          visible={step === 'byggeklodser' || step === 'glasset'}
        />
        <MapLegend
          items={NATIONAL_LEGEND}
          visible={step === 'overalt' || step === 'blindvinkel'}
        />
      </div>
    </>
  );
}
