'use client';

import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import { MapLabel } from '@/components/methodology/map-label';
import { MapLegend } from '@/components/methodology/map-legend';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

const F = EXAMPLE.fields;

const BURDEN_LEGEND = [
  { color: '#6abf69', label: 'Lav belastning' },
  { color: '#d4c54a', label: 'Moderat' },
  { color: '#d89135', label: 'Høj' },
  { color: '#c4512c', label: 'Meget høj' },
];

interface DisaggMapAnnotationsProps {
  step: DisaggStepId;
}

export function DisaggMapAnnotations({ step }: DisaggMapAnnotationsProps) {
  const showFieldLabels =
    step === 'fields' ||
    step === 'match' ||
    step === 'result' ||
    step === 'summary';
  const showDoseLabels = step === 'result' || step === 'summary';
  const showLegend = step === 'result' || step === 'scale';

  return (
    <>
      {step === 'context' && (
        <MapLabel lng={10.5} lat={56.0} text="~350.000 indberetninger/år" />
      )}
      {step === 'location' && (
        <MapLabel lng={9.5} lat={55.3} text="Haderslev" subtext="Sydjylland" />
      )}
      {step === 'overview' && (
        <MapLabel
          lng={EXAMPLE.center[0]}
          lat={EXAMPLE.center[1] + 0.01}
          text={`CVR ${EXAMPLE.cvr}`}
          subtext={EXAMPLE.municipality}
        />
      )}
      {showFieldLabels &&
        F.map((f) => (
          <MapLabel
            key={f.uuid}
            lng={f.centroid[0]}
            lat={f.centroid[1]}
            text={showDoseLabels ? `${f.dose.toFixed(1)} L` : `${f.areaHa} ha`}
          />
        ))}
      {step === 'match' && (
        <MapLabel
          lng={EXAMPLE.center[0]}
          lat={EXAMPLE.center[1] + 0.015}
          text="✓ 0% afvigelse"
          variant="muted"
        />
      )}
      {step === 'scale' && (
        <MapLabel
          lng={9.5}
          lat={55.5}
          text="~600.000 marker"
          subtext="Hele Danmark"
        />
      )}
      <div className="absolute bottom-3 left-3 z-10">
        <MapLegend items={BURDEN_LEGEND} visible={showLegend} />
      </div>
    </>
  );
}
