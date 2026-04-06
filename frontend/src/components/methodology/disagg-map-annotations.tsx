'use client';

import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import { MapLabel } from '@/components/methodology/map-label';
import { MapLegend } from '@/components/methodology/map-legend';
import { DisaggPulsingLabel } from '@/components/methodology/disagg-pulsing-label';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

const F = EXAMPLE.fields;

const BURDEN_LEGEND = [
  { color: '#6abf69', label: 'Lav belastning' },
  { color: '#d4c54a', label: 'Moderat' },
  { color: '#d89135', label: 'H\u00f8j' },
  { color: '#c4512c', label: 'Meget h\u00f8j' },
];

interface DisaggMapAnnotationsProps {
  step: DisaggStepId;
}

export function DisaggMapAnnotations({ step }: DisaggMapAnnotationsProps) {
  const showAreaLabels = step === 'fields' || step === 'match';
  const showDoseLabels = step === 'virkelighed';
  const showLegend = step === 'virkelighed' || step === 'scale';

  return (
    <>
      {step === 'context' && (
        <MapLabel
          lng={10.5}
          lat={56.0}
          text="~350.000 indberetninger/\u00e5r"
        />
      )}
      {step === 'location' && (
        <MapLabel lng={9.5} lat={55.26} text="Haderslev" subtext="Sydjylland" />
      )}
      {showAreaLabels &&
        F.map((f) => (
          <MapLabel
            key={f.uuid}
            lng={f.centroid[0]}
            lat={f.centroid[1]}
            text={`${f.areaHa} ha`}
          />
        ))}
      {step === 'match' && (
        <DisaggPulsingLabel lng={EXAMPLE.center[0]} lat={EXAMPLE.center[1]} />
      )}
      {showDoseLabels &&
        F.map((f) => (
          <MapLabel
            key={f.uuid}
            lng={f.centroid[0]}
            lat={f.centroid[1]}
            text={`${f.dose.toFixed(1)} L`}
            variant="alert"
          />
        ))}
      {step === 'scale' && (
        <MapLabel
          lng={10.5}
          lat={56.0}
          text="~350.000 indberetninger"
          subtext="Hele Danmark"
        />
      )}
      <div className="absolute bottom-3 left-3 z-10">
        <MapLegend items={BURDEN_LEGEND} visible={showLegend} />
      </div>
    </>
  );
}
