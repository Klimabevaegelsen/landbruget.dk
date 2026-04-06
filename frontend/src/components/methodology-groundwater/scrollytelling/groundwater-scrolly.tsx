'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import { ScrollyStats } from '@/components/methodology/scrolly-stats';
import { GroundwaterScrollyMap } from '@/components/methodology-groundwater/scrollytelling/groundwater-scrolly-map';
import { GwDegradationGraphic } from '@/components/methodology-groundwater/scrollytelling/gw-degradation-graphic';
import { GwTimelineOverlay } from '@/components/methodology-groundwater/scrollytelling/gw-timeline-overlay';
import { GwDepthCallout } from '@/components/methodology-groundwater/scrollytelling/gw-depth-callout';
import { GROUNDWATER_STEPS } from '@/components/methodology-groundwater/scrollytelling/groundwater-scrolly-steps';
import type { ScrollyStepId } from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

const STAT_ROWS: Partial<
  Record<ScrollyStepId, { label: string; value: string }[]>
> = {
  espe: [
    { label: 'Vandværk', value: 'Espe' },
    { label: 'Placering', value: 'Midtfyn' },
    { label: 'Opland', value: '~8 km²' },
  ],
  kilden: [
    { label: 'Marker', value: '6' },
    { label: 'Areal', value: '42,2 ha' },
    { label: 'Stof', value: 'Bentazon (Koc ~34)' },
  ],
  fundet: [
    { label: 'Boring', value: 'DGU 155. 1899' },
    { label: 'Dybde', value: '6 m' },
    { label: 'Koncentration', value: '490 µg/L' },
    { label: 'Grænseværdi', value: '0,1 µg/L' },
  ],
  mcpa: [
    { label: 'Vandværk', value: 'Vejen Forsyning' },
    { label: 'Stof', value: 'MCPA → metabolit' },
  ],
  fortidslevn: [
    { label: 'Boring', value: 'DGU 132. 1056' },
    { label: 'Dybde', value: '19,3 m' },
    { label: 'Metabolit', value: '3,2 µg/L' },
    { label: 'Persistent siden', value: '1997' },
  ],
  billede: [
    { label: 'Oplande analyseret', value: '3.154' },
    { label: 'Fund over grænse', value: '42 %' },
    { label: 'Stoffer sporet', value: 'Bentazon, MCPA m.fl.' },
  ],
};

const TIMELINE_STEPS = new Set<ScrollyStepId>([
  'rejsen',
  'fundet',
  'fortidslevn',
]);
const DEGRADATION_STEPS = new Set<ScrollyStepId>(['kilden', 'mcpa']);
const VADOSE_STEPS = new Set<ScrollyStepId>(['rejsen', 'fundet', 'mcpa']);

function getVariant(step: ScrollyStepId): 'bentazon' | 'mcpa' {
  return step === 'mcpa' || step === 'fortidslevn' ? 'mcpa' : 'bentazon';
}

function renderVisual(stepId: string) {
  const step = stepId as ScrollyStepId;
  const stats = STAT_ROWS[step];
  const showTimeline = TIMELINE_STEPS.has(step);
  const showDegradation = DEGRADATION_STEPS.has(step);
  const showVadose = VADOSE_STEPS.has(step);
  const variant = getVariant(step);

  return (
    <div className="relative h-full w-full">
      <GroundwaterScrollyMap step={step} showVadose={showVadose} />
      <GwDepthCallout visible={step === 'fundet'} />
      <div className="absolute top-3 right-3 z-10">
        <GwDegradationGraphic variant={variant} visible={showDegradation} />
      </div>
      <div className="absolute right-3 bottom-3 left-3 z-10">
        <GwTimelineOverlay variant={variant} visible={showTimeline} />
        <ScrollyStats
          rows={stats ?? []}
          visible={!!stats && !showTimeline}
          testId="groundwater-scrolly-stats"
        />
      </div>
    </div>
  );
}

export function GroundwaterScrolly() {
  return (
    <ScrollySection
      steps={GROUNDWATER_STEPS}
      stickyContent={renderVisual}
      className="-mx-6 my-10 lg:-mx-32"
    />
  );
}
