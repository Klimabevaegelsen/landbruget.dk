'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import { ScrollyStats } from '@/components/methodology/scrolly-stats';
import { GroundwaterScrollyMap } from './groundwater-scrolly-map';
import { GROUNDWATER_STEPS } from './groundwater-scrolly-steps';
import type { ScrollyStepId } from './scrolly-constants';

const STAT_ROWS: Partial<
  Record<ScrollyStepId, { label: string; value: string }[]>
> = {
  detection: [
    { label: 'Boring', value: 'DGU 155. 1899' },
    { label: 'Dybde', value: '6 m' },
    { label: 'Koncentration', value: '490 µg/L' },
    { label: 'Grænseværdi', value: '0,1 µg/L' },
  ],
  doseresponse: [
    { label: 'Oplande', value: '3.154' },
    { label: 'Q4/Q1', value: '4,4×' },
    { label: 'Korrelation', value: 'r = 0,213' },
    { label: 'p-værdi', value: '0,003' },
  ],
  metabolite: [
    { label: 'Boring', value: 'DGU 132. 1056' },
    { label: 'Dybde', value: '19,3 m' },
    { label: 'Metabolit', value: '3,2 µg/L' },
    { label: 'Persistent siden', value: '1997' },
  ],
};

function renderVisual(stepId: string) {
  const step = stepId as ScrollyStepId;
  const stats = STAT_ROWS[step];
  return (
    <div className="relative h-full w-full">
      <GroundwaterScrollyMap step={step} />
      <div className="absolute right-3 bottom-3 left-3 z-10">
        <ScrollyStats
          rows={stats ?? []}
          visible={!!stats}
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
