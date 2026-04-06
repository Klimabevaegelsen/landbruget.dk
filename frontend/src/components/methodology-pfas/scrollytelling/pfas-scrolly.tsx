'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import { ScrollyStats } from '@/components/methodology/scrolly-stats';
import { PfasScrollyMap } from './pfas-scrolly-map';
import { PFAS_STEPS } from './pfas-scrolly-steps';
import type { PfasScrollyStepId } from './scrolly-constants';

const STAT_ROWS: Partial<
  Record<PfasScrollyStepId, { label: string; value: string }[]>
> = {
  intro: [
    { label: 'Oplande i DK', value: '5.826' },
    { label: 'Overvågede', value: '2.113 (36 %)' },
  ],
  field: [
    { label: 'Opland', value: 'I/S Skrydstrup' },
    { label: 'Afgrøde', value: 'Vinterhvede' },
    { label: 'Areal', value: '62,9 ha' },
    { label: 'Kommune', value: 'Haderslev' },
  ],
  detection: [
    { label: 'Boring', value: 'DGU 151. 2426' },
    { label: 'Dybde', value: '5 m' },
    { label: 'TFA', value: '4,88 µg/L' },
    { label: 'Grænseværdi', value: '0,1 µg/L' },
  ],
  everywhere: [
    { label: 'Overvågede oplande', value: '2.113' },
    { label: 'TFA-detektionsrate', value: '100 %' },
    { label: 'PFAS-analyser', value: '441.589' },
    { label: 'Median TFA', value: '0,075 µg/L' },
  ],
  blindspot: [
    { label: 'Uden PFAS-overvågning', value: '3.713 (64 %)' },
    { label: 'Overvåget', value: '2.113 (36 %)' },
    { label: 'TFA-forming stoffer', value: '35' },
    { label: 'TFA-overvågning fra', value: '2020' },
  ],
};

function renderVisual(stepId: string) {
  const step = stepId as PfasScrollyStepId;
  const stats = STAT_ROWS[step];
  return (
    <div className="relative h-full w-full">
      <PfasScrollyMap step={step} />
      <div className="absolute right-3 bottom-3 left-3 z-10">
        <ScrollyStats
          rows={stats ?? []}
          visible={!!stats}
          testId="pfas-scrolly-stats"
        />
      </div>
    </div>
  );
}

export function PfasScrolly() {
  return (
    <ScrollySection
      steps={PFAS_STEPS}
      stickyContent={renderVisual}
      className="-mx-6 my-10 lg:-mx-32"
    />
  );
}
