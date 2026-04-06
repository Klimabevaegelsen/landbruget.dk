'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import { ScrollyStats } from '@/components/methodology/scrolly-stats';
import { PfasScrollyMap } from './pfas-scrolly-map';
import { PfasTimelineOverlay } from './pfas-timeline-overlay';
import { PfasCounterOverlay } from './pfas-counter-overlay';
import { PfasHeadlineOverlay } from './pfas-headline-overlay';
import { PfasMoleculeAnimation } from './pfas-molecule-animation';
import { PFAS_STEPS } from './pfas-scrolly-steps';
import type { PfasScrollyStepId } from './scrolly-constants';

const STAT_ROWS: Partial<
  Record<PfasScrollyStepId, { label: string; value: string }[]>
> = {
  skjult: [
    { label: 'Oplande i DK', value: '5.826' },
    { label: 'Overvågede', value: '2.113 (36 %)' },
  ],
  byggeklodser: [
    { label: 'Opland', value: 'I/S Skrydstrup' },
    { label: 'Afgrøde', value: 'Vinterhvede' },
    { label: 'Areal', value: '62,9 ha' },
  ],
  glasset: [
    { label: 'Boring', value: 'DGU 151. 2426' },
    { label: 'Dybde', value: '5 m' },
    { label: 'TFA', value: '4,88 µg/L' },
    { label: 'Grænseværdi', value: '0,1 µg/L' },
  ],
  overalt: [
    { label: 'Overvågede oplande', value: '2.113' },
    { label: 'TFA-detektionsrate', value: '100 %' },
    { label: 'PFAS-analyser', value: '441.589' },
  ],
  blindvinkel: [
    { label: 'Uden PFAS-overvågning', value: '3.713 (64 %)' },
    { label: 'Overvåget', value: '2.113 (36 %)' },
    { label: 'TFA-forming stoffer', value: '35' },
  ],
};

const TIMELINE_STEPS = new Set<PfasScrollyStepId>(['glasset']);

function renderVisual(stepId: string) {
  const step = stepId as PfasScrollyStepId;
  const stats = STAT_ROWS[step];
  const showTimeline = TIMELINE_STEPS.has(step);
  return (
    <div className="relative h-full w-full">
      <PfasScrollyMap step={step} />
      <PfasCounterOverlay visible={step === 'overalt'} />
      <PfasHeadlineOverlay visible={step === 'blindvinkel'} />
      <PfasMoleculeAnimation step={step} />
      <div className="absolute right-3 bottom-3 left-3 z-10">
        <PfasTimelineOverlay visible={showTimeline} />
        <ScrollyStats
          rows={stats ?? []}
          visible={!!stats && !showTimeline}
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
