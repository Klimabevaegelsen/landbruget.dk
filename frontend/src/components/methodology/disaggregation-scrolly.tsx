'use client';

import { ScrollySection } from '@/components/methodology/scrolly-section';
import { MethodologyMap } from '@/components/methodology/methodology-map';
import { ScrollyRecordOverlay } from '@/components/methodology/scrolly-record-overlay';
import { DISAGG_STEPS } from '@/components/methodology/scrolly-disagg-steps';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

function renderVisual(stepId: string) {
  const step = stepId as DisaggStepId;
  const showRecord =
    step === 'record' ||
    step === 'overview' ||
    step === 'fields' ||
    step === 'match';
  return (
    <div className="relative h-full w-full">
      <MethodologyMap step={step} />
      <div className="absolute top-3 right-3 left-3 z-10">
        <ScrollyRecordOverlay step={step} visible={showRecord} />
      </div>
    </div>
  );
}

export function DisaggregationScrolly() {
  return (
    <ScrollySection
      steps={DISAGG_STEPS}
      stickyContent={renderVisual}
      className="-mx-6 my-10 lg:-mx-32"
    />
  );
}
