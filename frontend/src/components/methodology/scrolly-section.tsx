'use client';

import { useState, type ReactNode } from 'react';
import { Scrollama, Step } from 'react-scrollama';
import { cn } from '@/lib/utils';

interface ScrollyStep {
  id: string;
  content: ReactNode;
}

interface ScrollySectionProps {
  steps: ScrollyStep[];
  stickyContent: (activeStepId: string) => ReactNode;
  className?: string;
  offset?: number;
}

export function ScrollySection({
  steps,
  stickyContent,
  className,
  offset = 0.5,
}: ScrollySectionProps) {
  const [activeStepId, setActiveStepId] = useState(steps[0]?.id ?? '');

  return (
    <section
      className={cn('relative', className)}
      data-testid="scrolly-section"
    >
      <div className="flex flex-col lg:flex-row lg:gap-8">
        {/* Sticky visualization pane */}
        <div className="mb-8 lg:sticky lg:top-24 lg:mb-0 lg:h-[calc(100vh-8rem)] lg:w-1/2">
          <div
            className="bg-card h-full overflow-hidden rounded-xl border shadow-sm"
            data-testid="scrolly-sticky-figure"
          >
            {stickyContent(activeStepId)}
          </div>
        </div>

        {/* Scrolling text steps */}
        <div className="lg:w-1/2">
          <Scrollama
            offset={offset}
            onStepEnter={({ data }) => setActiveStepId(data as string)}
          >
            {steps.map((step) => (
              <Step key={step.id} data={step.id}>
                <div
                  className={cn(
                    'mb-48 min-h-[40vh] transition-opacity duration-500',
                    activeStepId === step.id ? 'opacity-100' : 'opacity-30'
                  )}
                  data-testid={`scrolly-step-${step.id}`}
                >
                  {step.content}
                </div>
              </Step>
            ))}
          </Scrollama>
        </div>
      </div>
    </section>
  );
}
