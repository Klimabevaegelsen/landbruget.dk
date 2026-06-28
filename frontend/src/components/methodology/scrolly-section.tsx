'use client';

import { useState, type ReactNode } from 'react';
import { Scrollama, Step } from 'react-scrollama';
import { motion } from 'motion/react';
import { cn } from '@/lib/utils';
import { useMediaQuery } from '@/hooks/use-mobile-detection';

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
  const isMobile = useMediaQuery('(max-width: 1023px)');

  return (
    <section
      className={cn('relative', className)}
      data-testid="scrolly-section"
    >
      <div className="flex flex-col lg:flex-row lg:gap-8">
        {/* Sticky visualization pane */}
        <div className="sticky top-4 z-10 mb-8 h-[50dvh] lg:top-24 lg:mb-0 lg:h-[calc(100dvh-8rem)] lg:w-1/2">
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
            offset={isMobile ? 0.65 : offset}
            onStepEnter={({ data }) => setActiveStepId(data as string)}
          >
            {steps.map((step, i) => {
              const isActive = activeStepId === step.id;
              return (
                <Step key={step.id} data={step.id}>
                  <div
                    className="relative mb-48 min-h-[40dvh]"
                    data-testid={`scrolly-step-${step.id}`}
                  >
                    {step.id === 'regneark' && (
                      <div
                        aria-hidden="true"
                        className="pointer-events-none absolute inset-0"
                        data-testid="scrolly-step-record"
                      />
                    )}
                    <motion.div
                      animate={{
                        opacity: isActive ? 1 : 0.15,
                        y: isActive ? 0 : 6,
                      }}
                      transition={{
                        type: 'spring',
                        stiffness: 180,
                        damping: 28,
                      }}
                    >
                      <div className="text-muted-foreground mb-2 text-[11px] font-medium tracking-widest uppercase">
                        Trin {i + 1} / {steps.length}
                      </div>
                      {step.content}
                    </motion.div>
                  </div>
                </Step>
              );
            })}
          </Scrollama>
        </div>
      </div>
    </section>
  );
}
