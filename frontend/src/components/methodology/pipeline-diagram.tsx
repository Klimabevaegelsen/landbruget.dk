import { cn } from '@/lib/utils';

interface PipelineStage {
  name: string;
  label: string;
  description: string;
  opacity: string;
}

const STAGES: PipelineStage[] = [
  {
    name: 'bronze',
    label: 'Bronze',
    description: 'R\u00e5 data fra offentlige kilder, u\u00e6ndret',
    opacity: 'opacity-50',
  },
  {
    name: 'silver',
    label: 'Silver',
    description: 'Renset, valideret og standardiseret',
    opacity: 'opacity-70',
  },
  {
    name: 'gold',
    label: 'Gold',
    description: 'Fordelt til markniveau og analyseklar',
    opacity: '',
  },
];

function Arrow() {
  return (
    <svg
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      className="text-primary/30 shrink-0"
      aria-hidden="true"
    >
      <path
        d="M5 12h14m0 0l-4-4m4 4l-4 4"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

interface PipelineDiagramProps {
  activeStage?: string;
  className?: string;
}

export function PipelineDiagram({
  activeStage,
  className,
}: PipelineDiagramProps) {
  return (
    <div className={cn('my-4', className)} data-testid="pipeline-diagram">
      <div className="flex items-center justify-center gap-3 sm:gap-5">
        {STAGES.map((stage, i) => (
          <div key={stage.name} className="flex items-center gap-3 sm:gap-5">
            <div
              className={cn(
                'text-center transition-opacity',
                activeStage && activeStage !== stage.name && 'opacity-40',
                !activeStage && stage.opacity
              )}
            >
              <div className="bg-primary/[0.08] text-primary mb-2 inline-block rounded-full px-3.5 py-1 text-[11px] font-semibold tracking-[0.15em] uppercase">
                {stage.label}
              </div>
              <div className="text-muted-foreground max-w-[140px] text-[12.5px] leading-snug">
                {stage.description}
              </div>
            </div>
            {i < STAGES.length - 1 && <Arrow />}
          </div>
        ))}
      </div>
    </div>
  );
}
