import { cn } from '@/lib/utils';

interface PipelineStage {
  name: string;
  label: string;
  description: string;
  accent: string;
}

const STAGES: PipelineStage[] = [
  {
    name: 'bronze',
    label: 'Bronze',
    description: 'Rå data fra offentlige kilder, uændret',
    accent: 'bg-primary/10 text-primary/70',
  },
  {
    name: 'silver',
    label: 'Silver',
    description: 'Renset, valideret og standardiseret',
    accent: 'bg-primary/15 text-primary/80',
  },
  {
    name: 'gold',
    label: 'Gold',
    description: 'Fordelt til markniveau og analyseklar',
    accent: 'bg-primary/20 text-primary',
  },
];

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
      <div className="flex items-start justify-center gap-4 sm:gap-6">
        {STAGES.map((stage, i) => (
          <div key={stage.name} className="flex items-center gap-4 sm:gap-6">
            <div
              className={cn(
                'text-center transition-opacity',
                activeStage && activeStage !== stage.name && 'opacity-50'
              )}
            >
              <div
                className={cn(
                  'mb-2 inline-block rounded-full px-3 py-1 text-xs font-semibold tracking-widest uppercase',
                  stage.accent
                )}
              >
                {stage.label}
              </div>
              <div className="text-muted-foreground max-w-[150px] text-[13px] leading-snug">
                {stage.description}
              </div>
            </div>
            {i < STAGES.length - 1 && (
              <span className="text-primary/40 mt-1">→</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
