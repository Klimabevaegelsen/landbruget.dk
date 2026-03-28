import { cn } from '@/lib/utils';

interface PipelineStage {
  name: string;
  label: string;
  description: string;
}

const STAGES: PipelineStage[] = [
  {
    name: 'bronze',
    label: 'Bronze',
    description: 'Rå data fra offentlige kilder, uændret',
  },
  {
    name: 'silver',
    label: 'Silver',
    description: 'Renset, valideret og standardiseret',
  },
  {
    name: 'gold',
    label: 'Gold',
    description: 'Fordelt til markniveau og analyseklar',
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
              <div className="text-muted-foreground mb-1 text-xs font-semibold tracking-widest uppercase">
                {stage.label}
              </div>
              <div className="text-muted-foreground max-w-[150px] text-[13px] leading-snug">
                {stage.description}
              </div>
            </div>
            {i < STAGES.length - 1 && (
              <span className="text-border mt-1">&rarr;</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
