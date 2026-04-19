'use client';

import { cn } from '@/lib/utils';
import { AlertTriangle } from 'lucide-react';

type AlertType = 'bnbo' | 'violation';

interface AlertCalloutProps {
  type: AlertType;
  count: number;
}

const ALERT_CONFIG: Record<
  AlertType,
  {
    border: string;
    bg: string;
    icon: string;
    title: (count: number) => string;
    body: string;
  }
> = {
  bnbo: {
    border: 'border-l-info',
    bg: 'bg-info/5',
    icon: 'text-info',
    title: (n) => `${n} marker ligger tæt på drikkevandsboringer`,
    body: 'Sprøjtning nær drikkevandsboringer kan forurene grundvandet.',
  },
  violation: {
    border: 'border-l-destructive',
    bg: 'bg-destructive/5',
    icon: 'text-destructive',
    title: (n) => `${n} mulige overtrædelser fundet i dit nærområde`,
    body: 'Pesticider anvendt efter tilbagekaldelse eller i for høje doser.',
  },
};

export function AlertCallout({ type, count }: AlertCalloutProps) {
  if (count === 0) return null;

  const config = ALERT_CONFIG[type];

  return (
    <div
      data-testid={`alert-${type}`}
      className={cn('rounded-lg border-l-[3px] p-4', config.border, config.bg)}
    >
      <div className="flex gap-3">
        <AlertTriangle className={cn('mt-0.5 h-4 w-4 shrink-0', config.icon)} />
        <div>
          <p className="text-foreground text-sm font-medium">
            {config.title(count)}
          </p>
          <p className="text-muted-foreground mt-1 text-sm">{config.body}</p>
        </div>
      </div>
    </div>
  );
}
