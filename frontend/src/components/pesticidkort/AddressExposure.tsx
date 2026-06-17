'use client';

import type { ExposureSummary } from '@/components/pesticidkort/types';
import { EstimatedBadge } from '@/components/pesticidkort/EstimatedBadge';
import { formatRadiusLabel } from '@/components/pesticidkort/exposure-utils';

interface AddressExposureProps {
  exposure100m: ExposureSummary;
  exposure1000m: ExposureSummary;
}

function ExposureRow({ summary }: { summary: ExposureSummary }) {
  return (
    <div>
      <p className="text-foreground mb-1.5 text-xs font-semibold">
        Inden for {formatRadiusLabel(summary.radius_m)}
      </p>
      <div className="grid grid-cols-3 gap-3 text-center">
        <div>
          <p className="text-foreground text-lg font-semibold tabular-nums">
            {summary.fields_sprayed}
          </p>
          <p className="text-muted-foreground text-[10px] leading-tight">
            sprøjtede marker
          </p>
        </div>
        <div>
          <p className="text-foreground text-lg font-semibold tabular-nums">
            {summary.total_applications}
          </p>
          <p className="text-muted-foreground text-[10px] leading-tight">
            pesticider
          </p>
        </div>
        <div>
          <p className="text-foreground text-lg font-semibold tabular-nums">
            {summary.pfas_fields_count}
          </p>
          <p className="text-muted-foreground text-[10px] leading-tight">
            marker med PFAS
          </p>
        </div>
      </div>
    </div>
  );
}

export function AddressExposure({
  exposure100m,
  exposure1000m,
}: AddressExposureProps) {
  return (
    <div
      data-testid="address-exposure"
      className="bg-card rounded-xl px-5 py-4"
    >
      <div className="mb-3 flex items-center justify-between gap-2">
        <p className="text-muted-foreground text-xs font-semibold tracking-widest uppercase">
          Din adresses eksponering
        </p>
        <EstimatedBadge variant="estimeret" />
      </div>
      <div className="space-y-4">
        <ExposureRow summary={exposure100m} />
        <div className="border-border border-t" />
        <ExposureRow summary={exposure1000m} />
      </div>
    </div>
  );
}
