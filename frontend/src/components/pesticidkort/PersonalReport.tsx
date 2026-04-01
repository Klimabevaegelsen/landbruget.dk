'use client';

import { MapPin, Share2 } from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import { PesticideProximityScore } from '@/components/pesticidkort/PesticideProximityScore';
import { SummaryStats } from '@/components/pesticidkort/SummaryStats';
import { AlertCallout } from '@/components/pesticidkort/AlertCallout';
import { FieldCard } from '@/components/pesticidkort/FieldCard';
import type { PesticideReport } from '@/components/pesticidkort/types';
import { toast } from 'sonner';

interface PersonalReportProps {
  report: PesticideReport;
  onFieldSelect?: (fieldUuid: string) => void;
  onOpenStory?: () => void;
}

function handleShare(report: PesticideReport) {
  const params = new URLSearchParams({
    lat: report.lat.toFixed(5),
    lng: report.lng.toFixed(5),
    addr: report.address,
    y: String(report.year),
  });
  const url = `${window.location.origin}/pesticidkort?${params.toString()}`;
  navigator.clipboard.writeText(url);
  toast.success('Link kopieret', {
    description: 'Del linket, så andre kan tjekke deres egen adresse.',
  });
}

export function PersonalReport({
  report,
  onFieldSelect,
  onOpenStory,
}: PersonalReportProps) {
  const reducedMotion = useReducedMotion();
  const pfasFieldsInRadius = report.fields.filter(
    (f) => f.pfas_applications > 0
  );
  const bnboFields = report.fields.filter(
    (f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0
  );

  return (
    <div
      data-testid="personal-report"
      aria-live="polite"
      className="animate-fade-slide-up px-6 py-4 motion-reduce:animate-none"
    >
      <div className="text-muted-foreground flex items-center gap-2 text-sm">
        <MapPin aria-hidden="true" className="h-4 w-4 shrink-0" />
        <span className="truncate">{report.address}</span>
      </div>

      <PesticideProximityScore
        grade={report.grade.grade}
        label={report.grade.label}
        description={report.grade.description}
      />

      {onOpenStory && (
        <button
          onClick={onOpenStory}
          data-testid="open-story-button"
          className="text-primary mt-4 text-sm font-medium underline-offset-4 hover:underline"
        >
          Forstå dine resultater →
        </button>
      )}

      <div className="border-border border-t pt-6">
        <SummaryStats
          fieldsCount={report.fields_count}
          pfasFieldsCount={report.pfas_fields_count}
          nearestFieldM={report.nearest_field_m}
        />
      </div>

      {(pfasFieldsInRadius.length > 0 || bnboFields.length > 0) && (
        <div className="mt-8 space-y-3">
          {pfasFieldsInRadius.length > 0 && (
            <div className="animate-fade-slide-up [animation-delay:120ms]">
              <AlertCallout
                type="pfas"
                count={pfasFieldsInRadius.length}
                distanceM={report.radius_m}
              />
            </div>
          )}
          {bnboFields.length > 0 && (
            <div className="animate-fade-slide-up [animation-delay:180ms]">
              <AlertCallout type="bnbo" count={bnboFields.length} />
            </div>
          )}
        </div>
      )}

      <div className="mt-8">
        <h3 className="text-foreground mb-1 text-base font-semibold">
          Marker i dit nærområde
        </h3>
        <p className="text-muted-foreground mb-4 text-xs">
          {report.fields_count} marker inden for {report.radius_m / 1000} km,
          sorteret efter afstand
        </p>
        <div>
          {[...report.fields]
            .sort((a, b) => a.distance_m - b.distance_m)
            .map((field, index) => (
              <motion.div
                key={field.field_uuid}
                initial={reducedMotion ? false : { opacity: 0, y: 8 }}
                animate={reducedMotion ? undefined : { opacity: 1, y: 0 }}
                transition={{
                  duration: 0.32,
                  delay: reducedMotion ? 0 : Math.min(index * 0.04, 0.28),
                  ease: [0.25, 1, 0.5, 1],
                }}
              >
                <FieldCard
                  field={field}
                  onSelect={() => onFieldSelect?.(field.field_uuid)}
                />
              </motion.div>
            ))}
        </div>
      </div>

      <div className="sticky bottom-4 mt-8 flex justify-center pb-4">
        <button
          onClick={() => handleShare(report)}
          data-testid="share-report-button"
          className="bg-primary text-primary-foreground flex items-center gap-2 rounded-full px-5 py-2.5 text-sm font-medium shadow-lg transition-transform active:scale-95"
        >
          <Share2 aria-hidden="true" className="h-4 w-4" />
          Del din pesticidrapport
        </button>
      </div>
    </div>
  );
}
