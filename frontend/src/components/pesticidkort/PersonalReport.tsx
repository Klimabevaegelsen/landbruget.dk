'use client';

import Link from 'next/link';
import { Share2, BookOpen } from 'lucide-react';
import { useMemo } from 'react';
import { PesticideProximityScore } from '@/components/pesticidkort/PesticideProximityScore';
import { SummaryStats } from '@/components/pesticidkort/SummaryStats';
import { AlertCallout } from '@/components/pesticidkort/AlertCallout';
import { FieldList } from '@/components/pesticidkort/FieldList';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';
import type { PesticideReport } from '@/components/pesticidkort/types';
import { toast } from 'sonner';

interface PersonalReportProps {
  report: PesticideReport;
  histogram: HistogramBin[];
  onFieldSelect?: (fieldUuid: string) => void;
  onOpenStory?: () => void;
}

function handleShare({ lat, lng, address, year }: PesticideReport) {
  const params = new URLSearchParams({
    lat: lat.toFixed(5),
    lng: lng.toFixed(5),
    addr: address,
    y: String(year),
  });
  navigator.clipboard.writeText(
    `${window.location.origin}/pesticidkort?${params}`
  );
  toast.success('Link kopieret', {
    description: 'Del linket, så andre kan tjekke deres egen adresse.',
  });
}

export function PersonalReport({
  report,
  histogram,
  onFieldSelect,
  onOpenStory,
}: PersonalReportProps) {
  const { fields } = report;
  const pfasFieldsInRadius = useMemo(
    () => fields.filter((f) => f.pfas_applications > 0),
    [fields]
  );
  const bnboFields = useMemo(
    () =>
      fields.filter((f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0),
    [fields]
  );

  return (
    <div
      data-testid="personal-report"
      aria-live="polite"
      className="animate-fade-slide-up space-y-6 px-5 py-4 motion-reduce:animate-none"
    >
      <div className="bg-card rounded-xl p-5">
        <PesticideProximityScore
          grade={report.grade.grade}
          label={report.grade.label}
          description={report.grade.description}
        />
        <div className="mt-4 flex items-center gap-3">
          <button
            onClick={() => handleShare(report)}
            data-testid="share-report-button"
            className="text-muted-foreground flex min-h-[44px] items-center gap-1.5 text-sm font-medium underline-offset-4 transition-transform duration-100 hover:underline active:scale-95"
          >
            <Share2 aria-hidden="true" className="h-3.5 w-3.5" />
            Del resultat
          </button>
          {onOpenStory && (
            <>
              <span className="text-border">·</span>
              <button
                onClick={onOpenStory}
                data-testid="open-story-button"
                className="text-primary flex min-h-[44px] items-center text-sm font-medium underline-offset-4 hover:underline"
              >
                Forstå dine resultater
              </button>
            </>
          )}
        </div>
      </div>

      <SummaryStats
        fieldsCount={report.fields_count}
        pfasFieldsCount={report.pfas_fields_count}
        nearestFieldM={report.nearest_field_m}
      />

      {(pfasFieldsInRadius.length > 0 || bnboFields.length > 0) && (
        <div className="space-y-3">
          {pfasFieldsInRadius.length > 0 && (
            <AlertCallout
              type="pfas"
              count={pfasFieldsInRadius.length}
              distanceM={report.radius_m}
            />
          )}
          {bnboFields.length > 0 && (
            <AlertCallout type="bnbo" count={bnboFields.length} />
          )}
        </div>
      )}

      <FieldList
        fields={fields}
        histogram={histogram}
        onFieldSelect={onFieldSelect}
      />

      <footer className="text-muted-foreground pt-4 pb-4">
        <p className="text-xs leading-relaxed">
          Data fra Miljøstyrelsen, Landbrugsstyrelsen, Geodatastyrelsen og 18+
          andre offentlige kilder. Sidst opdateret 2023.
        </p>
        <Link
          href="/pesticidanalyse/metode"
          data-testid="report-methodology-link"
          className="text-primary mt-2 inline-flex items-center gap-1.5 text-sm font-medium underline-offset-4 hover:underline"
        >
          <BookOpen className="h-3.5 w-3.5" />
          Læs om vores metode
        </Link>
      </footer>
    </div>
  );
}
