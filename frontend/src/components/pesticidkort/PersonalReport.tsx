'use client';

import Link from 'next/link';
import { Share2, BookOpen } from 'lucide-react';
import { PesticideProximityScore } from '@/components/pesticidkort/PesticideProximityScore';
import { SummaryStats } from '@/components/pesticidkort/SummaryStats';
import { AddressExposure } from '@/components/pesticidkort/AddressExposure';
import { AlertCallout } from '@/components/pesticidkort/AlertCallout';
import { FieldList } from '@/components/pesticidkort/FieldList';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';
import type {
  PesticideReport,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';
import { handleShare } from '@/components/pesticidkort/share-report';

interface PersonalReportProps {
  report: PesticideReport;
  histogram: HistogramBin[];
  selectedFieldUuid?: string | null;
  clickedField?: NearbyFieldSummary | null;
  onFieldSelect?: (fieldUuid: string) => void;
  onOpenStory?: () => void;
}

export function PersonalReport({
  report,
  histogram,
  selectedFieldUuid,
  clickedField,
  onFieldSelect,
  onOpenStory,
}: PersonalReportProps) {
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

      <AddressExposure
        exposure100m={report.exposure_100m}
        exposure1000m={report.exposure_1000m}
      />

      <SummaryStats
        fieldsCount={report.fields_count}
        avgBurden={report.avg_burden}
        nearestFieldM={report.nearest_field_m}
      />

      <FieldList
        fields={report.fields}
        histogram={histogram}
        selectedFieldUuid={selectedFieldUuid}
        clickedField={clickedField}
        onFieldSelect={onFieldSelect}
      />

      {(report.pfas_fields_count > 0 || report.has_bnbo_overlap) && (
        <div className="space-y-3">
          {report.has_bnbo_overlap && (
            <AlertCallout
              type="bnbo"
              count={
                report.fields.filter(
                  (f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0
                ).length
              }
            />
          )}
          {report.pfas_fields_count > 0 && (
            <AlertCallout
              type="pfas"
              count={report.pfas_fields_count}
              distanceM={report.radius_m}
            />
          )}
        </div>
      )}

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
