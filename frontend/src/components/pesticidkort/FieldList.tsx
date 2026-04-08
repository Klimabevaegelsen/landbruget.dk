'use client';

import { useMemo, useState, useEffect } from 'react';
import { ChevronDown } from 'lucide-react';
import { motion, useReducedMotion } from 'motion/react';
import { FieldCard } from '@/components/pesticidkort/FieldCard';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';

const INITIAL_VISIBLE = 5;

interface FieldListProps {
  fields: NearbyFieldSummary[];
  histogram: HistogramBin[];
  selectedFieldUuid?: string | null;
  clickedField?: NearbyFieldSummary | null;
  onFieldSelect?: (fieldUuid: string) => void;
}

export function FieldList({
  fields,
  histogram,
  selectedFieldUuid,
  clickedField,
  onFieldSelect,
}: FieldListProps) {
  const reducedMotion = useReducedMotion();
  const [showAll, setShowAll] = useState(false);

  const sortedFields = useMemo(
    () => [...fields].sort((a, b) => a.distance_m - b.distance_m),
    [fields]
  );

  const visibleFields = showAll
    ? sortedFields
    : sortedFields.slice(0, INITIAL_VISIBLE);
  const hiddenCount = sortedFields.length - INITIAL_VISIBLE;

  const isAdHocField =
    clickedField &&
    selectedFieldUuid === clickedField.field_uuid &&
    !sortedFields.some((f) => f.field_uuid === selectedFieldUuid);

  useEffect(() => {
    if (!selectedFieldUuid) return;
    const idx = sortedFields.findIndex(
      (f) => f.field_uuid === selectedFieldUuid
    );
    if (idx >= INITIAL_VISIBLE && !showAll) {
      setShowAll(true);
    }
    // Allow DOM to settle (expand animation, show-all list) before scrolling
    const timer = setTimeout(() => {
      document
        .getElementById(selectedFieldUuid)
        ?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 80);
    return () => clearTimeout(timer);
  }, [selectedFieldUuid, sortedFields, showAll]);

  return (
    <div>
      <h3 className="text-foreground mb-3 text-sm font-semibold">
        Marker i dit nærområde
      </h3>
      <div>
        {visibleFields.map((field, index) => (
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
              histogram={histogram}
              isSelected={field.field_uuid === selectedFieldUuid}
              onSelect={() => onFieldSelect?.(field.field_uuid)}
            />
          </motion.div>
        ))}
      </div>
      {hiddenCount > 0 && !showAll && (
        <button
          onClick={() => setShowAll(true)}
          data-testid="show-all-fields-button"
          className="text-primary group mt-2 flex w-full items-center justify-center gap-1 py-3 text-sm font-medium hover:underline"
        >
          <ChevronDown className="h-4 w-4 transition-transform duration-200 group-hover:translate-y-0.5" />
          Vis alle {sortedFields.length} marker
        </button>
      )}
      {isAdHocField && clickedField && (
        <div className="border-border/50 mt-4 border-t pt-4">
          <h3 className="text-foreground mb-1 text-sm font-semibold">
            Valgt mark
          </h3>
          <p className="text-muted-foreground mb-3 text-xs">
            Uden for dit nærområde — indgår ikke i din score
          </p>
          <FieldCard
            field={clickedField}
            histogram={histogram}
            isSelected
            onSelect={() => onFieldSelect?.(clickedField.field_uuid)}
          />
        </div>
      )}
    </div>
  );
}
