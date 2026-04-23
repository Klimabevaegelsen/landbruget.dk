'use client';

import { useMemo, useRef, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { motion, useReducedMotion } from 'motion/react';
import { FieldCard } from '@/components/pesticidkort/FieldCard';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import { useSelectedFieldScroll } from '@/components/pesticidkort/useSelectedFieldScroll';

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
  const cardRefs = useRef<Map<string, HTMLDivElement | null>>(new Map());

  const sortedFields = useMemo(
    () => [...fields].sort((a, b) => a.distance_m - b.distance_m),
    [fields]
  );
  const sortedFieldIds = useMemo(
    () => sortedFields.map((field) => field.field_uuid),
    [sortedFields]
  );

  const visibleFields = showAll
    ? sortedFields
    : sortedFields.slice(0, INITIAL_VISIBLE);
  const hiddenCount = sortedFields.length - INITIAL_VISIBLE;

  const isAdHocField =
    clickedField &&
    selectedFieldUuid === clickedField.field_uuid &&
    !sortedFields.some((f) => f.field_uuid === selectedFieldUuid);

  useSelectedFieldScroll({
    selectedFieldUuid,
    sortedFieldIds,
    showAll,
    setShowAll,
    cardRefs,
    initialVisible: INITIAL_VISIBLE,
  });

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
              ref={(el) => {
                cardRefs.current.set(field.field_uuid, el);
              }}
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
            ref={(el) => {
              cardRefs.current.set(clickedField.field_uuid, el);
            }}
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
