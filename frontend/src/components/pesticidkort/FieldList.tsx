'use client';

import { useMemo, useState, useEffect, useRef } from 'react';
import { ChevronDown } from 'lucide-react';
import { motion, useReducedMotion } from 'motion/react';
import { FieldCard } from '@/components/pesticidkort/FieldCard';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';

const INITIAL_VISIBLE = 5;
const CARD_SCROLL_PADDING = 12;
const EXPANSION_OBSERVER_WINDOW_MS = 400;

function findScrollContainer(element: HTMLElement): HTMLElement | null {
  let current = element.parentElement;

  while (current) {
    const style = window.getComputedStyle(current);
    const overflowY = style.overflowY;
    if (
      (overflowY === 'auto' || overflowY === 'scroll') &&
      current.scrollHeight > current.clientHeight
    ) {
      return current;
    }
    current = current.parentElement;
  }

  return document.scrollingElement instanceof HTMLElement
    ? document.scrollingElement
    : null;
}

function ensureCardFullyVisible(
  element: HTMLElement,
  behavior: ScrollBehavior = 'smooth'
) {
  const container = findScrollContainer(element);

  if (!container) {
    element.scrollIntoView({ behavior, block: 'nearest' });
    return;
  }

  const containerRect =
    container === document.scrollingElement
      ? { top: 0, bottom: window.innerHeight }
      : container.getBoundingClientRect();
  const elementRect = element.getBoundingClientRect();
  const visibleTop = containerRect.top + CARD_SCROLL_PADDING;
  const visibleBottom = containerRect.bottom - CARD_SCROLL_PADDING;

  if (elementRect.top < visibleTop) {
    container.scrollBy({
      top: elementRect.top - visibleTop,
      behavior,
    });
    return;
  }

  if (elementRect.bottom > visibleBottom) {
    container.scrollBy({
      top: elementRect.bottom - visibleBottom,
      behavior,
    });
  }
}

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

    let animationFrame1 = 0;
    let animationFrame2 = 0;
    let resizeObserver: ResizeObserver | null = null;
    let resizeObserverTimeout = 0;

    const scrollSelectedCard = (behavior: ScrollBehavior = 'smooth') => {
      const selectedCard = cardRefs.current.get(selectedFieldUuid);
      if (!selectedCard) return;

      ensureCardFullyVisible(selectedCard, behavior);

      if (typeof ResizeObserver === 'undefined') return;

      resizeObserver?.disconnect();
      resizeObserver = new ResizeObserver(() => {
        ensureCardFullyVisible(selectedCard, 'auto');
      });
      resizeObserver.observe(selectedCard);

      window.clearTimeout(resizeObserverTimeout);
      resizeObserverTimeout = window.setTimeout(() => {
        resizeObserver?.disconnect();
        resizeObserver = null;
      }, EXPANSION_OBSERVER_WINDOW_MS);
    };

    const scheduleScroll = () => {
      animationFrame1 = requestAnimationFrame(() => {
        animationFrame2 = requestAnimationFrame(() => {
          scrollSelectedCard();
        });
      });
    };

    const idx = sortedFields.findIndex(
      (f) => f.field_uuid === selectedFieldUuid
    );
    if (idx >= INITIAL_VISIBLE && !showAll) {
      setShowAll(true);
      scheduleScroll();
    } else {
      scheduleScroll();
    }

    return () => {
      cancelAnimationFrame(animationFrame1);
      cancelAnimationFrame(animationFrame2);
      resizeObserver?.disconnect();
      window.clearTimeout(resizeObserverTimeout);
    };
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
