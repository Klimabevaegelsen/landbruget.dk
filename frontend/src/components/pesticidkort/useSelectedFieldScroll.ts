'use client';

import { useEffect } from 'react';

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
    container.scrollBy({ top: elementRect.top - visibleTop, behavior });
    return;
  }

  if (elementRect.bottom > visibleBottom) {
    container.scrollBy({ top: elementRect.bottom - visibleBottom, behavior });
  }
}

interface UseSelectedFieldScrollOptions {
  selectedFieldUuid?: string | null;
  sortedFieldIds: string[];
  showAll: boolean;
  setShowAll: (value: boolean) => void;
  cardRefs: React.MutableRefObject<Map<string, HTMLDivElement | null>>;
  initialVisible: number;
}

export function useSelectedFieldScroll({
  selectedFieldUuid,
  sortedFieldIds,
  showAll,
  setShowAll,
  cardRefs,
  initialVisible,
}: UseSelectedFieldScrollOptions) {
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

    const idx = sortedFieldIds.indexOf(selectedFieldUuid);
    if (idx >= initialVisible && !showAll) {
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
  }, [
    cardRefs,
    initialVisible,
    selectedFieldUuid,
    setShowAll,
    showAll,
    sortedFieldIds,
  ]);
}
