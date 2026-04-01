'use client';

import { useEffect, useRef, useState } from 'react';

export function useCountUp(value: number, durationMs = 900) {
  const [display, setDisplay] = useState(0);
  const prevValueRef = useRef(0);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    if (mediaQuery.matches) {
      prevValueRef.current = value;
      setDisplay(value);
      return;
    }

    const start = prevValueRef.current;
    const end = value;
    const startTs = performance.now();
    let raf = 0;

    const tick = (ts: number) => {
      const progress = Math.min((ts - startTs) / durationMs, 1);
      const eased = 1 - Math.pow(1 - progress, 4);
      setDisplay(start + (end - start) * eased);
      if (progress < 1) {
        raf = requestAnimationFrame(tick);
      } else {
        prevValueRef.current = end;
      }
    };

    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [value, durationMs]);

  return display;
}
