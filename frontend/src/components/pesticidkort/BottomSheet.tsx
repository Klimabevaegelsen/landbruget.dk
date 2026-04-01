'use client';

import { useRef, useCallback, useEffect, useState } from 'react';
import { cn } from '@/lib/utils';

type SheetState = 'peek' | 'half' | 'full';

interface BottomSheetProps {
  state: SheetState;
  onStateChange: (state: SheetState) => void;
  children: React.ReactNode;
}

const PEEK_HEIGHT = 140;
const HALF_FRACTION = 0.5;

export function BottomSheet({
  state,
  onStateChange,
  children,
}: BottomSheetProps) {
  const sheetRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const dragStartRef = useRef<{ y: number; height: number } | null>(null);
  const [currentHeight, setCurrentHeight] = useState(PEEK_HEIGHT);

  const getTargetHeight = useCallback((s: SheetState) => {
    const vh = window.innerHeight;
    if (s === 'peek') return PEEK_HEIGHT;
    if (s === 'half') return vh * HALF_FRACTION;
    return vh - 48;
  }, []);

  useEffect(() => {
    setCurrentHeight(getTargetHeight(state));
  }, [state, getTargetHeight]);

  useEffect(() => {
    if (!sheetRef.current || !contentRef.current) return;
    sheetRef.current.style.setProperty('--sheet-height', `${currentHeight}px`);
    contentRef.current.style.setProperty(
      '--sheet-content-height',
      `${Math.max(0, currentHeight - 40)}px`
    );
  }, [currentHeight]);

  const handlePointerDown = useCallback(
    (e: React.PointerEvent) => {
      dragStartRef.current = { y: e.clientY, height: currentHeight };
      (e.target as HTMLElement).setPointerCapture(e.pointerId);
    },
    [currentHeight]
  );

  const handlePointerMove = useCallback((e: React.PointerEvent) => {
    if (!dragStartRef.current) return;
    const dy = dragStartRef.current.y - e.clientY;
    const newH = Math.max(PEEK_HEIGHT, dragStartRef.current.height + dy);
    setCurrentHeight(Math.min(newH, window.innerHeight - 48));
  }, []);

  const handlePointerUp = useCallback(() => {
    if (!dragStartRef.current) return;
    const vh = window.innerHeight;
    const fraction = currentHeight / vh;
    dragStartRef.current = null;

    if (fraction < 0.25) onStateChange('peek');
    else if (fraction < 0.7) onStateChange('half');
    else onStateChange('full');
  }, [currentHeight, onStateChange]);

  return (
    <div
      ref={sheetRef}
      role="region"
      aria-label="Pesticidrapport"
      className={cn(
        'bg-background fixed inset-x-0 bottom-0 z-30 rounded-t-2xl shadow-2xl',
        'h-[var(--sheet-height,140px)]',
        'transition-[height] duration-300 ease-[cubic-bezier(0.25,1,0.5,1)]',
        dragStartRef.current && 'transition-none'
      )}
    >
      <div
        role="separator"
        aria-orientation="horizontal"
        aria-label="Træk for at ændre størrelse"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        className="flex cursor-grab items-center justify-center py-3 active:cursor-grabbing"
        data-testid="bottom-sheet-handle"
      >
        <div className="bg-muted-foreground/30 h-1 w-10 rounded-full" />
      </div>

      <div
        ref={contentRef}
        className="h-[var(--sheet-content-height,100px)] overflow-y-auto"
      >
        {children}
      </div>
    </div>
  );
}
