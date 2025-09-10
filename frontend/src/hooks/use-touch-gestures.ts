'use client';

import { useRef, useEffect, useCallback, useState } from 'react';

interface TouchGestureOptions {
  onSwipeLeft?: () => void;
  onSwipeRight?: () => void;
  onSwipeUp?: () => void;
  onSwipeDown?: () => void;
  onLongPress?: () => void;
  threshold?: number; // Minimum distance for swipe
  longPressDelay?: number; // Delay for long press in ms
}

/**
 * Hook for handling touch gestures
 * Inspired by Midday's mobile interaction patterns
 */
export function useTouchGestures<T extends HTMLElement>(
  options: TouchGestureOptions = {}
) {
  const elementRef = useRef<T>(null);
  const touchStartRef = useRef<{ x: number; y: number; time: number } | null>(
    null
  );
  const longPressTimerRef = useRef<NodeJS.Timeout | null>(null);

  const {
    onSwipeLeft,
    onSwipeRight,
    onSwipeUp,
    onSwipeDown,
    onLongPress,
    threshold = 50,
    longPressDelay = 500,
  } = options;

  const handleTouchStart = useCallback(
    (e: TouchEvent) => {
      const touch = e.touches[0];
      touchStartRef.current = {
        x: touch.clientX,
        y: touch.clientY,
        time: Date.now(),
      };

      // Start long press timer
      if (onLongPress) {
        longPressTimerRef.current = setTimeout(() => {
          onLongPress();
          touchStartRef.current = null; // Prevent swipe after long press
        }, longPressDelay);
      }
    },
    [onLongPress, longPressDelay]
  );

  const handleTouchMove = useCallback(() => {
    // Cancel long press if user moves finger
    if (longPressTimerRef.current) {
      clearTimeout(longPressTimerRef.current);
      longPressTimerRef.current = null;
    }
  }, []);

  const handleTouchEnd = useCallback(
    (e: TouchEvent) => {
      // Clear long press timer
      if (longPressTimerRef.current) {
        clearTimeout(longPressTimerRef.current);
        longPressTimerRef.current = null;
      }

      if (!touchStartRef.current) return;

      const touch = e.changedTouches[0];
      const deltaX = touch.clientX - touchStartRef.current.x;
      const deltaY = touch.clientY - touchStartRef.current.y;
      const absDeltaX = Math.abs(deltaX);
      const absDeltaY = Math.abs(deltaY);

      // Check if movement is significant enough to be considered a swipe
      if (absDeltaX > threshold || absDeltaY > threshold) {
        // Determine primary direction
        if (absDeltaX > absDeltaY) {
          // Horizontal swipe
          if (deltaX > 0 && onSwipeRight) {
            onSwipeRight();
          } else if (deltaX < 0 && onSwipeLeft) {
            onSwipeLeft();
          }
        } else {
          // Vertical swipe
          if (deltaY > 0 && onSwipeDown) {
            onSwipeDown();
          } else if (deltaY < 0 && onSwipeUp) {
            onSwipeUp();
          }
        }
      }

      touchStartRef.current = null;
    },
    [onSwipeLeft, onSwipeRight, onSwipeUp, onSwipeDown, threshold]
  );

  useEffect(() => {
    const element = elementRef.current;
    if (!element) return;

    // Add touch event listeners
    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchmove', handleTouchMove, { passive: true });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });

    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchmove', handleTouchMove);
      element.removeEventListener('touchend', handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchMove, handleTouchEnd]);

  return elementRef;
}

/**
 * Hook for handling pull-to-refresh gesture
 */
export function usePullToRefresh(onRefresh: () => void, threshold = 100) {
  const elementRef = useRef<HTMLElement>(null);
  const pullStartRef = useRef<{ y: number; scrollTop: number } | null>(null);
  const [isPulling, setIsPulling] = useState(false);

  const handleTouchStart = useCallback((e: TouchEvent) => {
    const element = elementRef.current;
    if (!element) return;

    const touch = e.touches[0];
    pullStartRef.current = {
      y: touch.clientY,
      scrollTop: element.scrollTop,
    };
  }, []);

  const handleTouchMove = useCallback(
    (e: TouchEvent) => {
      const element = elementRef.current;
      if (!element || !pullStartRef.current) return;

      const touch = e.touches[0];
      const deltaY = touch.clientY - pullStartRef.current.y;

      // Only allow pull when at top of scroll
      if (pullStartRef.current.scrollTop === 0 && deltaY > 0) {
        setIsPulling(deltaY > threshold);
        // Prevent default scrolling when pulling
        if (deltaY > 20) {
          e.preventDefault();
        }
      }
    },
    [threshold]
  );

  const handleTouchEnd = useCallback(() => {
    if (isPulling) {
      onRefresh();
    }
    setIsPulling(false);
    pullStartRef.current = null;
  }, [isPulling, onRefresh]);

  useEffect(() => {
    const element = elementRef.current;
    if (!element) return;

    element.addEventListener('touchstart', handleTouchStart, { passive: true });
    element.addEventListener('touchmove', handleTouchMove, { passive: false });
    element.addEventListener('touchend', handleTouchEnd, { passive: true });

    return () => {
      element.removeEventListener('touchstart', handleTouchStart);
      element.removeEventListener('touchmove', handleTouchMove);
      element.removeEventListener('touchend', handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchMove, handleTouchEnd]);

  return { elementRef, isPulling };
}
