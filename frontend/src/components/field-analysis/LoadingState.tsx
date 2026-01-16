'use client';

import React from 'react';

interface LoadingStateProps {
  message?: string;
}

export function LoadingState({ message = 'Indlæser...' }: LoadingStateProps) {
  return (
    <div className="bg-background/80 flex h-full items-center justify-center backdrop-blur-sm">
      <div className="text-center">
        {/* Skeleton-based loading state */}
        <div className="relative mb-6">
          {/* Tractor skeleton - using opacity animation (GPU-accelerated) */}
          <div className="mx-auto h-16 w-16">
            <div className="bg-muted h-full w-full animate-pulse rounded-lg" />
          </div>

          {/* Animated dots - using opacity animation (GPU-accelerated) */}
          <div className="mt-4 flex justify-center space-x-1">
            <div className="bg-primary/60 h-2 w-2 animate-pulse rounded-full" />
            <div
              className="bg-primary/60 h-2 w-2 animate-pulse rounded-full"
              style={{ animationDelay: '0.2s' }}
            />
            <div
              className="bg-primary/60 h-2 w-2 animate-pulse rounded-full"
              style={{ animationDelay: '0.4s' }}
            />
          </div>
        </div>

        {/* Loading message */}
        <div className="text-foreground mb-2 text-lg font-medium">
          {message}
        </div>
        <div className="text-muted-foreground mx-auto max-w-md text-sm">
          Forbereder visualisering af danske landbrugsmarker med miljødata...
        </div>

        {/* Progress skeleton */}
        <div className="mt-6 flex items-center justify-center space-x-2">
          <div className="bg-muted h-3 w-3 animate-pulse rounded-full" />
          <span className="text-muted-foreground/80 text-xs">
            Indlæser PMTiles data
          </span>
        </div>
      </div>
    </div>
  );
}
