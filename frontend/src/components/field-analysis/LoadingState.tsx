'use client';

import React from 'react';

interface LoadingStateProps {
  message?: string;
}

export function LoadingState({ message = 'Indlæser...' }: LoadingStateProps) {
  return (
    <div className="bg-background/80 flex h-full items-center justify-center backdrop-blur-sm">
      <div className="text-center">
        {/* Animated tractor loader */}
        <div className="relative mb-6">
          <div className="mx-auto h-16 w-16">
            <svg
              className="animate-bounce"
              viewBox="0 0 64 64"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              {/* Simple tractor icon */}
              <rect
                x="8"
                y="32"
                width="24"
                height="16"
                rx="2"
                className="fill-primary"
              />
              <rect
                x="32"
                y="24"
                width="20"
                height="24"
                rx="2"
                className="fill-primary"
              />
              <circle cx="16" cy="52" r="8" className="fill-muted-foreground" />
              <circle cx="44" cy="52" r="8" className="fill-muted-foreground" />
              <circle cx="16" cy="52" r="4" className="fill-foreground/60" />
              <circle cx="44" cy="52" r="4" className="fill-foreground/60" />
              <rect
                x="36"
                y="28"
                width="12"
                height="8"
                rx="1"
                className="fill-accent-foreground"
              />
            </svg>
          </div>

          {/* Animated dots */}
          <div className="mt-4 flex justify-center space-x-1">
            <div className="bg-organic h-2 w-2 animate-pulse rounded-full"></div>
            <div className="bg-organic h-2 w-2 animate-pulse rounded-full [animation-delay:0.2s]"></div>
            <div className="bg-organic h-2 w-2 animate-pulse rounded-full [animation-delay:0.4s]"></div>
          </div>
        </div>

        {/* Loading message */}
        <div className="text-foreground mb-2 text-lg font-medium">
          {message}
        </div>
        <div className="text-muted-foreground mx-auto max-w-md text-sm">
          Forbereder visualisering af danske landbrugsmarker med miljødata...
        </div>

        {/* Progress indicators */}
        <div className="text-muted-foreground/80 mt-6 space-y-2 text-xs">
          <div className="flex items-center justify-center space-x-2">
            <div className="border-primary h-3 w-3 animate-spin rounded-full border-2 border-t-transparent"></div>
            <span>Indlæser PMTiles data</span>
          </div>
        </div>
      </div>
    </div>
  );
}
