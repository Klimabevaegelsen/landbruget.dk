"use client";

import React from "react";

interface LoadingStateProps {
  message?: string;
}

export function LoadingState({ message = "Indlæser..." }: LoadingStateProps) {
  return (
    <div className="flex items-center justify-center h-full bg-gray-50">
      <div className="text-center">
        {/* Animated tractor loader */}
        <div className="relative mb-6">
          <div className="w-16 h-16 mx-auto">
            <svg
              className="animate-bounce"
              viewBox="0 0 64 64"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              {/* Simple tractor icon */}
              <rect x="8" y="32" width="24" height="16" rx="2" fill="#10B981" />
              <rect x="32" y="24" width="20" height="24" rx="2" fill="#10B981" />
              <circle cx="16" cy="52" r="8" fill="#374151" />
              <circle cx="44" cy="52" r="8" fill="#374151" />
              <circle cx="16" cy="52" r="4" fill="#6B7280" />
              <circle cx="44" cy="52" r="4" fill="#6B7280" />
              <rect x="36" y="28" width="12" height="8" rx="1" fill="#3B82F6" />
            </svg>
          </div>

          {/* Animated dots */}
          <div className="flex justify-center space-x-1 mt-4">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" style={{ animationDelay: "0.2s" }}></div>
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" style={{ animationDelay: "0.4s" }}></div>
          </div>
        </div>

        {/* Loading message */}
        <div className="text-lg font-medium text-gray-900 mb-2">{message}</div>
        <div className="text-sm text-gray-600 max-w-md mx-auto">
          Forbereder visualisering af danske landbrugsmarker med miljødata...
        </div>

        {/* Progress indicators */}
        <div className="mt-6 space-y-2 text-xs text-gray-500">
          <div className="flex items-center justify-center space-x-2">
            <div className="w-3 h-3 border-2 border-green-500 border-t-transparent rounded-full animate-spin"></div>
            <span>Indlæser PMTiles data</span>
          </div>
        </div>
      </div>
    </div>
  );
}
