"use client";

import React from "react";

interface MapErrorBoundaryState {
  hasError: boolean;
  error?: Error;
}

interface MapErrorBoundaryProps {
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

export class MapErrorBoundary extends React.Component<
  MapErrorBoundaryProps,
  MapErrorBoundaryState
> {
  constructor(props: MapErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): MapErrorBoundaryState {
    // Update state so the next render will show the fallback UI
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    // Log the error for debugging
    console.error("MapErrorBoundary caught an error:", error, errorInfo);

    // Log specific MapLibre GL JS errors
    if (error.message.includes("missing required property")) {
      console.error("MapLibre data validation error - likely missing GeoJSON data:", error.message);
    }
  }

  render() {
    if (this.state.hasError) {
      // Render fallback UI
      return (
        this.props.fallback || (
          <div className="p-8 text-center bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
            <div className="text-gray-600 mb-2">
              <svg
                className="mx-auto h-12 w-12 text-gray-400 mb-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1}
                  d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 01.553-.894L9 2l6 3 5.447-2.724A1 1 0 0121 3.382v10.764a1 1 0 01-.553.894L15 18l-6-3z"
                />
              </svg>
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                Kort kunne ikke indlæses
              </h3>
              <p className="text-sm text-gray-500">
                Der opstod en fejl under indlæsning af kortdata.
                {this.state.error?.message.includes("missing required property")
                  ? " Kortdata mangler eller er ikke korrekt formateret."
                  : ""}
              </p>
              <button
                onClick={() => this.setState({ hasError: false, error: undefined })}
                className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Prøv igen
              </button>
            </div>
          </div>
        )
      );
    }

    return this.props.children;
  }
}
