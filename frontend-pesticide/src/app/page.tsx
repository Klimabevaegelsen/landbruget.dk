'use client';

import { Suspense } from 'react';
import { ResponsiveLayout } from '@/components/ui/responsive-layout';
import { MapPlaceholder } from '@/components/map/MapPlaceholder';
import { TimeControls } from '@/components/map/TimeControls';
import { HeatmapToggle } from '@/components/map/HeatmapToggle';
import { LayerControls } from '@/components/map/LayerControls';
import { DataPanel } from '@/components/overlays/DataPanel';
import { LoadingSpinner } from '@/components/ui/loading-spinner';

function MapSection() {
  return (
    <div className="map-container">
      {/* Placeholder for the actual map component (Developer 2's responsibility) */}
      <Suspense fallback={<LoadingSpinner />}>
        <MapPlaceholder />
      </Suspense>
      
      {/* UI Controls - Developer 3's responsibility */}
      <HeatmapToggle />
      <LayerControls />
      <TimeControls />
      <DataPanel />
    </div>
  );
}

export default function HomePage() {
  return (
    <ResponsiveLayout>
      <MapSection />
    </ResponsiveLayout>
  );
} 