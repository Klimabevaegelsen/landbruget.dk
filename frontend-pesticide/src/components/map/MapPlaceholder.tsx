'use client';

export function MapPlaceholder() {
  return (
    <div className="h-full w-full bg-gray-100 flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="text-6xl">🗺️</div>
        <h2 className="text-2xl font-bold text-gray-700">Map Component Placeholder</h2>
        <p className="text-gray-600 max-w-md">
          This is where the Kepler.gl map component will be rendered by Developer 2.
          The map will show H3 hexagonal grids with PFAS exposure data, BNBO protected areas, and BBR building data.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-8 max-w-2xl">
          <div className="bg-white p-4 rounded-lg shadow">
            <div className="text-2xl mb-2">🔷</div>
            <h3 className="font-semibold">H3 Hexagons</h3>
            <p className="text-sm text-gray-600">PFAS exposure heatmap</p>
          </div>
          <div className="bg-white p-4 rounded-lg shadow">
            <div className="text-2xl mb-2">🌿</div>
            <h3 className="font-semibold">BNBO Areas</h3>
            <p className="text-sm text-gray-600">Protected nature areas</p>
          </div>
          <div className="bg-white p-4 rounded-lg shadow">
            <div className="text-2xl mb-2">🏠</div>
            <h3 className="font-semibold">BBR Buildings</h3>
            <p className="text-sm text-gray-600">Building registry data</p>
          </div>
        </div>
      </div>
    </div>
  );
} 