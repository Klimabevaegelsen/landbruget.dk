'use client';

export function MapPlaceholder() {
  return (
    <div className="flex h-full w-full items-center justify-center bg-gray-100">
      <div className="space-y-4 text-center">
        <div className="text-6xl">🗺️</div>
        <h2 className="text-2xl font-bold text-gray-700">
          Map Component Placeholder
        </h2>
        <p className="max-w-md text-gray-600">
          This is where the Kepler.gl map component will be rendered by
          Developer 2. The map will show H3 hexagonal grids with PFAS exposure
          data, BNBO protected areas, and BBR building data.
        </p>
        <div className="mt-8 grid max-w-2xl grid-cols-1 gap-4 md:grid-cols-3">
          <div className="rounded-lg bg-white p-4 shadow">
            <div className="mb-2 text-2xl">🔷</div>
            <h3 className="font-semibold">H3 Hexagons</h3>
            <p className="text-sm text-gray-600">PFAS exposure heatmap</p>
          </div>
          <div className="rounded-lg bg-white p-4 shadow">
            <div className="mb-2 text-2xl">🌿</div>
            <h3 className="font-semibold">BNBO Areas</h3>
            <p className="text-sm text-gray-600">Protected nature areas</p>
          </div>
          <div className="rounded-lg bg-white p-4 shadow">
            <div className="mb-2 text-2xl">🏠</div>
            <h3 className="font-semibold">BBR Buildings</h3>
            <p className="text-sm text-gray-600">Building registry data</p>
          </div>
        </div>
      </div>
    </div>
  );
}
