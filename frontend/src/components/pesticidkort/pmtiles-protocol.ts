import type { Protocol as ProtocolType, Cache as PMTilesCache } from 'pmtiles';

let protocolInstance: ProtocolType | null = null;
let sharedCache: PMTilesCache | null = null;

/** Register PMTiles protocol with MapLibre (idempotent). */
export async function registerPmtilesProtocol() {
  const [maplibregl, { Protocol, SharedPromiseCache }] = await Promise.all([
    import('maplibre-gl'),
    import('pmtiles'),
  ]);
  const win = window as unknown as {
    __pmtiles_protocol_registered?: boolean;
  };
  if (!win.__pmtiles_protocol_registered) {
    sharedCache = new SharedPromiseCache(600);
    protocolInstance = new Protocol();
    maplibregl.addProtocol('pmtiles', protocolInstance.tile);
    win.__pmtiles_protocol_registered = true;
  }
}

/** Get the registered Protocol instance (null if not yet registered). */
export function getPmtilesProtocol(): ProtocolType | null {
  return protocolInstance;
}

/** Get the shared cache for PMTiles instances (null if not yet registered). */
export function getSharedPmtilesCache(): PMTilesCache | null {
  return sharedCache;
}
