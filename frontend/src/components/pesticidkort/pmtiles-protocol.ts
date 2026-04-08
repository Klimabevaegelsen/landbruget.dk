import type { Protocol as ProtocolType } from 'pmtiles';

let protocolInstance: ProtocolType | null = null;

/** Register PMTiles protocol with MapLibre (idempotent). */
export async function registerPmtilesProtocol() {
  const [maplibregl, { Protocol }] = await Promise.all([
    import('maplibre-gl'),
    import('pmtiles'),
  ]);
  const win = window as unknown as {
    __pmtiles_protocol_registered?: boolean;
  };
  if (!win.__pmtiles_protocol_registered) {
    protocolInstance = new Protocol();
    maplibregl.default.addProtocol('pmtiles', protocolInstance.tile);
    win.__pmtiles_protocol_registered = true;
  }
}

/** Get the registered Protocol instance (null if not yet registered). */
export function getPmtilesProtocol(): ProtocolType | null {
  return protocolInstance;
}
