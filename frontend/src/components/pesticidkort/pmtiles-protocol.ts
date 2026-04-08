/** Register PMTiles protocol with MapLibre (idempotent). */
export async function registerPmtilesProtocol() {
  const [maplibregl, { Protocol, SharedCache }] = await Promise.all([
    import('maplibre-gl'),
    import('pmtiles'),
  ]);
  const win = window as unknown as {
    __pmtiles_protocol_registered?: boolean;
  };
  if (!win.__pmtiles_protocol_registered) {
    const cache = new SharedCache();
    const protocol = new Protocol(cache);
    maplibregl.default.addProtocol('pmtiles', protocol.tile);
    win.__pmtiles_protocol_registered = true;
  }
}
