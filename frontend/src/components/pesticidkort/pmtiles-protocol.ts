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
    const protocol = new Protocol();
    maplibregl.default.addProtocol('pmtiles', protocol.tile);
    win.__pmtiles_protocol_registered = true;
  }
}
