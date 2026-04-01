import type { AddressResult } from '@/components/pesticidkort/types';

export interface DAWAResult {
  tekst: string;
  adresse?: { id: string; href: string; x: number; y: number };
}

export async function resolveCoordinates(
  r: DAWAResult
): Promise<AddressResult | null> {
  if (r.adresse?.href) {
    try {
      const res = await fetch(r.adresse.href);
      const data = await res.json();
      if (data.adgangsadresse?.koordinater) {
        const [lng, lat] = data.adgangsadresse.koordinater;
        return { lat, lng, address: r.tekst };
      }
    } catch {
      /* fallback to x/y */
    }
  }
  if (r.adresse?.x && r.adresse?.y) {
    return { lat: r.adresse.y, lng: r.adresse.x, address: r.tekst };
  }
  return null;
}
