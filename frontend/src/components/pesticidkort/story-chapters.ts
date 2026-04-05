export interface StoryChapter {
  id: string;
  title: string;
  body: string;
  mapZoom: number;
  showLayers: ('fields-fill' | 'proximity-rings')[];
  paintMode: 'burden' | 'pfas' | 'default';
  visual?: 'standard' | 'stat-callout' | 'warning-accent' | 'grade-display';
  statKey?: string;
  statSuffix?: string;
}

/**
 * Chapter templates for scrollytelling. Placeholders like {year},
 * {fields_count}, {pfas_count}, {distance} are interpolated at runtime.
 */
export const STORY_CHAPTERS: StoryChapter[] = [
  {
    id: 'location',
    title: 'Du bor her',
    body: 'Inden for 1 kilometer fra din adresse ligger {fields_count} landbrugsmarker. De sprøjtes hvert år med pesticider — kemikalier, der skal beskytte afgrøder, men som også påvirker grundvand, insekter og mennesker.',
    mapZoom: 14,
    showLayers: ['fields-fill', 'proximity-rings'],
    paintMode: 'default',
    visual: 'standard',
  },
  {
    id: 'sprayed',
    title: 'Hvad sprøjtes?',
    body: 'I {year} blev markerne i dit nærområde behandlet med pesticider svarende til en samlet belastning på {total_burden} B/ha. Jo højere belastning, jo kraftigere er den samlede påvirkning af miljøet.',
    mapZoom: 14,
    showLayers: ['fields-fill'],
    paintMode: 'burden',
    visual: 'stat-callout',
    statKey: 'total_burden',
    statSuffix: 'B/ha',
  },
  {
    id: 'pfas',
    title: 'Evighedskemikalier',
    body: '{pfas_count} marker bruger produkter med PFAS — fluorholdige stoffer, som ikke nedbrydes i naturen. De ophobes i jord, vand og mennesker og er forbundet med kræft, hormonforstyrrelse og immunpåvirkning.',
    mapZoom: 14,
    showLayers: ['fields-fill'],
    paintMode: 'pfas',
    visual: 'warning-accent',
  },
  {
    id: 'proximity',
    title: 'Hvor tæt er for tæt?',
    body: 'Den nærmeste mark er {distance} meter fra din adresse. Forskning viser, at afdrift af sprøjtemidler kan nå 100–250 meter fra markens kant — selv med moderne præcisionssprøjter.',
    mapZoom: 15,
    showLayers: ['fields-fill', 'proximity-rings'],
    paintMode: 'default',
    visual: 'stat-callout',
    statKey: 'distance',
    statSuffix: 'm',
  },
  {
    id: 'score',
    title: 'Din score',
    body: 'Baseret på mængden, typen og afstanden af pesticider nær din adresse får du karakteren {grade}. Scoren kombinerer belastningstal, PFAS-brug og nærhed til drikkevandsboring.',
    mapZoom: 13,
    showLayers: ['fields-fill'],
    paintMode: 'burden',
    visual: 'grade-display',
    statKey: 'grade',
  },
];

/** Replace placeholders in chapter body text. */
export function interpolateChapter(
  body: string,
  data: Record<string, string | number>
): string {
  return body.replace(/\{(\w+)\}/g, (_, key: string) =>
    String(data[key] ?? `{${key}}`)
  );
}
