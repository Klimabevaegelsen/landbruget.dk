export type ChemicalType = 'pfas' | 'glyphosate' | 'diquat';

export interface ChemicalInfo {
  name: string;
  category: string;
  risk: 'high' | 'moderate' | 'low';
  summary: string;
  details: string;
  source: string;
}

export const CHEMICAL_EXPLAINERS: Record<ChemicalType, ChemicalInfo> = {
  pfas: {
    name: 'PFAS',
    category: 'Evighedskemikalie',
    risk: 'high',
    summary:
      'PFAS-holdige pesticider nedbrydes ikke i naturen og kan ophobes i drikkevand og jord.',
    details:
      'PFAS (per- og polyfluoralkylstoffer) er en gruppe syntetiske kemikalier, der er ekstremt persistente i miljøet. De kan akkumuleres i mennesker og dyr over tid. PFAS-holdige pesticider er under skærpet overvågning af Miljøstyrelsen.',
    source: 'Miljøstyrelsen / EFSA',
  },
  glyphosate: {
    name: 'Glyphosat',
    category: 'Ukrudtsmiddel',
    risk: 'moderate',
    summary:
      'Glyphosat er det mest anvendte pesticid i Danmark og bruges primært til ukrudtsbekæmpelse før såning.',
    details:
      'Glyphosat er et bredspektret herbicid, der hæmmer et enzym, som planter bruger til at vokse. EU har forlænget godkendelsen til 2033 med nye restriktioner for brug nær vandløb og beboelse.',
    source: 'EFSA / Miljøstyrelsen',
  },
  diquat: {
    name: 'Diquat',
    category: 'Ukrudtsmiddel (forbudt)',
    risk: 'high',
    summary:
      'Diquat er forbudt i EU siden 2019 på grund af sundhedsrisici. Historisk data viser tidligere brug.',
    details:
      'Diquat var et kontaktherbicid, der blev brugt til nedvisning af afgrøder før høst. EU-Kommissionen trak godkendelsen tilbage i 2019 efter vurdering af risici for menneskers sundhed og miljøet.',
    source: 'EU-Kommissionen',
  },
};
