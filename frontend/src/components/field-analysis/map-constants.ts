import { FilterState } from './types';

export interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown, beforeId?: string) => unknown;
  removeLayer: (id: string) => void;
  removeSource: (id: string) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  setPaintProperty: (id: string, prop: string, value: unknown) => void;
  addSource: (id: string, source: unknown) => void;
  addImage: (
    id: string,
    image: HTMLCanvasElement | ImageBitmap | ImageData
  ) => void;
  hasImage: (id: string) => boolean;
  setFilter: (id: string, filter: unknown) => void;
}

export interface MapLibreLayer {
  id: string;
  source: string;
  'source-layer': string;
  type: string;
  paint: Record<string, unknown>;
  layout: Record<string, unknown>;
  filter?: unknown;
  minzoom?: number;
  maxzoom?: number;
}

export interface TooltipInfo {
  x: number;
  y: number;
  properties: Record<string, unknown>;
  layerName: string;
  visualizationMode: FilterState['visualizationMode'];
  colorUnit: FilterState['colorUnit'];
}

export const FALLBACK_MAP_STYLE = {
  version: 8 as const,
  sources: {},
  layers: [
    {
      id: 'background',
      type: 'background' as const,
      paint: {
        'background-color': '#f8f9fa',
      },
    },
  ],
};

export const INTERACTIVE_LAYER_IDS = [
  'fields-fill',
  'bnbo-fill',
  'wetlands-fill',
  'water-projects-fill',
  'buildings-fill',
];

export function getLayerDisplayName(layerId: string): string {
  if (layerId.startsWith('fields-')) return 'Landbrugsmark';
  if (layerId.startsWith('bnbo-')) return 'BNBO Område';
  if (layerId.startsWith('wetlands-')) return 'Lavbundsområde';
  if (layerId.startsWith('water-projects-')) return 'Vandprojekt';
  if (layerId.startsWith('buildings-')) return 'Bygning';
  return 'Ukendt lag';
}

export const BBR_USAGE_LABELS: Record<string, string> = {
  // Boliger (100-199)
  '110': 'Stuehus til landbrugsejendom',
  '120': 'Fritliggende enfamiliehus',
  '130': 'Række-, kæde- eller dobbelthus',
  '140': 'Etageboligbebyggelse',
  '150': 'Kollegium',
  '160': 'Døgninstitution',
  '190': 'Anden boligbenyttelse',
  // Erhverv (200-299)
  '210': 'Kontor og lign.',
  '211': 'Pengeinstitut, forsikring og lign.',
  '212': 'Offentlig administration',
  '213': 'Liberalt erhverv',
  '214': 'Anden kontorvirksomhed',
  '215': 'Konsulentvirksomhed og lign.',
  '216': 'Virksomhed og kontor i samme bygning',
  '217': 'Blandet erhverv og kontor',
  '218': 'IT og kommunikation',
  '219': 'Anden erhvervsvirksomhed',
  '220': 'Butik og lign.',
  '230': 'Hotel og restaurant',
  '240': 'Finansiel tjeneste',
  '250': 'Håndværk og industri i bymæssig bebyggelse',
  '290': 'Anden erhvervsbebyggelse',
  // Produktions- og lagerbygninger (300-399)
  '310': 'Industri',
  '320': 'Værksted og lign.',
  '330': 'Lager',
  '340': 'Energiproduktion og -forsyning',
  '390': 'Anden produktions- og lagerbygning',
  // Transport (400-499)
  '410': 'Garageanlæg',
  '420': 'Bygning til kollektiv transport',
  '421': 'Jernbanestation og lign.',
  '422': 'Bustation og lign.',
  '429': 'Anden transportbygning',
  '441': 'Lufthavn',
  '490': 'Anden transportbebyggelse',
  // Institutioner (500-599)
  '510': 'Undervisning og forskning',
  '520': 'Hospital og sygehus',
  '530': 'Sundhed og sociale formål',
  '540': 'Institution',
  '550': 'Forsamling og sport',
  '560': 'Kultur og kirke',
  '590': 'Anden institutionsbebyggelse',
  // Fritidsbebyggelse (600-699)
  '610': 'Sommerhus',
  '620': 'Anden fritidsbebyggelse',
  '690': 'Anden fritidsbebyggelse',
  // Landbrugs- og skovbrugsbygninger (900-999)
  '910': 'Stuehus til landbrugsejendom',
  '920': 'Driftsbygning til landbrugsejendom',
  '930': 'Anden bygning til landbrugsformål',
  '940': 'Bygning til gartneri, planteskole og lign.',
  '950': 'Bygning til pelsdyravl',
  '960': 'Bygning til fiskeopdræt',
  '970': 'Skovbrugsbygning',
  '990': 'Anden landbrugs- eller skovbrugsbygning',
};

export const BUILDING_CATEGORY_LABELS: Record<string, string> = {
  residential: 'Bolig',
  agricultural: 'Landbrug',
  publicServices: 'Skole og daginstitutioner',
};

export const INSPIRE_USAGE_LABELS: Record<string, string> = {
  individualResidence: 'Enfamilieboliger',
  agriculture: 'Landbrugsbygninger',
  collectiveResidence: 'Flerfamilieboliger',
  twoDwellings: 'Tofamiliehuse',
  publicServices: 'Skole og daginstitutioner',
};
