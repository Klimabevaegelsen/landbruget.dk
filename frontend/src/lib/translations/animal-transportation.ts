/**
 * Danish translations for animal transportation destination and origin types
 * Based on CHR (Central Husbandry Register) business type classifications
 */

export function translateDestinationType(destinationType: string): string {
  const translations: Record<string, string> = {
    // Processing facilities
    Slaughterhouse: 'Slagteri',
    'Rendering Plant': 'Forarbejdningsanlæg',

    // Collection and logistics infrastructure
    'Collection Center': 'Samlecenter',
    'Collection Point': 'Afhentningsplads',
    'Cooling Facility': 'Køleanlæg',

    // Production farms by type
    'Production Farm': 'Produktionsbesætning',
    'Breeding Farm': 'Avls- og opformeringsbesætning',
    'Piglet Farm': 'Smågriseopdræt',
    'Free-range Pig Farm': 'Frilandssvin',
    'Organic Pig Farm': 'Økologisk svin',

    // Cattle farms by type
    'Dairy Farm': 'Malkekvæg',
    'Beef Farm': 'Kødkvæg',
    'Heifer Hotel': 'Kviehotel',
    'Veal Farm': 'Slagtekalv',

    // Specialized facilities
    'Quarantine Facility': 'Karantænefacilitet',
    'Research Facility': 'Forsøgsfacilitet',
    'AI Station': 'Sædopsamlingsstation',

    // Trading and markets
    'Market/Trading': 'Marked/Handel',

    // Hobby and recreational
    'Hobby Farm': 'Hobbybesætning',
    'Boarding/Riding School': 'Pension/Rideskole',
    'Stud Farm': 'Stutteri',
    'Racing/Training': 'Væddeløb/Træning',

    // Other livestock operations
    'Other Livestock': 'Øvrige husdyr',
    'Livestock Farm': 'Husdyrbesætning',

    // Environmental and seasonal
    'Seasonal Grazing': 'Sæsonafgræsning',
    'Nature Management': 'Naturpleje',

    // Events and exhibitions
    'Livestock Show': 'Dyrskue',
    Zoo: 'Zoologisk have',

    // International movements
    'International Export': 'International eksport',

    // Fallback categories
    'Other Commercial': 'Anden kommerciel',
    Unknown: 'Ukendt',
  };

  return translations[destinationType] || destinationType;
}

/**
 * Alias for origin types (uses same translations as destination types)
 */
export function translateOriginType(originType: string): string {
  return translateDestinationType(originType);
}

/**
 * Get all available destination type translations
 * Useful for filtering, legends, etc.
 */
export function getAllDestinationTypeTranslations(): Record<string, string> {
  return {
    // Processing facilities
    Slaughterhouse: 'Slagteri',
    'Rendering Plant': 'Forarbejdningsanlæg',

    // Collection and logistics infrastructure
    'Collection Center': 'Samlecenter',
    'Collection Point': 'Afhentningsplads',
    'Cooling Facility': 'Køleanlæg',

    // Production farms by type
    'Production Farm': 'Produktionsbesætning',
    'Breeding Farm': 'Avls- og opformeringsbesætning',
    'Piglet Farm': 'Smågriseopdræt',
    'Free-range Pig Farm': 'Frilandssvin',
    'Organic Pig Farm': 'Økologisk svin',

    // Cattle farms by type
    'Dairy Farm': 'Malkekvæg',
    'Beef Farm': 'Kødkvæg',
    'Heifer Hotel': 'Kviehotel',
    'Veal Farm': 'Slagtekalv',

    // Specialized facilities
    'Quarantine Facility': 'Karantænefacilitet',
    'Research Facility': 'Forsøgsfacilitet',
    'AI Station': 'Sædopsamlingsstation',

    // Trading and markets
    'Market/Trading': 'Marked/Handel',

    // Hobby and recreational
    'Hobby Farm': 'Hobbybesætning',
    'Boarding/Riding School': 'Pension/Rideskole',
    'Stud Farm': 'Stutteri',
    'Racing/Training': 'Væddeløb/Træning',

    // Other livestock operations
    'Other Livestock': 'Øvrige husdyr',
    'Livestock Farm': 'Husdyrbesætning',

    // Environmental and seasonal
    'Seasonal Grazing': 'Sæsonafgræsning',
    'Nature Management': 'Naturpleje',

    // Events and exhibitions
    'Livestock Show': 'Dyrskue',
    Zoo: 'Zoologisk have',

    // International movements
    'International Export': 'International eksport',

    // Fallback categories
    'Other Commercial': 'Anden kommerciel',
    Unknown: 'Ukendt',
  };
}
