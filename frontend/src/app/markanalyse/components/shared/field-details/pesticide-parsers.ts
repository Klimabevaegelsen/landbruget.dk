import type {
  ParsedPesticide,
  ParsedEnhancedPesticide,
} from './field-details-types';

export function parsePesticideDetail(
  detailString: string | undefined
): ParsedPesticide[] {
  return parsePesticideDetailWithUnit(detailString).map(({ name, dosage }) => ({
    name,
    dosage,
  }));
}

const UNIT_MAP: Record<string, string> = {
  '1': 'g',
  '2': 'kg',
  '3': 'tabletter',
  '4': 'L',
  '5': 'ml',
};

/**
 * Parse colon-delimited pesticide detail strings from PMTiles.
 *
 * Format (semicolon-separated products, colon-separated fields):
 *   name:dosage:unitCode:healthRisk:envRisk:signalWord:productGroup:burden
 *
 * Fields 0-5 are the original format. Fields 6-7 (productGroup, burden)
 * were added in the card-redesign (2026-04) and are optional for
 * backward compatibility with older PMTiles.
 */
export function parsePesticideDetailWithUnit(
  detailString: string | undefined
): ParsedEnhancedPesticide[] {
  if (!detailString || detailString.trim() === '') return [];

  try {
    return detailString
      .split(';')
      .map((item) => item.trim())
      .filter((item) => item.length > 0)
      .map((item) => {
        const parts = item.split(':');
        const name = parts[0]?.trim() || 'Ukendt produkt';
        const dosage = parseFloat(parts[1]?.trim() || '0');
        const rawUnit = parts[2]?.trim() || 'ukendt';
        const healthRisk = parts[3]?.trim() || undefined;
        const envRisk = parts[4]?.trim() || undefined;
        const signalWord = parts[5]?.trim() || undefined;
        const productGroup = parts[6]?.trim() || undefined;
        const rawBurden = parts[7]?.trim();
        const burden = rawBurden ? parseFloat(rawBurden) : undefined;

        return {
          name,
          dosage,
          unit: UNIT_MAP[rawUnit] || rawUnit,
          healthRisk: healthRisk && healthRisk !== '' ? healthRisk : undefined,
          envRisk: envRisk && envRisk !== '' ? envRisk : undefined,
          signalWord: signalWord && signalWord !== '' ? signalWord : undefined,
          productGroup:
            productGroup && productGroup !== '' ? productGroup : undefined,
          burden: burden && !isNaN(burden) && burden > 0 ? burden : undefined,
        };
      })
      .filter((item) => item.dosage > 0)
      .sort((a, b) => b.dosage - a.dosage);
  } catch {
    return [];
  }
}
