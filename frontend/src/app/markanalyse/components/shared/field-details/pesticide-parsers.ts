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

        return {
          name,
          dosage,
          unit: UNIT_MAP[rawUnit] || rawUnit,
          healthRisk: healthRisk && healthRisk !== '' ? healthRisk : undefined,
          envRisk: envRisk && envRisk !== '' ? envRisk : undefined,
          signalWord: signalWord && signalWord !== '' ? signalWord : undefined,
        };
      })
      .filter((item) => item.dosage > 0)
      .sort((a, b) => b.dosage - a.dosage);
  } catch {
    return [];
  }
}
