export interface FieldOperatorInfo {
  cvr: string;
  label: string;
  note?: string;
  href: string;
}

export function describeFieldOperator(
  cvr: string | number | null | undefined,
  hasPesticideData: boolean
): FieldOperatorInfo | null {
  if (cvr === null || cvr === undefined) return null;

  const digitsOnly = String(cvr).trim().replace(/\.0+$/, '').replace(/\D/g, '');
  if (!digitsOnly) return null;

  const normalizedCvr = digitsOnly.padStart(8, '0');
  if (!/^\d{8}$/.test(normalizedCvr)) return null;

  return {
    cvr: normalizedCvr,
    label: hasPesticideData
      ? `Dyrket og sprøjtning indberettet af CVR ${normalizedCvr}`
      : `Markansøger: CVR ${normalizedCvr}`,
    ...(hasPesticideData
      ? {
          note: 'Sprøjtning indberettes pr. virksomhed og afgrøde – fordelingen på den enkelte mark er beregnet.',
        }
      : {}),
    href: `https://www.landbruget.dk/virksomhed/${normalizedCvr}`,
  };
}
