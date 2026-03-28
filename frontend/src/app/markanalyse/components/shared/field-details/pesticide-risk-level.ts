import type { PesticideRiskLevel } from './field-details-types';

export function getPesticideRiskLevel(belastning: number): PesticideRiskLevel {
  if (belastning === 0)
    return {
      level: 'Ingen',
      color: 'text-primary',
      description: 'Ingen registreret pesticidanvendelse',
      variant: 'secondary',
    };
  if (belastning < 10)
    return {
      level: 'Lav',
      color: 'text-conventional',
      description: 'Lav pesticidbelastning',
      variant: 'default',
    };
  if (belastning < 50)
    return {
      level: 'Moderat',
      color: 'text-conventional',
      description: 'Moderat pesticidbelastning',
      variant: 'secondary',
    };
  return {
    level: 'Høj',
    color: 'text-destructive',
    description: 'Høj pesticidbelastning',
    variant: 'destructive',
  };
}
