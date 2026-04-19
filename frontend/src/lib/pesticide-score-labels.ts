/* oxlint-disable landbruget/require-test-coverage */
import type {
  PesticideGrade,
  GradeInfo,
} from '@/components/pesticidkort/types';

export const GRADE_DEFINITIONS: Record<
  PesticideGrade,
  Omit<GradeInfo, 'grade'>
> = {
  TOP_1: {
    label: 'Top 1% mest eksponeret',
    description:
      'Din adresse er blandt de 1% mest pesticideksponerede i Danmark',
  },
  TOP_5: {
    label: 'Top 5% mest eksponeret',
    description:
      'Din adresse er blandt de 5% mest pesticideksponerede i Danmark',
  },
  TOP_10: {
    label: 'Top 10% mest eksponeret',
    description: 'Din adresse er blandt de 10% mest pesticideksponerede',
  },
  TOP_25: {
    label: 'Top 25% mest eksponeret',
    description: 'Din adresse er mere eksponeret end 75% af Danmark',
  },
  TOP_50: {
    label: 'Top 50% mest eksponeret',
    description: 'Din adresse er mere eksponeret end halvdelen af Danmark',
  },
  UNDER_AVG: {
    label: 'Under gennemsnit',
    description:
      'Din adresse har lavere pesticideksponering end landsgennemsnittet',
  },
};

/** Best → worst. Iterate this when drawing the 6-segment bar. */
export const GRADE_ORDER: PesticideGrade[] = [
  'UNDER_AVG',
  'TOP_50',
  'TOP_25',
  'TOP_10',
  'TOP_5',
  'TOP_1',
];
