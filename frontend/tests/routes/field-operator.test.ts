import { describe, expect, it } from 'vitest';

import { describeFieldOperator } from '@/lib/field-operator';

describe('describeFieldOperator', () => {
  it('pads seven-digit CVR numbers', () => {
    expect(describeFieldOperator(1234567, false)).toMatchObject({
      cvr: '01234567',
      href: 'https://www.landbruget.dk/virksomhed/01234567',
    });
  });

  it('accepts float-serialised CVR numbers from tiles', () => {
    expect(describeFieldOperator('12345678.0', false)?.cvr).toBe('12345678');
  });

  it('returns null for invalid CVR numbers', () => {
    expect(describeFieldOperator('abc', false)).toBeNull();
    expect(describeFieldOperator('123456789', true)).toBeNull();
    expect(describeFieldOperator(null, true)).toBeNull();
  });

  it('describes fields with pesticide data', () => {
    expect(describeFieldOperator('12345678', true)).toEqual({
      cvr: '12345678',
      label: 'Dyrket og sprøjtning indberettet af CVR 12345678',
      note: 'Sprøjtning indberettes pr. virksomhed og afgrøde – fordelingen på den enkelte mark er beregnet.',
      href: 'https://www.landbruget.dk/virksomhed/12345678',
    });
  });

  it('describes fields without pesticide data', () => {
    expect(describeFieldOperator('12345678', false)).toEqual({
      cvr: '12345678',
      label: 'Markansøger: CVR 12345678',
      href: 'https://www.landbruget.dk/virksomhed/12345678',
    });
  });
});
