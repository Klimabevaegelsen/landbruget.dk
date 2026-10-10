import { describe, expect, it } from 'vitest';

import {
  formatDistanceFromField,
  formatDistanceM,
  formatProximityListForCsv,
  parseDistanceM,
  parseProximityList,
} from '@/lib/proximity-parser';

describe('proximity parser', () => {
  it('parses the old address:distance format', () => {
    expect(
      parseProximityList(
        'Egsgyden 25, 5600 Faaborg:12.3m\nEgsgyden 27, Horne, 5600 Faaborg:48.0m'
      )
    ).toEqual([
      { address: 'Egsgyden 25, 5600 Faaborg', distanceM: 12.3 },
      { address: 'Egsgyden 27, Horne, 5600 Faaborg', distanceM: 48 },
    ]);
  });

  it('parses the address|type:distance format', () => {
    expect(
      parseProximityList('Egsgyden 25, 5600 Faaborg|Sommerhus:12.3m')
    ).toEqual([
      {
        address: 'Egsgyden 25, 5600 Faaborg',
        buildingType: 'Sommerhus',
        distanceM: 12.3,
      },
    ]);
  });

  it('splits on the last colon so addresses may contain colons', () => {
    expect(parseProximityList('Ved porten: stuehus, 1234 By:8.6m')).toEqual([
      { address: 'Ved porten: stuehus, 1234 By', distanceM: 8.6 },
    ]);
  });

  it('keeps only the nearest entry per address', () => {
    expect(
      parseProximityList(
        'Egsgyden 25, 5600 Faaborg:12.3m\nEgsgyden 25, 5600 Faaborg:30.1m\nEgsgyden 27, 5600 Faaborg:48.0m'
      )
    ).toEqual([
      { address: 'Egsgyden 25, 5600 Faaborg', distanceM: 12.3 },
      { address: 'Egsgyden 27, 5600 Faaborg', distanceM: 48 },
    ]);
  });

  it('handles empty and undefined values', () => {
    expect(parseProximityList('')).toEqual([]);
    expect(parseProximityList(undefined)).toEqual([]);
    expect(parseDistanceM(undefined)).toBeNull();
  });

  it('skips junk lines', () => {
    expect(
      parseProximityList('junk\nEgsgyden 25, 5600 Faaborg:12.3m\nnope:abc')
    ).toEqual([{ address: 'Egsgyden 25, 5600 Faaborg', distanceM: 12.3 }]);
  });

  it('rounds distances with a normal space', () => {
    expect(formatDistanceM(12.3)).toBe('12 m');
    expect(formatDistanceM(12.6)).toBe('13 m');
  });

  it('describes distance from the field, treating 0 m as adjacent', () => {
    expect(formatDistanceFromField(7.2)).toBe('7 m fra marken');
    expect(formatDistanceFromField(0.3)).toBe('grænser op til marken');
  });

  it('formats proximity lists for CSV', () => {
    expect(
      formatProximityListForCsv(
        'Egsgyden 25, 5600 Faaborg|Sommerhus:12.3m\nEgsgyden 27, Horne, 5600 Faaborg:48.0m'
      )
    ).toBe(
      'Egsgyden 25, 5600 Faaborg – Sommerhus (12 m); Egsgyden 27, Horne, 5600 Faaborg (48 m)'
    );
  });
});
