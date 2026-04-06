import { test, expect } from '@playwright/test';

test.describe('searchIndex', () => {
  const ENTRIES = [
    {
      cvr: 12345678,
      name: 'Gård Hansen',
      municipality: 'Aarhus',
      type: 'Landbrug',
    },
    {
      cvr: 87654321,
      name: 'Jensen Agro',
      municipality: 'Odense',
      type: 'Landbrug',
    },
    {
      cvr: 12000001,
      name: 'Nordic Farms',
      municipality: 'København',
      type: 'Fødevarer',
    },
    {
      cvr: 99999999,
      name: null as unknown as string,
      municipality: '',
      type: '',
    },
  ];

  function runSearch(
    entries: typeof ENTRIES,
    query: string,
    searchType: string,
    limit = 20
  ) {
    const q = query.toLowerCase();
    const matches: {
      name: string;
      cvr: string;
      address: string;
      type: string;
      id: string;
    }[] = [];
    for (const entry of entries) {
      if (matches.length >= limit) break;
      const cvrStr = String(entry.cvr);
      let match = false;
      if (searchType === 'cvr') {
        match = cvrStr.startsWith(q);
      } else if (searchType === 'company_name') {
        match = entry.name?.toLowerCase().includes(q) ?? false;
      } else {
        match =
          cvrStr.startsWith(q) ||
          (entry.name?.toLowerCase().includes(q) ?? false);
      }
      if (match) {
        matches.push({
          name: entry.name || '',
          cvr: cvrStr,
          address: entry.municipality || '',
          type: entry.type || '',
          id: cvrStr,
        });
      }
    }
    return matches;
  }

  test('should match by CVR prefix in cvr mode', () => {
    const results = runSearch(ENTRIES, '123', 'cvr');
    expect(results).toHaveLength(2);
    expect(results[0].cvr).toBe('12345678');
    expect(results[1].cvr).toBe('12000001');
  });

  test('should match by company name in company_name mode', () => {
    const results = runSearch(ENTRIES, 'jensen', 'company_name');
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Jensen Agro');
    expect(results[0].address).toBe('Odense');
  });

  test('should match by either CVR or name in default mode', () => {
    const results = runSearch(ENTRIES, 'nordic', 'all');
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Nordic Farms');

    const byNumber = runSearch(ENTRIES, '876', 'all');
    expect(byNumber).toHaveLength(1);
    expect(byNumber[0].cvr).toBe('87654321');
  });

  test('should respect limit parameter', () => {
    const results = runSearch(ENTRIES, '1', 'cvr', 1);
    expect(results).toHaveLength(1);
  });

  test('should return empty array for no matches', () => {
    const results = runSearch(ENTRIES, 'zzzzz', 'all');
    expect(results).toHaveLength(0);
  });

  test('should handle null name gracefully', () => {
    const results = runSearch(ENTRIES, 'null', 'company_name');
    expect(results).toHaveLength(0);
  });

  test('should be case-insensitive', () => {
    const results = runSearch(ENTRIES, 'GÅRD', 'company_name');
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Gård Hansen');
  });

  test('should map fields correctly to SearchResult shape', () => {
    const results = runSearch(ENTRIES, '12345678', 'cvr');
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({
      name: 'Gård Hansen',
      cvr: '12345678',
      address: 'Aarhus',
      type: 'Landbrug',
      id: '12345678',
    });
  });
});
