import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  addressResultFromDetails,
  utm32ToWgs84,
} from '@/components/pesticidkort/address-coordinates';
import {
  isSelectableAddress,
  resolveCoordinates,
  searchAddresses,
  type AdressevaelgerResult,
} from '@/components/pesticidkort/address-utils';

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function mockFetch(response: Response) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
  return vi.mocked(globalThis.fetch);
}

function requestedUrl(fetchMock: typeof fetch) {
  const [input] = vi.mocked(fetchMock).mock.calls[0];
  return new URL(String(input));
}

function addressResult(
  type: AdressevaelgerResult['type'],
  id = 'address-id'
): AdressevaelgerResult {
  return { id, type, titel: 'Rådhuspladsen 1, 1550 København V' };
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('searchAddresses', () => {
  it('parses Adressevælger fund and drops malformed items', async () => {
    const fetchMock = mockFetch(
      jsonResponse({
        status: 'ok',
        fund: [
          {
            type: 'adresse',
            id: '0a3f50bc-2a2c-32b8-e044-0003ba298018',
            titel: 'Nørregade 1, 6000 Kolding',
          },
          { type: 'adresse', id: 'missing-title' },
          null,
          'not an address',
        ],
      })
    );

    await expect(searchAddresses('Nørregade')).resolves.toEqual([
      {
        type: 'adresse',
        id: '0a3f50bc-2a2c-32b8-e044-0003ba298018',
        titel: 'Nørregade 1, 6000 Kolding',
      },
    ]);
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('throws Adressevælger error descriptions', async () => {
    mockFetch(
      jsonResponse({
        status: 'fejl',
        beskrivelse: 'Søgningen kunne ikke gennemføres.',
        fund: [],
      })
    );

    await expect(searchAddresses('Nørregade')).rejects.toThrow(
      'Søgningen kunne ikke gennemføres.'
    );
  });

  it('returns no results below two trimmed characters without fetching', async () => {
    const fetchMock = vi.mocked(globalThis.fetch);

    await expect(searchAddresses(' A ')).resolves.toEqual([]);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('rejects search text over 73 characters without fetching', async () => {
    const fetchMock = vi.mocked(globalThis.fetch);

    await expect(searchAddresses('a'.repeat(74))).rejects.toThrow(
      'Adressevælger accepts search text up to 73 characters.'
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('requests up to eight suggestions with the configured token', async () => {
    const fetchMock = mockFetch(jsonResponse({ status: 'ok', fund: [] }));

    await searchAddresses('  Nørregade  ');

    const url = requestedUrl(fetchMock);
    expect(url.pathname).toBe('/adresser/soeg');
    expect(url.searchParams.get('tekst')).toBe('Nørregade');
    expect(url.searchParams.get('token')).toBe('adressevaelger123');
    expect(url.searchParams.get('maksimum')).toBe('8');
  });
});

describe('resolveCoordinates', () => {
  it('resolves a husnummer from /husnumre/{id} and uses its access address label', async () => {
    const fetchMock = mockFetch(
      jsonResponse({
        status: 'ok',
        husnummer: {
          adgangsadressebetegnelse: 'Rådhuspladsen 1, 1550 København V',
          adgangspunkt: {
            koordinater: { x: 724434.93, y: 6175755.61 },
          },
        },
      })
    );

    const location = await resolveCoordinates(addressResult('husnummer'));

    expect(requestedUrl(fetchMock).pathname).toBe('/husnumre/address-id');
    expect(location?.address).toBe('Rådhuspladsen 1, 1550 København V');
    expect(location?.lat).toBeCloseTo(55.6756275, 4);
    expect(location?.lng).toBeCloseTo(12.5695777, 4);
  });

  it('resolves an adresse from /adresser/{id} and uses its address label', async () => {
    const fetchMock = mockFetch(
      jsonResponse({
        status: 'ok',
        adresse: {
          adressebetegnelse: 'Rådhuspladsen 1, 3300 Frederiksværk',
          husnummer: {
            adgangspunkt: {
              koordinater: { x: 688190.35, y: 6207578.8 },
            },
          },
        },
      })
    );

    const location = await resolveCoordinates(addressResult('adresse'));

    expect(requestedUrl(fetchMock).pathname).toBe('/adresser/address-id');
    expect(location?.address).toBe('Rådhuspladsen 1, 3300 Frederiksværk');
  });

  it('does not fetch a result that cannot be selected', async () => {
    const fetchMock = vi.mocked(globalThis.fetch);

    await expect(
      resolveCoordinates(addressResult('navngivenvejpostnummer'))
    ).resolves.toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('returns null when the detail response has no coordinates', async () => {
    mockFetch(
      jsonResponse({
        status: 'ok',
        adresse: { adressebetegnelse: 'Rådhuspladsen 1' },
      })
    );

    await expect(
      resolveCoordinates(addressResult('adresse'))
    ).resolves.toBeNull();
  });

  it('identifies only addresses and house numbers as selectable', () => {
    expect(isSelectableAddress(addressResult('adresse'))).toBe(true);
    expect(isSelectableAddress(addressResult('husnummer'))).toBe(true);
    expect(isSelectableAddress(addressResult('navngivenvejpostnummer'))).toBe(
      false
    );
  });
});

describe('addressResultFromDetails', () => {
  it('reads geometry coordinates when projected coordinate properties are absent', () => {
    const result = addressResultFromDetails(
      {
        adresse: {
          adressebetegnelse: 'Test address',
          husnummer: {
            adgangspunkt: {
              geometri: { coordinates: [724434.93, 6175755.61] },
            },
          },
        },
      },
      'Fallback address'
    );

    expect(result).toEqual({
      address: 'Test address',
      lat: expect.any(Number),
      lng: expect.any(Number),
    });
  });
});

describe('utm32ToWgs84', () => {
  it('converts Rådhuspladsen coordinates against an independent PROJ result', () => {
    // Independent `gdaltransform -s_srs EPSG:25832 -t_srs EPSG:4326` output:
    // (724434.93, 6175755.61) -> (lng 12.56957768, lat 55.67562750).
    // pyproj is not installed here. SPEC.md's lng 12.5690 differs from this
    // EPSG:25832 result by about 36 m; the port retains the reference transform.
    const point = utm32ToWgs84(724434.93, 6175755.61);

    expect(point.lng).toBeCloseTo(12.56957768, 4);
    expect(point.lat).toBeCloseTo(55.6756275, 4);
  });

  it('keeps Skagen and Bornholm points within Denmark bounds', () => {
    // Independent PROJ outputs: Skagen -> (10.52748181, 57.71494144),
    // Bornholm -> (14.80415877, 55.11514725). Check plausible DK bounds here.
    for (const [x, y] of [
      [591000, 6398000],
      [870000, 6123000],
    ]) {
      const point = utm32ToWgs84(x, y);
      expect(point.lng).toBeGreaterThan(7);
      expect(point.lng).toBeLessThan(16);
      expect(point.lat).toBeGreaterThan(54.5);
      expect(point.lat).toBeLessThan(58);
    }
  });
});
