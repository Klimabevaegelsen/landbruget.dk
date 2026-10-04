import type { AddressResult } from '@/components/pesticidkort/types';

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null;
}

function getObject(value: unknown, key: string): JsonObject | null {
  if (!isObject(value)) return null;
  const nested = value[key];
  return isObject(nested) ? nested : null;
}

function finiteNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getProjectedCoordinates(
  data: unknown
): { x: number; y: number } | null {
  const root = isObject(data) ? data : null;
  const selectedAddress = root?.husnummer ?? root?.adresse;
  const address = isObject(selectedAddress) ? selectedAddress : null;
  const houseNumber = getObject(address, 'husnummer') ?? address;
  const accessPoint = getObject(houseNumber, 'adgangspunkt');
  const coordinates = getObject(accessPoint, 'koordinater');
  const x = finiteNumber(coordinates?.x);
  const y = finiteNumber(coordinates?.y);
  if (x !== null && y !== null) return { x, y };

  const geometry = getObject(accessPoint, 'geometri');
  const pair = geometry?.coordinates;
  if (!Array.isArray(pair) || pair.length < 2) return null;
  const geometryX = finiteNumber(pair[0]);
  const geometryY = finiteNumber(pair[1]);
  if (geometryX === null || geometryY === null) return null;
  return { x: geometryX, y: geometryY };
}

/** Convert ETRS89 / UTM zone 32N (EPSG:25832) to WGS84 longitude/latitude. */
function utm32ToWgs84(x: number, y: number): { lat: number; lng: number } {
  const semiMajorAxis = 6378137;
  const flattening = 1 / 298.257222101;
  const eccentricitySquared = flattening * (2 - flattening);
  const secondEccentricitySquared =
    eccentricitySquared / (1 - eccentricitySquared);
  const scale = 0.9996;
  const e1 =
    (1 - Math.sqrt(1 - eccentricitySquared)) /
    (1 + Math.sqrt(1 - eccentricitySquared));
  const xFromOrigin = x - 500000;
  const meridionalArc = y / scale;
  const mu =
    meridionalArc /
    (semiMajorAxis *
      (1 -
        eccentricitySquared / 4 -
        (3 * eccentricitySquared ** 2) / 64 -
        (5 * eccentricitySquared ** 3) / 256));

  const footprintLatitude =
    mu +
    ((3 * e1) / 2 - (27 * e1 ** 3) / 32) * Math.sin(2 * mu) +
    ((21 * e1 ** 2) / 16 - (55 * e1 ** 4) / 32) * Math.sin(4 * mu) +
    ((151 * e1 ** 3) / 96) * Math.sin(6 * mu) +
    ((1097 * e1 ** 4) / 512) * Math.sin(8 * mu);

  const sinLatitude = Math.sin(footprintLatitude);
  const cosLatitude = Math.cos(footprintLatitude);
  const tanLatitude = Math.tan(footprintLatitude);
  const radiusPrimeVertical =
    semiMajorAxis / Math.sqrt(1 - eccentricitySquared * sinLatitude ** 2);
  const radiusMeridian =
    (semiMajorAxis * (1 - eccentricitySquared)) /
    (1 - eccentricitySquared * sinLatitude ** 2) ** 1.5;
  const tangentSquared = tanLatitude ** 2;
  const c = secondEccentricitySquared * cosLatitude ** 2;
  const d = xFromOrigin / (radiusPrimeVertical * scale);

  const latitude =
    footprintLatitude -
    ((radiusPrimeVertical * tanLatitude) / radiusMeridian) *
      (d ** 2 / 2 -
        ((5 +
          3 * tangentSquared +
          10 * c -
          4 * c ** 2 -
          9 * secondEccentricitySquared) *
          d ** 4) /
          24 +
        ((61 +
          90 * tangentSquared +
          298 * c +
          45 * tangentSquared ** 2 -
          252 * secondEccentricitySquared -
          3 * c ** 2) *
          d ** 6) /
          720);
  const longitude =
    (9 * Math.PI) / 180 +
    (d -
      ((1 + 2 * tangentSquared + c) * d ** 3) / 6 +
      ((5 -
        2 * c +
        28 * tangentSquared -
        3 * c ** 2 +
        8 * secondEccentricitySquared +
        24 * tangentSquared ** 2) *
        d ** 5) /
        120) /
      cosLatitude;

  return {
    lat: (latitude * 180) / Math.PI,
    lng: (longitude * 180) / Math.PI,
  };
}

export function addressResultFromDetails(
  data: unknown,
  fallbackLabel: string
): AddressResult | null {
  const coordinates = getProjectedCoordinates(data);
  if (!coordinates) return null;

  const { lat, lng } = utm32ToWgs84(coordinates.x, coordinates.y);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  const root = isObject(data) ? data : null;
  const address = root?.husnummer ?? root?.adresse;
  let label = fallbackLabel;
  if (isObject(address)) {
    if (typeof address.adgangsadressebetegnelse === 'string') {
      label = address.adgangsadressebetegnelse;
    } else if (typeof address.adressebetegnelse === 'string') {
      label = address.adressebetegnelse;
    }
  }

  return { lat, lng, address: label };
}
