const DEFAULT_R2_BASE_URL =
  'https://pub-b8c2f72ba51b4fe6804e9bb92280567c.r2.dev';

export function getR2BaseUrl(): string {
  return (
    process.env.NEXT_PUBLIC_R2_URL ||
    process.env.NEXT_PUBLIC_R2_BASE_URL ||
    DEFAULT_R2_BASE_URL
  );
}
