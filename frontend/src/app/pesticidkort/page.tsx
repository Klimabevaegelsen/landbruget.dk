/* oxlint-disable landbruget/require-test-coverage */
import { Suspense } from 'react';
import { PesticidkortApp } from '@/components/pesticidkort/PesticidkortApp';
import type { Metadata } from 'next';

interface PageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export async function generateMetadata({
  searchParams,
}: PageProps): Promise<Metadata> {
  const params = await searchParams;
  const addr = typeof params.addr === 'string' ? params.addr : null;
  const year = typeof params.y === 'string' ? params.y : null;

  if (addr) {
    const title = `Pesticidkort — ${addr}${year ? ` (${year})` : ''}`;
    const ogParams = new URLSearchParams({ addr });
    const ogImage = `/api/og/pesticidkort?${ogParams.toString()}`;
    return {
      title,
      description: `Se pesticidbelastningen nær ${addr}. Baseret på offentlige data fra 92% af alle danske marker.`,
      openGraph: {
        title,
        description: `Se pesticidbelastningen nær ${addr}.`,
        type: 'website',
        siteName: 'Pesticidkortet',
        images: [{ url: ogImage, width: 1200, height: 630 }],
      },
    };
  }

  return {
    title: 'Pesticidkortet — Hvad sprøjtes i dit nærområde?',
    description:
      'Se hvilke pesticider der bruges tæt på din adresse. Baseret på offentlige data fra 92% af alle danske landbrugsmarker.',
    openGraph: {
      title: 'Pesticidkortet — Hvad sprøjtes i dit nærområde?',
      description: 'Se hvilke pesticider der bruges tæt på din adresse.',
      type: 'website',
      siteName: 'Pesticidkortet',
    },
  };
}

export default function PesticidkortPage() {
  return (
    <Suspense>
      <PesticidkortApp />
    </Suspense>
  );
}
