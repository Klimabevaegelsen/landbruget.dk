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
      description: `Se et estimat af pesticidbelastningen nær ${addr}. Modellerede tal baseret på offentlige data fra ca. 92 % af danske landbrugsmarker.`,
      openGraph: {
        title,
        description: `Se et estimat af pesticidbelastningen nær ${addr}. Modellerede tal – kan afvige fra faktisk sprøjtning.`,
        type: 'website',
        siteName: 'Pesticidkortet',
        images: [{ url: ogImage, width: 1200, height: 630 }],
      },
    };
  }

  return {
    title: 'Pesticidkortet — Hvad kan være sprøjtet i dit nærområde?',
    description:
      'Se et estimat af, hvilke pesticider der er rapporteret brugt nær din adresse. Modellerede tal baseret på offentlige data fra ca. 92 % af danske landbrugsmarker.',
    openGraph: {
      title: 'Pesticidkortet — Hvad kan være sprøjtet i dit nærområde?',
      description:
        'Se et estimat af, hvilke pesticider der er rapporteret brugt nær din adresse. Modellerede tal – kan afvige fra faktisk sprøjtning.',
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
