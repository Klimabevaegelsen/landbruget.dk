import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Markanalyse - Omfattende Landbrugsdata',
  description:
    'Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger',
  keywords: [
    'landbrugsmarker',
    'pesticidforbrug',
    'BNBO',
    'lavbundsjorder',
    'tørv',
    'miljøanalyse',
    'Danmark',
  ],
  openGraph: {
    title: 'Markanalyse - Omfattende Landbrugsdata',
    description:
      'Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger',
    type: 'website',
  },
};

interface MarkanalyseLayoutProps {
  children: React.ReactNode;
}

export default function MarkanalyseLayout({
  children,
}: MarkanalyseLayoutProps) {
  return children;
}
