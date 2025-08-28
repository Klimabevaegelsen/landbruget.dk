import { Container } from '@/components/layout/container';
import Hero from '@/components/page-sections/hero';
import HomepageRankings from '@/components/homepage/HomepageRankings';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Landbruget.dk',
  description: 'Dansk landbrugsdata - samlet ét sted',
};

export default function Home() {
  return (
    <div>
      <Container className="bg-primary-darker">
        <Hero />
      </Container>
      <Container className="py-12">
        <HomepageRankings />
      </Container>
    </div>
  );
}
