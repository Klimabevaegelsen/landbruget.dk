import { Container } from '@/components/layout/container';
import Hero from '@/components/page-sections/hero';
import AllRankings from '@/components/homepage/AllRankings';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Landbruget.dk',
  description: 'Dansk landbrugsdata - samlet ét sted',
};

export default function Home() {
  return (
    <div>
      <Container variant="hero">
        <Hero />
      </Container>
      <Container className="py-12">
        <AllRankings />
      </Container>
    </div>
  );
}
