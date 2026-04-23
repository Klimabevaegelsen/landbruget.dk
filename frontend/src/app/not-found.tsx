import Link from 'next/link';
import { Container } from '@/components/layout/container';
import { Button } from '@/components/ui/button';

export default function NotFound() {
  return (
    <Container section>
      <div className="mx-auto max-w-2xl py-16 text-center">
        <p className="text-muted-foreground text-sm font-semibold tracking-[0.2em] uppercase">
          404
        </p>
        <h1 className="mt-4 text-3xl font-bold">Siden blev ikke fundet</h1>
        <p className="text-muted-foreground mt-4">
          Indholdet findes ikke eller er blevet flyttet.
        </p>
        <div className="mt-8">
          <Button asChild>
            <Link href="/">Tilbage til forsiden</Link>
          </Button>
        </div>
      </div>
    </Container>
  );
}
