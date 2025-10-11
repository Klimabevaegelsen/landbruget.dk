import Link from 'next/link';
import { Container } from '../container';
import { Logo } from './logo';

export function Footer() {
  return (
    <div className="bg-primary-foreground">
      <Container variant="nav">
        <div className="flex flex-col items-center justify-between gap-4 py-6 lg:flex-row">
          <Logo className="h-[26px]" />

          {/* Navigation Links */}
          <div className="flex flex-col items-center gap-2 lg:flex-row lg:gap-6">
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-2 text-center">
              <Link
                className="touch-target flex min-h-[44px] min-w-[44px] items-center justify-center px-3 py-2 text-sm font-medium hover:underline"
                href="/om-os"
              >
                Om Landbruget.dk
              </Link>
              <Link
                className="touch-target flex min-h-[44px] min-w-[44px] items-center justify-center px-3 py-2 text-sm font-medium hover:underline"
                href="/brugsvilkaar"
              >
                Brugsvilkår
              </Link>
              <Link
                className="touch-target flex min-h-[44px] min-w-[44px] items-center justify-center px-3 py-2 text-sm font-medium hover:underline"
                href="/privatlivspolitik"
              >
                Privatlivspolitik
              </Link>
            </div>
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-2 text-center">
              <Link
                className="touch-target flex min-h-[44px] min-w-[44px] items-center justify-center px-3 py-2 text-sm font-medium hover:underline"
                href="/kilder"
              >
                Kilder
              </Link>
              <a
                className="touch-target flex min-h-[44px] min-w-[44px] items-center justify-center px-3 py-2 text-sm font-medium hover:underline"
                href="mailto:info@landbruget.dk"
              >
                info@landbruget.dk
              </a>
            </div>
          </div>

          <p className="text-sm">© Copyright 2025 Landbruget.dk</p>
        </div>
      </Container>
    </div>
  );
}
