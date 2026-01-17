import { CheckBadgeIcon } from '@heroicons/react/24/outline';
import { Container } from '../container';
import Link from 'next/link';

export function NavBanner() {
  return (
    <div className="w-full">
      {/* Original Nav Banner */}
      <div className="bg-primary-foreground h-10 w-full">
        <Container variant="nav" className="h-full" subclassName="h-full">
          <div className="flex size-full justify-center md:justify-between">
            <div className="flex gap-x-6">
              <p className="flex items-center gap-x-1 text-xs font-bold">
                <CheckBadgeIcon strokeWidth={2} className="size-4" />
                Fri adgang og{' '}
                <a
                  href="https://github.com/klimabevaegelsen/landbruget.dk/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="touch-target inline-flex min-h-[44px] min-w-[44px] items-center justify-center px-2 py-1 underline hover:no-underline"
                >
                  open source
                </a>
              </p>
              <p className="flex items-center gap-x-1 text-xs font-bold">
                <CheckBadgeIcon strokeWidth={2} className="size-4" />
                Valideret data
              </p>
              <p className="hidden items-center gap-x-1 text-xs font-bold md:flex">
                <CheckBadgeIcon strokeWidth={2} className="size-4" />
                Månedlig opdatering af data
              </p>
            </div>
            <div className="hidden gap-x-6 md:flex">
              <Link
                href="/kilder"
                className="flex items-center text-xs font-medium hover:underline"
              >
                Kilder
              </Link>
              <Link
                href="/om-os"
                className="flex items-center text-xs font-medium hover:underline"
              >
                Om landbruget.dk
              </Link>
            </div>
          </div>
        </Container>
      </div>
    </div>
  );
}
