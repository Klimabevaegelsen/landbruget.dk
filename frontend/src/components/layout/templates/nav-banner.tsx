import { CheckBadgeIcon } from '@heroicons/react/24/outline';
import { Container } from '../container';
import { Tractor, Heart } from 'lucide-react';
import Link from 'next/link';

export function NavBanner() {
  return (
    <div className="w-full">
      {/* Maintenance Banner */}
      <div className="border-conventional/20 bg-conventional/10 w-full border-b py-3">
        <Container variant="nav">
          <div className="flex justify-center text-center">
            <p className="text-conventional text-sm font-medium">
              <Tractor className="mr-2 inline h-4 w-4" />
              <strong>Undskyld vi roder</strong>
              <span className="mx-2">•</span>
              Hjemmesiden er ved at blive gjort klar til lanceringen. Det
              betyder, at du vil opleve at noget data mangler eller er forkert,
              og at tingene ikke helt spiller. Vi tager imod feedback på{' '}
              <a
                href="https://github.com/Klimabevaegelsen/landbruget.dk/issues"
                target="_blank"
                rel="noopener noreferrer"
                className="touch-target inline-flex min-h-[44px] min-w-[44px] items-center justify-center px-2 py-1 font-semibold underline hover:no-underline"
              >
                Github
              </a>{' '}
              eller{' '}
              <a
                href="https://join.slack.com/t/landbrugetdk/shared_invite/zt-3bcf1whh0-mY6GqDGRhC0BuG3ADNLB2Q"
                target="_blank"
                rel="noopener noreferrer"
                className="touch-target inline-flex min-h-[44px] min-w-[44px] items-center justify-center px-2 py-1 font-semibold underline hover:no-underline"
              >
                Slack
              </a>{' '}
              med kyshånd <Heart className="ml-1 inline h-4 w-4" />
            </p>
          </div>
        </Container>
      </div>

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
