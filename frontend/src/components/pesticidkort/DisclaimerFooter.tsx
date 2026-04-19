'use client';

import { useState } from 'react';
import Link from 'next/link';
import { InfoIcon, XIcon } from 'lucide-react';
import { cn } from '@/lib/utils';

const LINKS: { href: string; id: string; label: string }[] = [
  { href: '/pesticidanalyse/metode', id: 'metode', label: 'Om metoden' },
  { href: '/kilder', id: 'kilder', label: 'Kilder' },
  { href: '/brugsvilkaar', id: 'brugsvilkaar', label: 'Brugsvilkår' },
  {
    href: '/privatlivspolitik',
    id: 'privatlivspolitik',
    label: 'Privatlivspolitik',
  },
];

export function DisclaimerFooter() {
  const [expanded, setExpanded] = useState(false);

  return (
    <div
      data-testid="pesticidkort-disclaimer-footer"
      className="bg-background/90 border-border/60 pointer-events-auto fixed right-0 bottom-0 left-0 z-40 border-t backdrop-blur-sm"
    >
      <div className="mx-auto flex max-w-5xl flex-col gap-2 px-4 py-2 text-xs sm:flex-row sm:items-center sm:justify-between sm:gap-4">
        <p
          className={cn(
            'text-muted-foreground leading-snug',
            !expanded && 'hidden sm:block'
          )}
        >
          Beregnede data baseret på offentlige kilder – værdier er estimater og
          kan afvige fra faktisk sprøjtning.
        </p>

        <div className="hidden items-center gap-3 sm:flex">
          {LINKS.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="text-muted-foreground hover:text-foreground underline-offset-2 hover:underline"
              data-testid={`pesticidkort-disclaimer-link-${link.id}`}
            >
              {link.label}
            </Link>
          ))}
        </div>

        <div className="flex items-center justify-between sm:hidden">
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="text-muted-foreground flex items-center gap-1"
            data-testid="pesticidkort-disclaimer-toggle"
            aria-expanded={expanded}
            aria-label={expanded ? 'Skjul forbehold' : 'Vis forbehold og links'}
          >
            {expanded ? (
              <XIcon className="size-3" />
            ) : (
              <InfoIcon className="size-3" />
            )}
            {expanded ? 'Skjul' : 'Forbehold & vilkår'}
          </button>
          {expanded && (
            <div className="flex flex-wrap justify-end gap-x-3 gap-y-1">
              {LINKS.map((link) => (
                <Link
                  key={link.href}
                  href={link.href}
                  className="text-muted-foreground hover:text-foreground underline-offset-2 hover:underline"
                  data-testid={`pesticidkort-disclaimer-mlink-${link.id}`}
                >
                  {link.label}
                </Link>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
