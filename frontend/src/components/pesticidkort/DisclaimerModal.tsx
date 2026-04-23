'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

const STORAGE_KEY = 'pesticidkort-disclaimer-v1';

const BULLETS: string[] = [
  'Data kommer fra offentlige registre (bl.a. Miljøstyrelsen og Landbrugsstyrelsen).',
  'Tallene er modellerede – faktisk sprøjtning på en konkret mark kan afvige fra det viste.',
  'Brug kortet som udgangspunkt for dialog og oplysning, ikke som endelig dokumentation.',
];

export function DisclaimerModal() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    setOpen(!window.localStorage.getItem(STORAGE_KEY));
  }, []);

  const handleAccept = () => {
    window.localStorage.setItem(STORAGE_KEY, new Date().toISOString());
    setOpen(false);
  };

  return (
    <Dialog open={open} onOpenChange={(next) => !next && handleAccept()}>
      <DialogContent
        data-testid="pesticidkort-disclaimer-modal"
        className="sm:max-w-md"
      >
        <DialogHeader>
          <DialogTitle>Sådan skal du læse kortet</DialogTitle>
          <DialogDescription>
            Pesticidkortet viser modellerede estimater baseret på offentlige
            data. Før du bruger kortet, er det vigtigt at vide:
          </DialogDescription>
        </DialogHeader>

        <ul className="text-foreground list-disc space-y-2 pl-5 text-sm leading-relaxed">
          {BULLETS.map((bullet) => (
            <li key={bullet}>{bullet}</li>
          ))}
        </ul>

        <DialogFooter className="sm:justify-between">
          <Link
            href="/pesticidanalyse/metode"
            className="text-primary self-center text-sm underline-offset-4 hover:underline"
            data-testid="pesticidkort-disclaimer-methodology"
          >
            Læs mere om metoden
          </Link>
          <Button
            onClick={handleAccept}
            data-testid="pesticidkort-disclaimer-accept"
          >
            Jeg forstår
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
