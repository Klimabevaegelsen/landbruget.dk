'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { motion, useReducedMotion } from 'motion/react';
import { AddressAutocomplete } from '@/components/pesticidkort/AddressAutocomplete';
import {
  fadeSlideUp,
  staggerContainer,
} from '@/components/pesticidkort/motion-config';
import type { AddressResult } from '@/components/pesticidkort/types';

interface LandingHeroProps {
  onAddressSelect: (result: AddressResult) => void;
  onExploreMap: () => void;
}

export function LandingHero({
  onAddressSelect,
  onExploreMap,
}: LandingHeroProps) {
  const router = useRouter();
  const reducedMotion = useReducedMotion();

  return (
    <motion.main
      variants={staggerContainer(!!reducedMotion)}
      initial={reducedMotion ? false : 'hidden'}
      animate="visible"
      className="bg-primary/[0.02] relative flex min-h-screen flex-col overflow-hidden px-6 sm:px-8"
    >
      <div className="mx-auto flex w-full max-w-2xl flex-1 flex-col justify-center py-16">
        <motion.p
          variants={fadeSlideUp}
          className="text-muted-foreground mb-4 text-sm font-medium tracking-[0.14em] uppercase"
        >
          Baseret på offentlige data fra 92 % af alle danske marker
        </motion.p>

        <motion.h1
          variants={fadeSlideUp}
          className="font-display text-foreground text-4xl leading-[1.05] font-semibold tracking-tight sm:text-5xl lg:text-[3.75rem]"
        >
          Hvad sprøjtes
          <br />
          <span className="text-primary">tæt på dit hjem?</span>
        </motion.h1>

        <motion.p
          variants={fadeSlideUp}
          className="text-muted-foreground mt-3 max-w-md text-lg leading-relaxed"
        >
          Over 1,2 millioner danskere bor inden for 1 km af marker, der sprøjtes
          med pesticider. Se hvad der sprøjtes tæt på din adresse.
        </motion.p>

        <motion.div variants={fadeSlideUp} className="mt-8 max-w-lg">
          <AddressAutocomplete onSelect={onAddressSelect} />
        </motion.div>

        <motion.div
          variants={fadeSlideUp}
          className="mt-6 flex items-center gap-3 text-sm"
        >
          <button
            onClick={onExploreMap}
            data-testid="explore-map-button"
            className="text-primary flex min-h-[44px] items-center font-medium underline-offset-4 hover:underline"
          >
            Udforsk kortet
          </button>
          <span className="text-border">·</span>
          <button
            onClick={() => router.push('/markanalyse')}
            data-testid="go-expert-button"
            className="text-muted-foreground flex min-h-[44px] items-center underline-offset-4 hover:underline"
          >
            Ekspertvisning
          </button>
          <span className="text-border">·</span>
          <Link
            href="/pesticidanalyse/metode"
            data-testid="methodology-link"
            className="text-primary flex min-h-[44px] items-center font-medium underline-offset-4 hover:underline"
          >
            Om metoden
          </Link>
        </motion.div>
      </div>
    </motion.main>
  );
}
