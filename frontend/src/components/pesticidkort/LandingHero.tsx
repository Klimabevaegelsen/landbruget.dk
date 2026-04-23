'use client';

import Image from 'next/image';
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
      className="relative flex min-h-screen flex-col overflow-hidden px-6 pb-[var(--pesticidkort-footer-height)] sm:px-8"
    >
      {/* Map background */}
      <div className="absolute inset-0 -z-10" aria-hidden="true">
        <Image
          src="/images/pesticidkort-bg.webp"
          alt=""
          fill
          className="object-cover"
          priority
        />
        <div className="from-background/85 via-background/80 to-background/70 absolute inset-0 bg-gradient-to-b" />
      </div>

      <div className="mx-auto flex w-full max-w-2xl flex-1 flex-col justify-center py-16">
        <motion.p
          variants={fadeSlideUp}
          className="text-muted-foreground mb-4 text-sm font-medium tracking-[0.14em] uppercase"
        >
          Estimater baseret på offentlige data fra ca. 92 % af danske
          landbrugsmarker
        </motion.p>

        <motion.h1
          variants={fadeSlideUp}
          className="font-display text-foreground text-4xl leading-[1.05] font-semibold tracking-tight sm:text-5xl lg:text-[3.75rem]"
        >
          Hvad kan være sprøjtet
          <br />
          <span className="text-primary">tæt på dit hjem?</span>
        </motion.h1>

        <motion.p
          variants={fadeSlideUp}
          className="text-muted-foreground mt-3 max-w-md text-lg leading-relaxed"
        >
          Over 4 millioner danskere bor inden for 1 km af en landbrugsmark. Se
          et estimat af, hvilke pesticider der er rapporteret brugt nær din
          adresse. Tallene er modellerede og kan afvige fra faktisk sprøjtning.
        </motion.p>

        <motion.div variants={fadeSlideUp} className="mt-8 max-w-lg">
          <AddressAutocomplete onSelect={onAddressSelect} />
        </motion.div>

        <motion.div
          variants={fadeSlideUp}
          className="mt-6 flex flex-wrap items-center gap-x-3 gap-y-2 text-sm"
        >
          <button
            onClick={onExploreMap}
            data-testid="explore-map-button"
            className="text-primary flex min-h-[44px] items-center font-medium underline-offset-4 hover:underline"
          >
            Udforsk kortet
          </button>
          <span className="text-border hidden sm:block">·</span>
          <button
            onClick={() => router.push('/markanalyse')}
            data-testid="go-expert-button"
            className="text-muted-foreground flex min-h-[44px] items-center underline-offset-4 hover:underline"
          >
            Ekspertvisning
          </button>
          <span className="text-border hidden sm:block">·</span>
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
