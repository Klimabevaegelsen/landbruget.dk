'use client';

import { useRouter } from 'next/navigation';
import { motion, useReducedMotion } from 'framer-motion';
import { AddressAutocomplete } from '@/components/pesticidkort/AddressAutocomplete';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import {
  fadeSlideUp,
  staggerContainer,
} from '@/components/pesticidkort/motion-config';
import type { AddressResult } from '@/components/pesticidkort/types';

interface LandingHeroProps {
  onAddressSelect: (result: AddressResult) => void;
  onExploreMap: () => void;
  year: number;
  onYearChange: (year: number) => void;
}

export function LandingHero({
  onAddressSelect,
  onExploreMap,
  year,
  onYearChange,
}: LandingHeroProps) {
  const router = useRouter();
  const reducedMotion = useReducedMotion();

  return (
    <motion.main
      variants={staggerContainer(!!reducedMotion)}
      initial={reducedMotion ? false : 'hidden'}
      animate="visible"
      className="bg-background relative flex min-h-screen flex-col overflow-hidden px-6 sm:px-8"
    >
      <div
        aria-hidden="true"
        className="pointer-events-none absolute inset-0 -z-10"
      >
        <div className="bg-primary/20 absolute top-[-12rem] left-1/2 h-[28rem] w-[28rem] -translate-x-1/2 rounded-full blur-3xl" />
        <div className="bg-warning/20 absolute right-[-8rem] bottom-[-8rem] h-[22rem] w-[22rem] rounded-full blur-3xl" />
        <div className="bg-info/15 absolute top-1/3 left-[-10rem] h-[20rem] w-[20rem] rounded-full blur-3xl" />
      </div>
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
          className="text-muted-foreground mt-5 max-w-md text-lg leading-relaxed"
        >
          Over 1,2 millioner danskere bor inden for 1 km af marker, der sprøjtes
          med pesticider. Se hvad der sprøjtes tæt på din adresse.
        </motion.p>

        <motion.div variants={fadeSlideUp} className="mt-8 max-w-lg">
          <AddressAutocomplete onSelect={onAddressSelect} />
        </motion.div>

        <motion.div variants={fadeSlideUp} className="mt-6 max-w-lg">
          <YearTimeline year={year} onChange={onYearChange} />
        </motion.div>

        <motion.button
          variants={fadeSlideUp}
          onClick={onExploreMap}
          data-testid="explore-map-button"
          className="text-primary mt-2 flex min-h-[44px] w-fit items-center text-sm font-medium underline-offset-4 hover:underline"
        >
          Eller udforsk kortet direkte
        </motion.button>

        <motion.button
          variants={fadeSlideUp}
          onClick={() => router.push('/markanalyse')}
          data-testid="go-expert-button"
          className="text-muted-foreground mt-5 flex min-h-[44px] w-fit items-center text-sm underline-offset-4 hover:underline"
        >
          Arbejder du professionelt med data? Gå til ekspertvisning
        </motion.button>
      </div>

      <motion.footer
        variants={fadeSlideUp}
        className="text-muted-foreground mx-auto w-full max-w-2xl border-t pt-6 pb-8"
      >
        <p className="text-xs leading-relaxed">
          Data fra Miljøstyrelsen, Landbrugsstyrelsen, Geodatastyrelsen og 18+
          andre offentlige kilder. Sidst opdateret 2023.
        </p>
      </motion.footer>
    </motion.main>
  );
}
