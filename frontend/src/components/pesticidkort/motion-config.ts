import type { Variants } from 'framer-motion';

const EASE_OUT_QUART: [number, number, number, number] = [0.25, 1, 0.5, 1];

export const fadeSlideUp: Variants = {
  hidden: { opacity: 0, y: 16 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.45, ease: EASE_OUT_QUART },
  },
};

export const fadeIn: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { duration: 0.35, ease: EASE_OUT_QUART },
  },
};

export function staggerContainer(reducedMotion: boolean): Variants {
  return {
    hidden: {},
    visible: {
      transition: reducedMotion
        ? {}
        : {
            delayChildren: 0.05,
            staggerChildren: 0.08,
          },
    },
  };
}
