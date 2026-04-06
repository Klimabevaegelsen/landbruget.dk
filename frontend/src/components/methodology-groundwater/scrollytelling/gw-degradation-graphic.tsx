'use client';

import { motion, AnimatePresence } from 'motion/react';

interface GwDegradationGraphicProps {
  variant: 'bentazon' | 'mcpa';
  visible: boolean;
}

const BENTAZON_STEPS = [
  {
    label: 'Fighter 480',
    sub: 'pesticid',
    style: 'muted' as const,
  },
  {
    label: 'Bentazon',
    sub: 'Koc ~34 · høj mobilitet',
    style: 'primary' as const,
  },
  {
    label: 'Detekteret i grundvand',
    sub: 'persistent — ingen signifikant nedbrydning',
    style: 'destructive' as const,
  },
];

const MCPA_STEPS = [
  {
    label: 'U46 M / Agroxone',
    sub: 'pesticid',
    style: 'muted' as const,
  },
  {
    label: 'MCPA',
    sub: 'DT₅₀ ~25 dage · hurtig nedbrydning',
    style: 'primary' as const,
  },
  {
    label: '4-chlor-2-methylphenol',
    sub: 'metabolit via hydrolyse (etherbinding)',
    style: 'accent' as const,
  },
  {
    label: 'Detekteret i grundvand',
    sub: 'mobil + persistent metabolit',
    style: 'destructive' as const,
  },
];

const CONNECTORS: Record<string, string[]> = {
  bentazon: ['aktivstof', 'persistent'],
  mcpa: ['aktivstof', 'nedbrydning ↓', 'transport'],
};

const PILL_CLASSES: Record<string, string> = {
  muted: 'bg-muted text-muted-foreground',
  primary: 'bg-primary/15 text-primary border-primary/30',
  accent: 'bg-amber-500/15 text-amber-700 border-amber-500/30',
  destructive: 'bg-destructive/15 text-destructive border-destructive/30',
};

export function GwDegradationGraphic({
  variant,
  visible,
}: GwDegradationGraphicProps) {
  const steps = variant === 'bentazon' ? BENTAZON_STEPS : MCPA_STEPS;
  const connectors = CONNECTORS[variant];

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.4 }}
          className="bg-card/90 w-52 rounded-lg border p-3 shadow-sm backdrop-blur-sm"
          data-testid="degradation-graphic"
        >
          <p className="text-muted-foreground mb-2 text-[9px] font-medium tracking-wider uppercase">
            Nedbrydningsvej
          </p>
          <div className="flex flex-col items-center gap-0">
            {steps.map((step, i) => (
              <motion.div
                key={step.label}
                initial={{ opacity: 0, y: 6 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: i * 0.3, duration: 0.4 }}
                className="flex w-full flex-col items-center"
              >
                {i > 0 && (
                  <div className="flex flex-col items-center py-1">
                    <motion.div
                      initial={{ scaleY: 0 }}
                      animate={{ scaleY: 1 }}
                      transition={{ delay: i * 0.3 - 0.15, duration: 0.3 }}
                      className="bg-border h-3 w-px origin-top"
                    />
                    <span className="text-muted-foreground text-[8px]">
                      {connectors[i - 1]}
                    </span>
                    <span className="text-muted-foreground text-[10px]">▼</span>
                  </div>
                )}
                <div
                  className={`w-full rounded-md border px-2 py-1.5 text-center ${PILL_CLASSES[step.style]}`}
                >
                  <p className="text-[11px] leading-tight font-semibold">
                    {step.label}
                  </p>
                  <p className="mt-0.5 text-[9px] leading-tight opacity-75">
                    {step.sub}
                  </p>
                </div>
              </motion.div>
            ))}
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
