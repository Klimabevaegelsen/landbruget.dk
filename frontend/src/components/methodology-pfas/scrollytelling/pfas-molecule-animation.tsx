'use client';

import { motion, AnimatePresence } from 'motion/react';
import type { PfasScrollyStepId } from './scrolly-constants';
import { EXAMPLE_FIELD } from './scrolly-constants';

interface PfasMoleculeAnimationProps {
  step: PfasScrollyStepId;
}

const PRODUCTS = EXAMPLE_FIELD.products;

const DISPERSE_OFFSETS = [
  { x: -80, y: -60 },
  { x: 60, y: -40 },
  { x: -40, y: 50 },
  { x: 80, y: 30 },
  { x: 0, y: -70 },
];

function ProductPills() {
  return (
    <div className="flex flex-col items-center gap-2">
      {PRODUCTS.map((p, i) => (
        <motion.div
          key={p.name}
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: i * 0.2, duration: 0.4 }}
          className="bg-card/90 flex items-center gap-2 rounded-full border px-3 py-1.5 shadow-sm backdrop-blur-sm"
        >
          <span className="text-foreground text-xs font-medium">{p.name}</span>
          <motion.span
            layoutId={`cf3-${i}`}
            className="text-destructive text-xs font-bold"
          >
            CF&#x2083;
          </motion.span>
        </motion.div>
      ))}
    </div>
  );
}

function TfaDispersal() {
  return (
    <div className="relative h-32 w-48">
      {DISPERSE_OFFSETS.map((offset, i) => (
        <motion.span
          key={i}
          initial={{ opacity: 0, x: 0, y: 0, scale: 0.5 }}
          animate={{ opacity: [0, 1, 0.7], x: offset.x, y: offset.y, scale: 1 }}
          transition={{ duration: 1.5, delay: i * 0.15, ease: 'easeOut' }}
          className="text-destructive absolute top-1/2 left-1/2 text-sm font-bold"
        >
          TFA
        </motion.span>
      ))}
    </div>
  );
}

export function PfasMoleculeAnimation({ step }: PfasMoleculeAnimationProps) {
  const show = step === 'byggeklodser' || step === 'evighed';
  if (!show) return null;

  return (
    <div
      className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center"
      data-testid="pfas-molecule-animation"
    >
      <AnimatePresence mode="wait">
        {step === 'byggeklodser' && (
          <motion.div
            key="pills"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <ProductPills />
          </motion.div>
        )}
        {step === 'evighed' && (
          <motion.div
            key="dispersal"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <TfaDispersal />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
