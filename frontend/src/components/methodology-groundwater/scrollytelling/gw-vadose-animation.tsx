'use client';

import { Marker } from '@vis.gl/react-maplibre';
import { motion, AnimatePresence } from 'motion/react';
import {
  ESPE_WELL,
  VEJEN_WELL,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

interface GwVadoseAnimationProps {
  variant: 'bentazon' | 'mcpa';
  visible: boolean;
}

const PARTICLE_COUNT = 4;

/** Bentazon: fast (1.5yr), shallow (6m). MCPA: slow (9yr), deep (19.3m). */
function getConfig(variant: 'bentazon' | 'mcpa') {
  if (variant === 'bentazon') {
    return {
      duration: 2,
      lineHeight: 100,
      label: '6 m',
      color: '#ef4444',
      lng: ESPE_WELL.lng,
      lat: ESPE_WELL.lat,
    };
  }
  return {
    duration: 5,
    lineHeight: 160,
    label: '19,3 m',
    color: '#f59e0b',
    lng: VEJEN_WELL.lng,
    lat: VEJEN_WELL.lat,
  };
}

export function GwVadoseAnimation({
  variant,
  visible,
}: GwVadoseAnimationProps) {
  const config = getConfig(variant);
  const particles = Array.from({ length: PARTICLE_COUNT }, (_, i) => i);

  return (
    <AnimatePresence>
      {visible && (
        <Marker longitude={config.lng} latitude={config.lat} anchor="top">
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.5 }}
            className="pointer-events-none"
            data-testid="vadose-animation"
          >
            <svg
              width="40"
              height={config.lineHeight + 40}
              viewBox={`0 0 40 ${config.lineHeight + 40}`}
            >
              {/* Vertical path line */}
              <line
                x1="20"
                y1="10"
                x2="20"
                y2={config.lineHeight + 10}
                stroke={config.color}
                strokeWidth="1.5"
                strokeDasharray="4 3"
                opacity="0.5"
              />
              {/* Animated particles */}
              {particles.map((i) => (
                <motion.circle
                  key={i}
                  cx="20"
                  r="3.5"
                  fill={config.color}
                  opacity="0.8"
                  animate={{ cy: [10, config.lineHeight + 10] }}
                  transition={{
                    duration: config.duration,
                    repeat: Infinity,
                    delay: (i / PARTICLE_COUNT) * config.duration,
                    ease: 'linear',
                  }}
                />
              ))}
            </svg>
            {/* Depth label */}
            <p className="text-muted-foreground mt-1 text-center text-[10px] font-medium">
              {config.label}
            </p>
          </motion.div>
        </Marker>
      )}
    </AnimatePresence>
  );
}
