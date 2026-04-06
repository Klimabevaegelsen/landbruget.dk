'use client';

import { Marker } from '@vis.gl/react-maplibre';
import { motion } from 'motion/react';

interface DisaggPulsingLabelProps {
  lng: number;
  lat: number;
}

export function DisaggPulsingLabel({ lng, lat }: DisaggPulsingLabelProps) {
  return (
    <Marker longitude={lng} latitude={lat} anchor="center">
      <motion.div
        animate={{ scale: [1, 1.06, 1] }}
        transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
        className="bg-card/95 border-primary pointer-events-none rounded-lg border-2 px-3 py-1.5 text-center shadow-md backdrop-blur-sm"
        data-testid="pulsing-total-label"
      >
        <p className="text-foreground text-xs font-bold">Total: 38,47 ha</p>
        <p className="text-primary text-[10px] font-semibold">0 % afvigelse</p>
      </motion.div>
    </Marker>
  );
}
