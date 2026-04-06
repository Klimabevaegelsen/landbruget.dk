'use client';

import { motion, AnimatePresence } from 'motion/react';

interface LegendItem {
  color: string;
  label: string;
  shape?: 'square' | 'circle';
}

interface MapLegendProps {
  items: LegendItem[];
  visible: boolean;
}

export function MapLegend({ items, visible }: MapLegendProps) {
  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.4 }}
          className="bg-card/90 rounded-md border px-2.5 py-2 shadow-sm backdrop-blur-sm"
          data-testid="map-legend"
        >
          <ul className="space-y-1">
            {items.map((item) => {
              const swatchStyle = { backgroundColor: item.color };
              return (
                <li
                  key={item.label}
                  className="flex items-center gap-1.5 text-[10px]"
                >
                  <span
                    className={
                      item.shape === 'circle'
                        ? 'inline-block h-2 w-2 rounded-full'
                        : 'inline-block h-2 w-2 rounded-sm'
                    }
                    style={swatchStyle}
                  />
                  <span className="text-muted-foreground">{item.label}</span>
                </li>
              );
            })}
          </ul>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
