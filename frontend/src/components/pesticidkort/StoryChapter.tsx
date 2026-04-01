'use client';

import { useRef } from 'react';
import { motion, useInView } from 'framer-motion';

interface StoryChapterProps {
  index: number;
  title: string;
  body: string;
  onEnterView: () => void;
}

export function StoryChapter({
  index,
  title,
  body,
  onEnterView,
}: StoryChapterProps) {
  const ref = useRef<HTMLDivElement>(null);
  const isInView = useInView(ref, { amount: 0.5, once: false });

  if (isInView) {
    onEnterView();
  }

  return (
    <div
      ref={ref}
      data-testid={`story-chapter-${index}`}
      className="flex min-h-screen items-center"
    >
      <motion.div
        initial={{ opacity: 0, y: 30 }}
        animate={isInView ? { opacity: 1, y: 0 } : { opacity: 0.2, y: 0 }}
        transition={{ duration: 0.5, ease: [0.25, 0.1, 0.25, 1] }}
        className="max-w-sm px-6 py-16 md:px-10"
      >
        <p className="text-muted-foreground mb-2 text-xs font-medium tracking-widest uppercase">
          {index + 1} / 5
        </p>
        <h2 className="text-foreground text-2xl leading-tight font-bold tracking-tight md:text-3xl">
          {title}
        </h2>
        <p className="text-muted-foreground mt-4 text-base leading-relaxed">
          {body}
        </p>
      </motion.div>
    </div>
  );
}
