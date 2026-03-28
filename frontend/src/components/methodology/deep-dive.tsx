'use client';

import { type ReactNode } from 'react';
import { cn } from '@/lib/utils';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';

interface DeepDiveProps {
  title: string;
  children: ReactNode;
  className?: string;
  testId?: string;
}

export function DeepDive({
  title,
  children,
  className,
  testId,
}: DeepDiveProps) {
  return (
    <Accordion type="single" collapsible className={cn('my-6', className)}>
      <AccordionItem
        value="detail"
        className="border-border bg-card rounded border px-4"
      >
        <AccordionTrigger
          className="text-muted-foreground hover:text-foreground text-[13px] hover:no-underline"
          data-testid={testId ?? 'deep-dive-trigger'}
        >
          {title}
        </AccordionTrigger>
        <AccordionContent className="text-foreground/80 text-[14px] leading-relaxed">
          {children}
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}
