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
    <Accordion type="single" collapsible className={cn('my-8', className)}>
      <AccordionItem
        value="detail"
        className="border-primary/20 bg-primary/[0.03] rounded border px-4"
      >
        <AccordionTrigger
          className="text-primary text-[15px] font-medium hover:no-underline"
          data-testid={testId ?? 'deep-dive-trigger'}
        >
          {title}
        </AccordionTrigger>
        <AccordionContent className="text-foreground/85 text-[16px] leading-relaxed">
          {children}
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}
