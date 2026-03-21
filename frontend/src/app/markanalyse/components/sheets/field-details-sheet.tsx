'use client';

import React from 'react';
import { Sheet, SheetContent } from '@/components/ui/sheet';
import { ScrollArea } from '@/components/ui/scroll-area';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { FieldDetailsContent } from '@/app/markanalyse/components/shared/field-details-content';

interface FieldDetailsSheetProps {
  field: FieldAnalysisData | null;
  isOpen: boolean;
  onClose: () => void;
}

export function FieldDetailsSheet({
  field,
  isOpen,
  onClose,
}: FieldDetailsSheetProps) {
  if (!field) return null;

  return (
    <Sheet open={isOpen} onOpenChange={onClose}>
      <SheetContent
        side="bottom"
        title="Markdetaljer"
        description={field.crop_name || 'Ukendt afgrøde'}
        className="h-[85vh]"
      >
        <ScrollArea className="h-full">
          <div className="p-4">
            {/* Mobile swipe indicator */}
            <div className="bg-border mx-auto mb-4 h-1 w-12 rounded-full"></div>

            <FieldDetailsContent field={field} />
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  );
}
