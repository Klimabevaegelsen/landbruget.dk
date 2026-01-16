'use client';

import React from 'react';
import {
  Drawer,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
  DrawerDescription,
} from '@/components/ui/drawer';
import { ScrollArea } from '@/components/ui/scroll-area';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { FieldDetailsContent } from '../shared/field-details-content';

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
    <Drawer open={isOpen} onOpenChange={onClose}>
      <DrawerContent className="max-h-[85vh]">
        <DrawerHeader>
          <DrawerTitle>Markdetaljer</DrawerTitle>
          <DrawerDescription>
            {field.crop_name || 'Ukendt afgrøde'}
          </DrawerDescription>
        </DrawerHeader>
        <ScrollArea className="flex-1 overflow-auto">
          <div className="p-4">
            <FieldDetailsContent field={field} />
          </div>
        </ScrollArea>
      </DrawerContent>
    </Drawer>
  );
}
