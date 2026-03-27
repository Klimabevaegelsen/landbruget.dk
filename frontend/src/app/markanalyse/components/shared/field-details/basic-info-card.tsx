'use client';

import React from 'react';
import { Leaf } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';

interface BasicInfoCardProps {
  field: FieldAnalysisData;
}

export function BasicInfoCard({ field }: BasicInfoCardProps) {
  return (
    <Card className="p-4 lg:p-6">
      <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
        Grundoplysninger
      </h3>
      <div className="space-y-3 text-sm lg:text-base">
        <div className="flex justify-between">
          <span className="text-muted-foreground">Kommune:</span>
          <span className="font-medium">{field.kommune}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">CVR:</span>
          <span className="font-mono text-xs lg:text-sm">
            {field.cvr_number}
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Areal:</span>
          <span className="font-medium">
            {formatNumber(field.area_hectares) || '0'} ha
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Afgrøde:</span>
          <span className="font-medium">{field.crop_name || 'Ukendt'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Økologisk:</span>
          <div className="flex items-center gap-2">
            {field.is_organic ? (
              <Badge variant="default" className="bg-organic text-white">
                <Leaf className="mr-1 h-3 w-3" />
                Ja
              </Badge>
            ) : (
              <span className="text-muted-foreground">Nej</span>
            )}
          </div>
        </div>
      </div>
    </Card>
  );
}
