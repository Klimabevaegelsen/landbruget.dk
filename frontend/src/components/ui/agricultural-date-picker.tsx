'use client';

import { useState } from 'react';
import {
  format,
  subDays,
  startOfYear,
  endOfYear,
  startOfMonth,
  endOfMonth,
} from 'date-fns';
import { da } from 'date-fns/locale';
import { Calendar } from '@/components/ui/calendar';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Calendar as CalendarIcon,
  ChevronDown,
  Wheat,
  Droplets,
  TrendingUp,
  RotateCcw,
} from 'lucide-react';
import { cn } from '@/lib/utils';

export interface DateRange {
  from: Date | undefined;
  to: Date | undefined;
}

interface AgriculturalDatePickerProps {
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  className?: string;
  placeholder?: string;
  showPresets?: boolean;
  context?: 'pesticide' | 'harvest' | 'analysis' | 'general';
}

// Agricultural season presets
const agriculturalPresets = {
  pesticide: [
    {
      label: 'Sprøjtesæson 2024',
      description: 'Mar - Sep 2024',
      range: {
        from: new Date(2024, 2, 1),
        to: new Date(2024, 8, 30),
      },
      icon: <Droplets className="h-4 w-4" />,
    },
    {
      label: 'Forårssæson',
      description: 'Mar - Maj',
      range: {
        from: new Date(2024, 2, 1),
        to: new Date(2024, 4, 31),
      },
      icon: <Wheat className="h-4 w-4" />,
    },
    {
      label: 'Sommersæson',
      description: 'Jun - Aug',
      range: {
        from: new Date(2024, 5, 1),
        to: new Date(2024, 7, 31),
      },
      icon: <Wheat className="h-4 w-4" />,
    },
  ],
  harvest: [
    {
      label: 'Høstsæson 2024',
      description: 'Jul - Oct 2024',
      range: {
        from: new Date(2024, 6, 1),
        to: new Date(2024, 9, 31),
      },
      icon: <Wheat className="h-4 w-4" />,
    },
    {
      label: 'Vinterhvede høst',
      description: 'Jul - Aug',
      range: {
        from: new Date(2024, 6, 15),
        to: new Date(2024, 7, 31),
      },
      icon: <Wheat className="h-4 w-4" />,
    },
  ],
  analysis: [
    {
      label: 'Indeværende år',
      description: '2024',
      range: {
        from: startOfYear(new Date()),
        to: endOfYear(new Date()),
      },
      icon: <TrendingUp className="h-4 w-4" />,
    },
    {
      label: 'Sidste 12 måneder',
      description: 'Rullende år',
      range: {
        from: subDays(new Date(), 365),
        to: new Date(),
      },
      icon: <TrendingUp className="h-4 w-4" />,
    },
    {
      label: 'Indeværende måned',
      description: format(new Date(), 'MMMM yyyy', { locale: da }),
      range: {
        from: startOfMonth(new Date()),
        to: endOfMonth(new Date()),
      },
      icon: <TrendingUp className="h-4 w-4" />,
    },
  ],
  general: [
    {
      label: 'Sidste 7 dage',
      description: 'Seneste uge',
      range: {
        from: subDays(new Date(), 7),
        to: new Date(),
      },
      icon: <CalendarIcon className="h-4 w-4" />,
    },
    {
      label: 'Sidste 30 dage',
      description: 'Seneste måned',
      range: {
        from: subDays(new Date(), 30),
        to: new Date(),
      },
      icon: <CalendarIcon className="h-4 w-4" />,
    },
    {
      label: 'Sidste 90 dage',
      description: 'Seneste kvartal',
      range: {
        from: subDays(new Date(), 90),
        to: new Date(),
      },
      icon: <CalendarIcon className="h-4 w-4" />,
    },
  ],
};

export function AgriculturalDatePicker({
  dateRange,
  onDateRangeChange,
  className,
  placeholder = 'Vælg datoperiode',
  showPresets = true,
  context = 'general',
}: AgriculturalDatePickerProps) {
  const [isOpen, setIsOpen] = useState(false);

  const presets = agriculturalPresets[context] || agriculturalPresets.general;

  const formatDateRange = (range: DateRange) => {
    if (!range.from) return placeholder;
    if (!range.to) return format(range.from, 'dd MMM yyyy', { locale: da });
    if (range.from.getTime() === range.to.getTime()) {
      return format(range.from, 'dd MMM yyyy', { locale: da });
    }
    return `${format(range.from, 'dd MMM', { locale: da })} - ${format(range.to, 'dd MMM yyyy', { locale: da })}`;
  };

  const handlePresetClick = (preset: (typeof presets)[0]) => {
    onDateRangeChange(preset.range);
    setIsOpen(false);
  };

  const handleReset = () => {
    onDateRangeChange({ from: undefined, to: undefined });
  };

  const getDaysSelected = () => {
    if (!dateRange.from || !dateRange.to) return 0;
    return (
      Math.abs(
        Math.ceil(
          (dateRange.to.getTime() - dateRange.from.getTime()) /
            (1000 * 60 * 60 * 24)
        )
      ) + 1
    );
  };

  return (
    <div className={cn('space-y-2', className)}>
      <Popover open={isOpen} onOpenChange={setIsOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className={cn(
              'justify-between text-left font-normal',
              !dateRange.from && 'text-muted-foreground'
            )}
          >
            <div className="flex items-center gap-2">
              <CalendarIcon className="h-4 w-4" />
              {formatDateRange(dateRange)}
            </div>
            <div className="flex items-center gap-2">
              {dateRange.from && dateRange.to && (
                <Badge variant="secondary" className="text-xs">
                  {getDaysSelected()} dage
                </Badge>
              )}
              <ChevronDown className="h-4 w-4 opacity-50" />
            </div>
          </Button>
        </PopoverTrigger>

        <PopoverContent className="w-auto p-0" align="start">
          <div className="flex">
            {/* Presets Sidebar */}
            {showPresets && (
              <div className="min-w-[200px] space-y-2 border-r p-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium">Hurtigvalg</h4>
                  {(dateRange.from || dateRange.to) && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleReset}
                      className="h-6 w-6 p-0"
                    >
                      <RotateCcw className="h-3 w-3" />
                    </Button>
                  )}
                </div>

                <div className="space-y-1">
                  {presets.map((preset) => (
                    <Button
                      key={preset.label}
                      variant="ghost"
                      size="sm"
                      className="h-auto w-full justify-start p-2"
                      onClick={() => handlePresetClick(preset)}
                    >
                      <div className="flex w-full items-start gap-2">
                        {preset.icon}
                        <div className="text-left">
                          <div className="text-sm font-medium">
                            {preset.label}
                          </div>
                          <div className="text-muted-foreground text-xs">
                            {preset.description}
                          </div>
                        </div>
                      </div>
                    </Button>
                  ))}
                </div>
              </div>
            )}

            {/* Calendar */}
            <div className="p-3">
              <Calendar
                mode="range"
                selected={dateRange}
                onSelect={(range) =>
                  onDateRangeChange(
                    range
                      ? { from: range.from, to: range.to }
                      : { from: undefined, to: undefined }
                  )
                }
                numberOfMonths={2}
                locale={da}
                className="rounded-md"
              />

              {/* Selection Summary */}
              {dateRange.from && dateRange.to && (
                <div className="bg-muted mt-3 rounded-md p-2">
                  <div className="text-sm font-medium">Valgt periode</div>
                  <div className="text-muted-foreground text-sm">
                    {format(dateRange.from, 'dd MMMM yyyy', { locale: da })} -{' '}
                    {format(dateRange.to, 'dd MMMM yyyy', { locale: da })}
                  </div>
                  <div className="text-muted-foreground mt-1 text-xs">
                    {getDaysSelected()} dage valgt
                  </div>
                </div>
              )}
            </div>
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
}

// Context-specific date picker components
export function PesticideDatePicker({
  dateRange,
  onDateRangeChange,
  className,
}: {
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  className?: string;
}) {
  return (
    <AgriculturalDatePicker
      dateRange={dateRange}
      onDateRangeChange={onDateRangeChange}
      className={className}
      placeholder="Vælg sprøjteperiode"
      context="pesticide"
    />
  );
}

export function HarvestDatePicker({
  dateRange,
  onDateRangeChange,
  className,
}: {
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  className?: string;
}) {
  return (
    <AgriculturalDatePicker
      dateRange={dateRange}
      onDateRangeChange={onDateRangeChange}
      className={className}
      placeholder="Vælg høstperiode"
      context="harvest"
    />
  );
}

export function AnalysisDatePicker({
  dateRange,
  onDateRangeChange,
  className,
}: {
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  className?: string;
}) {
  return (
    <AgriculturalDatePicker
      dateRange={dateRange}
      onDateRangeChange={onDateRangeChange}
      className={className}
      placeholder="Vælg analyseperiode"
      context="analysis"
    />
  );
}
