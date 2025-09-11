'use client';

import { Timeline } from '@/services/supabase/types';
import {
  VerticalTimeline,
  VerticalTimelineElement,
} from 'react-vertical-timeline-component';
import 'react-vertical-timeline-component/style.min.css';
import { format } from 'date-fns';
import { da } from 'date-fns/locale';
import { useState, useMemo } from 'react';
import { VizColors } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { useCategoryDataContext } from './CategoryDataContext';
import { NoDataPlaceholder } from './no-data-placeholder';
import {
  DocumentationAccordion,
  TimelineCSVDownloadButton,
} from '@/components/chart';

type TimelineEvent = {
  date: string;
  description: string;
  event_type: string;
};

type TimelineEventCandidate = {
  date?: unknown;
  description?: unknown;
  event_type?: unknown;
};

function isValidEvent(event: TimelineEventCandidate): event is TimelineEvent {
  return (
    typeof event === 'object' &&
    event !== null &&
    typeof event.date === 'string' &&
    typeof event.description === 'string' &&
    typeof event.event_type === 'string' &&
    event.event_type.length > 0
  );
}

// Translation function for veterinary timeline event types
function translateEventType(eventType: string): string {
  const translations: Record<string, string> = {
    certificate_issued: 'Certifikat udstedt',
    certificate_rejected: 'Certifikat afvist',
    control_inspection: 'Kontrolbesøg',
    disease_status: 'Sygdomsstatus',
    disease_status_change: 'Ændring i sygdomsstatus',
    intervention_end: 'Intervention afsluttet',
    intervention_start: 'Intervention startet',
    sanitation_process: 'Sanering',
    stable_fire: 'Staldbrand',
    under_approval: 'Under godkendelse',
  };

  return translations[eventType] || eventType;
}

export function BlockTimeline({ timeline }: { timeline: Timeline }) {
  const { isInCategoryWithData } = useCategoryDataContext();

  // Get unique event types and assign colors
  const eventTypes = useMemo(() => {
    const validEvents = timeline.events.filter(isValidEvent);
    // Only get unique event types if we have filterColumns
    if (!timeline.config?.filterColumns?.length) {
      return {};
    }
    const types = Array.from(
      new Set(validEvents.map((event) => event.event_type))
    );
    return types.reduce(
      (acc, type, index) => {
        acc[type] = VizColors[index % VizColors.length];
        return acc;
      },
      {} as Record<string, string>
    );
  }, [timeline.events, timeline.config?.filterColumns]);

  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(() => {
    const validEvents = timeline.events.filter(isValidEvent);
    // If no filterColumns, return empty set
    if (!timeline.config?.filterColumns?.length) {
      return new Set();
    }
    return new Set(validEvents.map((event) => event.event_type));
  });

  const filteredEvents = useMemo(() => {
    let events: TimelineEvent[];

    if (!timeline.config?.filterColumns?.length) {
      events = timeline.events.filter(isValidEvent);
    } else {
      const validEvents = timeline.events.filter(isValidEvent);
      events = validEvents.filter((event) =>
        selectedTypes.has(event.event_type)
      );
    }

    return events.sort(
      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()
    );
  }, [timeline.events, selectedTypes, timeline.config?.filterColumns]);

  // Group events by date
  const groupedEvents = useMemo(() => {
    const grouped = new Map<string, TimelineEvent[]>();

    filteredEvents.forEach((event) => {
      const dateKey = format(new Date(event.date), 'yyyy-MM-dd');
      if (!grouped.has(dateKey)) {
        grouped.set(dateKey, []);
      }
      grouped.get(dateKey)!.push(event);
    });

    // Convert to array and sort by date (newest first)
    return Array.from(grouped.entries())
      .sort(
        ([dateA], [dateB]) =>
          new Date(dateB).getTime() - new Date(dateA).getTime()
      )
      .map(([date, events]) => ({
        date,
        events: events.sort((a, b) => {
          // Within the same date, sort by event type for consistency
          if (a.event_type && b.event_type) {
            return a.event_type.localeCompare(b.event_type);
          }
          return 0;
        }),
      }));
  }, [filteredEvents]);

  const toggleEventType = (type: string) => {
    const newSelected = new Set(selectedTypes);

    // If all types are currently selected and we're clicking one
    if (selectedTypes.size === Object.keys(eventTypes).length) {
      // Clear all and only select the clicked type
      newSelected.clear();
      newSelected.add(type);
    } else if (selectedTypes.size === 1 && selectedTypes.has(type)) {
      // If this is the last active type being clicked, enable all types
      Object.keys(eventTypes).forEach((t) => newSelected.add(t));
    } else {
      // Normal toggle behavior
      if (newSelected.has(type)) {
        newSelected.delete(type);
      } else {
        newSelected.add(type);
      }
    }

    setSelectedTypes(newSelected);
  };

  // Check if timeline has no data
  if (!timeline.events || timeline.events.length === 0) {
    if (!isInCategoryWithData) {
      return <NoDataPlaceholder />;
    } else {
      // If we're in a category with data, render nothing instead of individual message
      return null;
    }
  }

  return (
    <div>
      <div className="mx-auto w-full max-w-6xl py-4">
        {/* Header with filters and download button */}
        <div className="mb-8 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="flex-1">
            {/* Filter buttons - only show if we have filterColumns */}
            {timeline.config?.filterColumns?.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {Object.entries(eventTypes).map(([type, color]) => (
                  <Button
                    size="sm"
                    key={type}
                    onClick={() => toggleEventType(type)}
                    className={`flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                      selectedTypes.has(type)
                        ? 'text-primary-foreground'
                        : 'text-muted-foreground hover:bg-muted/50'
                    }`}
                    style={{
                      backgroundColor: selectedTypes.has(type)
                        ? color
                        : 'transparent',
                      border: `1px solid ${color}`,
                    }}
                  >
                    {translateEventType(type)}
                  </Button>
                ))}
              </div>
            )}
          </div>
          <TimelineCSVDownloadButton
            timeline={timeline}
            chartTitle={timeline.title}
            chartKey={timeline._key}
            className="self-start sm:ml-4"
          />
        </div>

        {/* Timeline */}
        <VerticalTimeline
          lineColor="#e5e7eb"
          animate={false}
          layout="2-columns"
        >
          {groupedEvents.map(({ date, events }, groupIndex) => {
            // Use the first event's type color for the icon, or create a gradient for multiple types
            const uniqueTypes = Array.from(
              new Set(events.map((e) => e.event_type).filter(Boolean))
            );
            const primaryColor =
              uniqueTypes.length > 0
                ? (eventTypes[uniqueTypes[0]] ?? VizColors[0])
                : VizColors[0];

            // Create gradient background if multiple event types
            const iconStyle =
              uniqueTypes.length > 1
                ? {
                    background: `linear-gradient(45deg, ${uniqueTypes
                      .slice(0, 3)
                      .map((type) => eventTypes[type] ?? VizColors[0])
                      .join(', ')})`,
                    color: primaryColor,
                    border: '2px solid white',
                    boxShadow: '0 0 0 1px white',
                    width: events.length > 1 ? '24px' : '20px',
                    height: events.length > 1 ? '24px' : '20px',
                  }
                : {
                    background: primaryColor,
                    color: primaryColor,
                    border: '2px solid white',
                    boxShadow: '0 0 0 1px white',
                    width: '20px',
                    height: '20px',
                  };

            return (
              <VerticalTimelineElement
                key={`${date}-${groupIndex}`}
                className="vertical-timeline-element"
                contentStyle={{
                  background: 'var(--color-card)',
                  boxShadow: '0 0 0 1px var(--color-border)',
                  border: 'none',
                  borderRadius: '0.25rem',
                }}
                contentArrowStyle={{
                  borderRight: '7px solid var(--color-card)',
                }}
                date={format(new Date(date), 'd. MMMM yyyy', {
                  locale: da,
                })}
                dateClassName="text-muted-foreground font-medium min-[1170px]:mx-4 font-bold"
                iconStyle={iconStyle}
                iconClassName={`!ml-2.5 !top-3 min-[1170px]:!-ml-2.5 min-[1170px]:!top-5 ${events.length > 1 ? '!-ml-3.5 min-[1170px]:!-ml-3' : ''}`}
              >
                <div className="flex flex-col gap-2">
                  {events.length > 1 && (
                    <div className="bg-muted text-muted-foreground mb-1 inline-block rounded-md px-2 py-1 text-xs font-medium">
                      {events.length} begivenheder på samme dag
                    </div>
                  )}
                  {events.map((event, eventIndex) => {
                    const color =
                      eventTypes[event.event_type ?? ''] ?? VizColors[0];
                    const isLastEvent = eventIndex === events.length - 1;
                    return (
                      <div
                        key={`${date}-${eventIndex}`}
                        className={`flex flex-col gap-1 ${!isLastEvent && events.length > 1 ? 'border-b border-gray-200 pb-2' : ''}`}
                      >
                        <div
                          className="text-sm font-semibold"
                          style={{ color }}
                        >
                          {translateEventType(event.event_type || '')}
                        </div>
                        <div className="text-foreground p-0">
                          {event.description}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </VerticalTimelineElement>
            );
          })}
        </VerticalTimeline>
      </div>

      {/* Documentation accordion */}
      {timeline.documentation && (
        <DocumentationAccordion documentation={timeline.documentation} />
      )}
    </div>
  );
}
