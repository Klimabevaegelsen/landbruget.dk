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
import { DocumentationAccordion } from '@/components/chart/documentation-accordion';

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
    if (!timeline.config?.filterColumns?.length) {
      return timeline.events.sort(
        (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()
      );
    }

    const validEvents = timeline.events.filter(isValidEvent);
    // If no filterColumns, return all valid events
    if (!timeline.config?.filterColumns?.length) {
      return validEvents.sort(
        (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()
      );
    }
    return validEvents
      .filter((event) => selectedTypes.has(event.event_type))
      .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  }, [timeline.events, selectedTypes, timeline.config?.filterColumns]);

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
      return (
        <div className="py-8 text-center text-gray-500">
          Ingen tidslinje data tilgængelig
        </div>
      );
    }
  }

  return (
    <div>
      <div className="mx-auto w-full max-w-6xl py-4">
        {/* Filter buttons - only show if we have filterColumns */}
        {timeline.config?.filterColumns?.length > 0 && (
          <div className="mb-8 flex flex-wrap gap-2">
            {Object.entries(eventTypes).map(([type, color]) => (
              <Button
                size="sm"
                key={type}
                onClick={() => toggleEventType(type)}
                className={`flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                  selectedTypes.has(type)
                    ? 'text-white'
                    : 'text-gray-600 hover:bg-gray-100'
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

        {/* Timeline */}
        <VerticalTimeline
          lineColor="#e5e7eb"
          animate={false}
          layout="2-columns"
        >
          {filteredEvents.map((event, index) => {
            const color = eventTypes[event.event_type ?? ''] ?? VizColors[0];

            return (
              <VerticalTimelineElement
                key={`${event.date}-${index}`}
                className="vertical-timeline-element"
                contentStyle={{
                  background: '#eef8f2',
                  boxShadow: '0 0 0 1px #eef8f2',
                  border: 'none',
                  borderRadius: '0.25rem',
                }}
                contentArrowStyle={{ borderRight: '7px solid #eef8f2' }}
                date={format(new Date(event.date), 'd. MMMM yyyy', {
                  locale: da,
                })}
                dateClassName="text-gray-600 font-medium min-[1170px]:mx-4 font-bold"
                iconStyle={{
                  background: color,
                  color: color,
                  border: '2px solid white',
                  boxShadow: '0 0 0 1px white',
                  width: '20px',
                  height: '20px',
                }}
                iconClassName="!ml-2.5 !top-3   min-[1170px]:!-ml-2.5 min-[1170px]:!top-5"
              >
                <div className="flex flex-col gap-1">
                  <div className="text-sm font-semibold" style={{ color }}>
                    {translateEventType(event.event_type || '')}
                  </div>
                  <div className="p-0 text-gray-700">{event.description}</div>
                  {/* <span className="text-sm text-gray-500">
                    {format(new Date(event.date), "d. MMMM yyyy", { locale: da })}
                  </span> */}
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
