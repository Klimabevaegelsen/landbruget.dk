'use client';

import { useState } from 'react';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
import { DocumentationLink } from '@/services/supabase/types';
import { BookOpen } from 'lucide-react';

interface DocumentationAccordionProps {
  documentation: DocumentationLink;
}

export function DocumentationAccordion({
  documentation,
}: DocumentationAccordionProps) {
  const [isOpen, setIsOpen] = useState(false);

  if (!documentation) {
    return null;
  }

  return (
    <div className="mt-4 border-t border-gray-200 pt-4">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex w-full items-center justify-between text-left text-sm font-medium text-gray-700 hover:text-gray-900 focus:ring-2 focus:ring-green-500 focus:ring-offset-2 focus:outline-none"
      >
        <span className="flex items-center">
          <BookOpen className="mr-2 h-4 w-4" />
          Datakilder og dokumentation
        </span>
        {isOpen ? (
          <ChevronUpIcon className="h-4 w-4" />
        ) : (
          <ChevronDownIcon className="h-4 w-4" />
        )}
      </button>

      {isOpen && (
        <div className="mt-3 space-y-4 text-sm">
          {/* Description */}
          <div>
            <h4 className="font-medium text-gray-900">{documentation.title}</h4>
            <p className="mt-1 text-gray-600">{documentation.description}</p>
          </div>

          {/* Data Sources */}
          <div>
            <h5 className="font-medium text-gray-900">Datakilder:</h5>
            <ul className="mt-2 space-y-2">
              {documentation.sources.map((source, index) => (
                <li key={index} className="flex items-start">
                  <span className="mr-2 text-green-600">•</span>
                  <div>
                    <a
                      href={source.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-medium text-green-700 hover:text-green-800 hover:underline"
                    >
                      {source.name}
                    </a>
                  </div>
                </li>
              ))}
            </ul>
          </div>

          {/* Data Lineage */}
          {documentation.dataLineage && (
            <div>
              <h5 className="font-medium text-gray-900">Komplet dataflow:</h5>
              <p className="mt-1">
                <a
                  href={documentation.dataLineage}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-green-700 hover:text-green-800 hover:underline"
                >
                  Se detaljeret databehandling og kvalitetssikring →
                </a>
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
