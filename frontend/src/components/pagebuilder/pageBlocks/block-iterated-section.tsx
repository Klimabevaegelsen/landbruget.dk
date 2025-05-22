"use client";

import { useState } from "react";
import { IteratedSection } from "@/services/supabase/types";
import { PageBlock } from "../pagebuilder";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";
import { NavigationItem } from "../../layout/sidenav";
import { BlockContainer } from "./block-container";
import { cn, slugify } from "@/lib/utils";

export function IteratedSectionMenu({
  iteratedSection,
  level,
}: {
  iteratedSection: IteratedSection;
  level: number;
}) {
  const navigationItems: NavigationItem[] = iteratedSection.sections.map(
    (item, index) => ({
      name: item.title,
      href: `${slugify(item.title)}-${index}-${level}`,
      current: index === 0,
      id: `${slugify(item.title)}-${index}-${level}`,
    })
  );

  return (
    <div className="flex gap-4">
      {navigationItems.map((item, index) => (
        <div
          key={`${item.name}-${index}-${level}`}
          className="rounded-full border px-4 py-3 text-sm"
        >
          <div
            onClick={() => {
              const element = document.getElementById(item.id ?? item.href);
              element?.scrollIntoView({ behavior: "smooth" });
            }}
            className="cursor-pointer"
          >
            {item.name}
          </div>
        </div>
      ))}
    </div>
  );
}

export function BlockIteratedSection({
  iteratedSection,
  level = 0,
}: {
  iteratedSection: IteratedSection;
  level?: number;
}) {
  const [expandedSections, setExpandedSections] = useState<
    Record<string, boolean>
  >(
    iteratedSection.sections.reduce((acc, item) => {
      acc[item.title] = true;
      return acc;
    }, {} as Record<string, boolean>)
  );

  const toggleSection = (title: string) => {
    setExpandedSections((prev) => ({
      ...prev,
      [title]: !prev[title],
    }));
  };

  return (
    <div className={cn("flex flex-col w-full gap-4 relative")}>
      <div
        className={cn(
          "w-full sticky top-0 z-10 bg-white py-2",
          level === 0 && "top-16 z-30",
          level === 1 && "top-[126px] z-20",
          level === 2 && "top-[188px] z-10"
        )}
      >
        <IteratedSectionMenu iteratedSection={iteratedSection} level={level} />
      </div>
      <div className="flex flex-col gap-11">
        {iteratedSection.sections.map((item, index) => (
          <div
            key={`${item.title}-${index}-${level}`}
            id={`${slugify(item.title)}-${index}-${level}`}
            className=""
          >
            <button
              onClick={() => toggleSection(item.title)}
              className="pb-6 py-4 flex items-center gap-4 group cursor-pointer"
            >
              <h3 className="text-lg font-semibold group-hover:underline">
                {item.title}
              </h3>
              {expandedSections[item.title] ? (
                <ChevronUpIcon className="h-5 w-5" />
              ) : (
                <ChevronDownIcon className="h-5 w-5" />
              )}
            </button>
            {expandedSections[item.title] && (
              <div className="pb-6">
                <div className="flex flex-col gap-11">
                  {item.content.map((item) => (
                    <div key={item._key} id={item._key}>
                      <BlockContainer
                        title={item.title}
                        href={`#${item._key}-${index}-${level}`}
                        secondaryTitle={item._type}
                      >
                        <PageBlock block={item} level={level + 1} />
                      </BlockContainer>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
