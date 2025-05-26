"use client";

import { useState } from "react";
import { IteratedSection } from "@/services/supabase/types";
import { PageBlock } from "../pagebuilder";
import { NavigationItem } from "../../layout/sidenav";
import { BlockContainer } from "./block-container";
import { cn, slugify } from "@/lib/utils";

interface ExtendedNavigationItem extends NavigationItem {
  current: boolean;
}

export function IteratedSectionMenu({
  iteratedSection,
  level,
  activeSection,
  onSectionChange,
}: {
  iteratedSection: IteratedSection;
  level: number;
  activeSection: string;
  onSectionChange: (sectionId: string) => void;
}) {
  const navigationItems: ExtendedNavigationItem[] =
    iteratedSection.sections.map((item, index) => ({
      name: item.title,
      href: `${slugify(item.title)}-${index}-${level}`,
      current: `${slugify(item.title)}-${index}-${level}` === activeSection,
      id: `${slugify(item.title)}-${index}-${level}`,
    }));

  return (
    <div className="flex gap-4">
      {navigationItems.map((item, index) => (
        <div
          key={`${item.name}-${index}-${level}`}
          className={cn(
            "rounded-full border px-4 py-3 text-sm",
            item.current && "bg-gray-100"
          )}
        >
          <div
            onClick={() => {
              onSectionChange(item.id ?? item.href);
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
  const [activeSection, setActiveSection] = useState<string>(
    `${slugify(iteratedSection.sections[0].title)}-0-${level}`
  );

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
        <IteratedSectionMenu
          iteratedSection={iteratedSection}
          level={level}
          activeSection={activeSection}
          onSectionChange={setActiveSection}
        />
      </div>
      <div className="flex flex-col gap-11">
        {iteratedSection.sections.map((item, index) => {
          const sectionId = `${slugify(item.title)}-${index}-${level}`;
          const isActive = sectionId === activeSection;

          return (
            <div
              key={sectionId}
              id={sectionId}
              className={cn(!isActive && "hidden")}
            >
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
            </div>
          );
        })}
      </div>
    </div>
  );
}
