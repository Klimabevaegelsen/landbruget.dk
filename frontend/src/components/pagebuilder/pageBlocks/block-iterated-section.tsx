"use client";

import { useEffect, useState } from "react";
import { IteratedSection } from "@/services/supabase/types";
import { PageBlock } from "../pagebuilder";
import { NavigationItem } from "../../layout/sidenav";
import { BlockContainer } from "./block-container";
import { cn, slugify, scrollToElement } from "@/lib/utils";
import { useHashStore } from "@/stores/hashStore";

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
    iteratedSection.sections.map((item) => ({
      name: item.title,
      href: `${slugify(item._key)}`,
      current: `${slugify(item._key)}` === activeSection,
      id: `${slugify(item._key)}`,
    }));

  return (
    <div className="flex gap-4 overflow-x-auto">
      {navigationItems.map((item, index) => (
        <div
          key={`${item.name}-${index}-${level}`}
          className={cn(
            "rounded-full border px-4 py-3 text-sm",
            item.current && "border-black"
          )}
        >
          <div
            onClick={() => {
              onSectionChange(item.id ?? item.href);
              const offset = level === 0 ? 0 : 80;
              scrollToElement(iteratedSection._key, offset);
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
    `${slugify(iteratedSection.sections[0]._key)}`
  );

  const { currentHash } = useHashStore();

  useEffect(() => {
    if (currentHash) {
      // check if the current hash is a section in the iterated section, and if so, set the active section
      const sectionId = currentHash.split("#")[1];
      if (
        iteratedSection.sections.some((section) => section._key === sectionId)
      ) {
        setActiveSection(sectionId);
        setTimeout(() => {
          scrollToElement(sectionId, 135);
        }, 200);
      }
    }
  }, [currentHash, iteratedSection, level]);

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
        {iteratedSection.sections.map((item) => {
          const sectionId = `${slugify(item._key)}`;
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
                        href={`#${item._key}`}
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
