"use client";

import { cn } from "@/lib/utils";
import { LinkIcon } from "@heroicons/react/24/outline";
import Link from "next/link";
import { useEffect, useRef } from "react";

export function BlockContainer({
  children,
  title,
  href,
  secondaryTitle,
  stickyTitle,
}: {
  children: React.ReactNode;
  title: string;
  href: string;
  secondaryTitle?: string;
  stickyTitle?: boolean;
}) {
  const headerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (stickyTitle && headerRef.current) {
      const updateHeaderHeight = () => {
        const height = headerRef.current?.offsetHeight ?? 0;
        document.documentElement.style.setProperty("--sticky-header-height", `${height}px`);
      };

      updateHeaderHeight();
      window.addEventListener("resize", updateHeaderHeight);
      return () => window.removeEventListener("resize", updateHeaderHeight);
    }
  }, [stickyTitle]);

  return (
    <div className="relative flex flex-col gap-3">
      <div
        ref={headerRef}
        className={cn(
          "group flex flex-col gap-2 overflow-hidden md:flex-row md:items-center",
          stickyTitle && "sticky top-0 z-40 bg-white py-4"
        )}
      >
        <h2 className="text-xl font-bold md:text-2xl">{title}</h2>
        <div className="flex items-center gap-2">
          <Link href={href} className="items-center gap-2 group-hover:block md:hidden">
            <LinkIcon className="text-primary size-6" />
          </Link>
          {secondaryTitle && <h3 className="text-xs italic">{secondaryTitle}</h3>}
        </div>
      </div>
      {children}
    </div>
  );
}
