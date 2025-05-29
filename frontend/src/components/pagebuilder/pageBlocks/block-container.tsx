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
        document.documentElement.style.setProperty(
          "--sticky-header-height",
          `${height}px`
        );
      };

      updateHeaderHeight();
      window.addEventListener("resize", updateHeaderHeight);
      return () => window.removeEventListener("resize", updateHeaderHeight);
    }
  }, [stickyTitle]);

  return (
    <div className="flex flex-col gap-3 relative">
      <div
        ref={headerRef}
        className={cn(
          "flex flex-col md:flex-row md:items-center gap-2 group overflow-hidden",
          stickyTitle && "sticky top-0 z-40 bg-white py-4"
        )}
      >
        <h2 className="text-xl md:text-2xl font-bold">{title}</h2>
        <div className="flex items-center gap-2">
          <Link
            href={href}
            className="md:hidden group-hover:block items-center gap-2"
          >
            <LinkIcon className="size-6 text-primary" />
          </Link>
          {secondaryTitle && (
            <h3 className="text-xs  italic">{secondaryTitle}</h3>
          )}
        </div>
      </div>
      {children}
    </div>
  );
}
