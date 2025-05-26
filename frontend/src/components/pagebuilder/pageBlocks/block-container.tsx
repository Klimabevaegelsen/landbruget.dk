import { cn } from "@/lib/utils";
import { LinkIcon } from "@heroicons/react/24/outline";
import Link from "next/link";

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
  return (
    <div className="flex flex-col gap-3 relative">
      <div
        className={cn(
          "flex items-center gap-2 group overflow-hidden",
          stickyTitle && "sticky top-0 z-40 bg-white py-4"
        )}
      >
        <h2 className="text-xl md:text-2xl font-bold">{title}</h2>
        <Link
          href={href}
          className="hidden group-hover:block items-center gap-2"
        >
          <LinkIcon className="size-6 text-primary" />
        </Link>
        {secondaryTitle && (
          <h3 className="text-xs  italic">{secondaryTitle}</h3>
        )}
      </div>
      {children}
    </div>
  );
}
