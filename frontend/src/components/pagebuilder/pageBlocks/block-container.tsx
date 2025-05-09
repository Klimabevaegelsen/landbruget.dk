import { LinkIcon } from "@heroicons/react/24/outline";
import Link from "next/link";

export function BlockContainer({
  children,
  title,
  href,
}: {
  children: React.ReactNode;
  title: string;
  href: string;
}) {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2 group">
        <h2 className="text-2xl font-bold">{title}</h2>
        <Link
          href={href}
          className="hidden group-hover:block items-center gap-2"
        >
          <LinkIcon className="size-6 text-primary" />
        </Link>
      </div>
      {children}
    </div>
  );
}
