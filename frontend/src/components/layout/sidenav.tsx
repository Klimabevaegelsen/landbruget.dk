import { cn } from "@/lib/utils";
import Link from "next/link";

export interface NavigationItem {
  name: string;
  href: string;
  current: boolean;
}

export function Sidenav({
  navigation,
  title,
  className,
}: {
  navigation: NavigationItem[];
  title: string;
  className?: string;
}) {
  return (
    <nav aria-label="Sidebar" className={cn("flex flex-1 flex-col", className)}>
      <div className="mb-4">
        <h2 className="text-2xl font-bold">{title}</h2>
      </div>
      <ul role="list" className="space-y-1 divide-y divide-slate-300 text-sm">
        {navigation.map((item) => (
          <li key={item.name}>
            <Link
              href={item.href}
              className={cn(
                item.current
                  ? " text-black font-bold"
                  : "font-medium text-gray-700 hover:font-semibold hover:text-black",
                "group flex gap-x-3  p-4 pl-0 "
              )}
            >
              <div
                className={cn(
                  "pl-3",
                  item.current && "border-l-2 border-primary"
                )}
              >
                {item.name}
              </div>
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  );
}
