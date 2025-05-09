"use client";
import { cn } from "@/lib/utils";
import { useEffect, useState } from "react";

export interface NavigationItem {
  name: string;
  href: string;
}

function SidenavClient({
  navigation,
  title,
  className,
}: {
  navigation: NavigationItem[];
  title: string;
  className?: string;
}) {
  const [current, setCurrent] = useState<string>("");

  useEffect(() => {
    console.log(window.location.hash);

    setCurrent(window.location.hash);
  }, []);

  const handleClick = (item: NavigationItem) => {
    const hash = item.href.split("#")[1];
    if (hash) {
      setCurrent("#" + hash);

      const element = document.getElementById(hash);
      if (element) {
        element.scrollIntoView({ behavior: "smooth" });
      }
    }
  };

  return (
    <nav aria-label="Sidebar" className={cn("flex flex-1 flex-col", className)}>
      <div className="mb-4">
        <h2 className="text-2xl font-bold">{title}</h2>
      </div>
      <ul role="list" className="space-y-1 divide-y divide-slate-300 text-sm">
        {navigation.map((item) => {
          const itemHash = item.href.split("#")[1];
          const isCurrent = current === "#" + itemHash;

          return (
            <li key={item.name}>
              <div
                className={cn(
                  isCurrent
                    ? " text-black font-bold"
                    : "font-medium text-gray-700 hover:font-semibold hover:text-black",
                  "group flex gap-x-3  p-4 pl-0 "
                )}
                onClick={() => {
                  handleClick(item);
                }}
              >
                <div
                  className={cn(
                    "pl-3",
                    isCurrent && "border-l-2 border-primary"
                  )}
                >
                  {item.name}
                </div>
              </div>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}

// Server component wrapper
export function Sidenav(props: {
  navigation: NavigationItem[];
  title: string;
  className?: string;
}) {
  return <SidenavClient {...props} />;
}
