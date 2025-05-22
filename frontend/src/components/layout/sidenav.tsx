"use client";

import { cn } from "@/lib/utils";
import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";

// Custom hook for media query
function useMediaQuery(query: string) {
  const [matches, setMatches] = useState(false);

  useEffect(() => {
    const media = window.matchMedia(query);
    if (media.matches !== matches) {
      setMatches(media.matches);
    }
    const listener = () => setMatches(media.matches);
    media.addEventListener("change", listener);
    return () => media.removeEventListener("change", listener);
  }, [matches, query]);

  return matches;
}

export interface NavigationItem {
  name: string;
  href: string;
  id?: string;
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
  const isMobile = useMediaQuery("(max-width: 768px)");
  const [isCollapsed, setIsCollapsed] = useState(isMobile);

  // Update isCollapsed when screen size changes
  useEffect(() => {
    setIsCollapsed(isMobile);
  }, [isMobile]);

  useEffect(() => {
    const hash = window.location.hash;
    if (hash) {
      setCurrent(hash);
    } else {
      const firstItem = navigation[0];
      if (firstItem) {
        setCurrent(firstItem.href);
      }
    }
  }, [navigation]);

  useEffect(() => {
    const onHashChanged = () => setCurrent(window.location.hash);
    const { pushState, replaceState } = window.history;
    window.history.pushState = function (...args) {
      pushState.apply(window.history, args);
      setTimeout(() => setCurrent(window.location.hash));
    };
    window.history.replaceState = function (...args) {
      replaceState.apply(window.history, args);
      setTimeout(() => setCurrent(window.location.hash));
    };
    window.addEventListener("hashchange", onHashChanged);
    return () => {
      window.removeEventListener("hashchange", onHashChanged);
    };
  }, []);

  const handleClick = (item: NavigationItem) => {
    const hash = item.href.split("#")[1];
    if (hash) {
      setCurrent("#" + hash);

      // replace the current url with the new hash without reloading the page
      window.history.pushState({}, "", item.href);

      const element = document.getElementById(hash);
      if (element) {
        element.scrollIntoView({ behavior: "smooth" });
      }
    }
  };

  return (
    <nav
      aria-label="Sidebar"
      className={cn(
        "flex flex-1 flex-col transition-all duration-300",

        className
      )}
    >
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-2xl font-bold">{title}</h2>
        <button
          onClick={() => setIsCollapsed(!isCollapsed)}
          className="p-2 hover:bg-gray-100 rounded-md transition-colors block md:hidden"
          aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {isCollapsed ? (
            <ChevronDownIcon className="size-5" />
          ) : (
            <ChevronUpIcon className="size-5" />
          )}
        </button>
      </div>
      <AnimatePresence>
        {!isCollapsed && (
          <motion.ul
            initial={{ opacity: 0 }}
            animate={{ opacity: isCollapsed ? 0 : 1 }}
            exit={{ opacity: 0 }}
            role="list"
            className={cn(
              "space-y-1 divide-y divide-slate-300 text-sm transition-all duration-300",
              isCollapsed && "opacity-0"
            )}
          >
            {navigation.map((item) => {
              const itemHash = item.href.split("#")[1];
              const isCurrent = current === "#" + itemHash;

              return (
                <li key={item.name}>
                  <div
                    className={cn(
                      isCurrent
                        ? "text-black font-bold"
                        : "font-medium text-gray-700 hover:font-semibold hover:text-black",
                      "group flex gap-x-3 p-4 pl-0 cursor-pointer"
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
                      {!isCollapsed && item.name}
                    </div>
                  </div>
                </li>
              );
            })}
          </motion.ul>
        )}
      </AnimatePresence>
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
