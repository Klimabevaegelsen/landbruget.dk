'use client';

import { cn } from '@/lib/utils';
import { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
import { LucideIcon } from 'lucide-react';

import { useHashStore } from '@/stores/hashStore';

function useMediaQuery(query: string) {
  const [matches, setMatches] = useState(false);

  useEffect(() => {
    const media = window.matchMedia(query);
    if (media.matches !== matches) {
      setMatches(media.matches);
    }
    const listener = () => setMatches(media.matches);
    media.addEventListener('change', listener);
    return () => media.removeEventListener('change', listener);
  }, [matches, query]);

  return matches;
}

export interface NavigationItem {
  name: string;
  href: string;
  id?: string;
  subItems?: NavigationItem[];
}

export interface NavigationGroup {
  name: string;
  items: NavigationItem[];
  icon?: LucideIcon;
}

function NavLink({
  item,
  currentHash,
  isCollapsed,
  indent,
  onClick,
}: {
  item: NavigationItem;
  currentHash: string;
  isCollapsed: boolean;
  indent: 'pl-3' | 'pl-6';
  onClick: () => void;
}) {
  const itemHash = item.href.split('#')[1];
  const isCurrent = currentHash === '#' + itemHash;
  return (
    <div
      className={cn(
        isCurrent
          ? 'text-foreground font-bold'
          : 'text-foreground hover:text-foreground font-medium hover:font-semibold',
        'group flex cursor-pointer gap-x-3 p-4 pl-0'
      )}
      onClick={onClick}
    >
      <div className={cn(indent, isCurrent && 'border-primary border-l-2')}>
        {!isCollapsed && item.name}
      </div>
    </div>
  );
}

function NavItemWithSubs({
  item,
  currentHash,
  isCollapsed,
  onItemClick,
  onSubItemClick,
}: {
  item: NavigationItem;
  currentHash: string;
  isCollapsed: boolean;
  onItemClick: (item: NavigationItem) => void;
  onSubItemClick: (item: NavigationItem) => void;
}) {
  return (
    <li key={item.id}>
      <NavLink
        item={item}
        currentHash={currentHash}
        isCollapsed={isCollapsed}
        indent="pl-3"
        onClick={() => onItemClick(item)}
      />
      {item.subItems && (
        <ul>
          {item.subItems.map((subItem) => (
            <li key={subItem.id}>
              <NavLink
                item={subItem}
                currentHash={currentHash}
                isCollapsed={isCollapsed}
                indent="pl-6"
                onClick={() => onSubItemClick(subItem)}
              />
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

function SidenavClient({
  navigation,
  groupedNavigation,
  title,
  className,
}: {
  navigation?: NavigationItem[];
  groupedNavigation?: NavigationGroup[];
  title: string;
  className?: string;
}) {
  const { currentHash, setCurrentHash } = useHashStore();
  const isMobile = useMediaQuery('(max-width: 768px)');
  const [isCollapsed, setIsCollapsed] = useState(isMobile);

  useEffect(() => {
    setIsCollapsed(isMobile);
  }, [isMobile]);

  useEffect(() => {
    const hash = window.location.hash;
    if (hash) {
      setCurrentHash(hash);
    } else {
      const firstItem = groupedNavigation
        ? groupedNavigation[0]?.items[0]
        : navigation?.[0];
      if (firstItem) {
        setCurrentHash(firstItem.href);
      }
    }
  }, [navigation, groupedNavigation, setCurrentHash]);

  useEffect(() => {
    const onHashChanged = () => setCurrentHash(window.location.hash);
    const { pushState, replaceState } = window.history;
    window.history.pushState = function (...args) {
      pushState.apply(window.history, args);
      setTimeout(() => setCurrentHash(window.location.hash));
    };
    window.history.replaceState = function (...args) {
      replaceState.apply(window.history, args);
      setTimeout(() => setCurrentHash(window.location.hash));
    };
    window.addEventListener('hashchange', onHashChanged);
    return () => {
      window.removeEventListener('hashchange', onHashChanged);
    };
  }, [setCurrentHash]);

  const handleClick = (item: NavigationItem, isSubItem?: boolean) => {
    const hash = item.href.split('#')[1];
    if (hash) {
      setCurrentHash('#' + hash);

      window.history.pushState({}, '', item.href);

      const element = document.getElementById(hash);
      if (element && !isSubItem) {
        element.scrollIntoView({ behavior: 'smooth' });
      }

      if (element && isSubItem) {
        const offset = 135;
        const elementPosition = element?.getBoundingClientRect().top;
        if (elementPosition) {
          const offsetPosition = elementPosition + window.pageYOffset - offset;
          window.scrollTo({
            top: offsetPosition,
            behavior: 'smooth',
          });
        }
      }
    }
  };

  return (
    <nav
      aria-label="Sidebar"
      className={cn(
        'flex flex-1 flex-col transition-all duration-300',

        className
      )}
    >
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-xl font-bold md:text-2xl">{title}</h2>
        <button
          onClick={() => setIsCollapsed(!isCollapsed)}
          data-testid="toggle-sidebar-button"
          className="hover:bg-muted block rounded-md p-2 transition-colors md:hidden"
          aria-label={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
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
              'divide-border space-y-1 divide-y text-sm transition-all duration-300',
              isCollapsed && 'opacity-0'
            )}
          >
            {groupedNavigation
              ? groupedNavigation.map((group, groupIndex) => (
                  <div
                    key={group.name}
                    className={groupIndex > 0 ? 'mt-6' : ''}
                  >
                    <div className="mb-2 px-3">
                      <h3 className="text-muted-foreground flex items-center gap-2 text-sm font-semibold tracking-wider uppercase">
                        {group.icon && <group.icon className="size-4" />}
                        {group.name}
                      </h3>
                    </div>
                    {group.items.map((item) => (
                      <NavItemWithSubs
                        key={item.id}
                        item={item}
                        currentHash={currentHash}
                        isCollapsed={isCollapsed}
                        onItemClick={handleClick}
                        onSubItemClick={(sub) => handleClick(sub, true)}
                      />
                    ))}
                  </div>
                ))
              : navigation?.map((item) => (
                  <NavItemWithSubs
                    key={item.id}
                    item={item}
                    currentHash={currentHash}
                    isCollapsed={isCollapsed}
                    onItemClick={handleClick}
                    onSubItemClick={(sub) => handleClick(sub, true)}
                  />
                ))}
          </motion.ul>
        )}
      </AnimatePresence>
    </nav>
  );
}

export function Sidenav(props: {
  navigation?: NavigationItem[];
  groupedNavigation?: NavigationGroup[];
  title: string;
  className?: string;
}) {
  return <SidenavClient {...props} />;
}
