'use client';

import {
  Disclosure,
  DisclosureButton,
  DisclosurePanel,
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
} from '@headlessui/react';
import {
  Bars3Icon,
  MagnifyingGlassIcon,
  ChevronDownIcon,
} from '@heroicons/react/24/solid';
import { motion } from 'framer-motion';
import { Logo } from '@/components/layout/templates/logo';
import Link from 'next/link';
import { Container } from '../container';
import { cn } from '@/lib/utils';
import { GlobalSearch } from '@/components/global-search';
import { useState } from 'react';
import { Button } from '@/components/ui/button';

const analyserLinks = [
  { href: '/markanalyse', label: 'Markanalyse' },
  { href: '/pesticidanalyse', label: 'Pesticidanalyse' },
];

function DesktopNav() {
  return (
    <nav className="relative hidden items-center gap-4 lg:flex">
      <Menu as="div" className="relative">
        <MenuButton className="flex items-center px-4 py-3 text-sm font-medium text-gray-950 bg-blend-multiply hover:underline data-hover:bg-black/[2.5%]">
          Analyser
          <ChevronDownIcon className="ml-1 h-4 w-4" aria-hidden="true" />
        </MenuButton>
        <MenuItems className="ring-opacity-5 absolute right-0 z-10 mt-2 w-48 origin-top-right rounded-md bg-white py-1 shadow-lg ring-1 ring-black focus:outline-none">
          {analyserLinks.map(({ href, label }) => (
            <MenuItem key={href}>
              <Link
                href={href}
                className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 hover:text-gray-900"
              >
                {label}
              </Link>
            </MenuItem>
          ))}
        </MenuItems>
      </Menu>
      <Link
        href="https://github.com/klimabevaegelsen/landbruget.dk/"
        target="_blank"
        rel="noopener noreferrer"
        className="text-sm"
      >
        <Button>Hjælp til</Button>
      </Link>
    </nav>
  );
}

function MobileNavButton() {
  return (
    <DisclosureButton
      className="flex size-12 items-center justify-center self-center rounded-lg data-hover:bg-black/5 lg:hidden"
      aria-label="Open main menu"
    >
      <Bars3Icon className="size-6" />
    </DisclosureButton>
  );
}

function MobileNavSearch({ onClick }: { onClick: () => void }) {
  return (
    <div
      onClick={onClick}
      className="flex size-12 items-center justify-center self-center rounded-lg hover:bg-black/5 lg:hidden"
      aria-label="Open main menu"
    >
      <MagnifyingGlassIcon className="size-6" />
    </div>
  );
}

function MobileNav() {
  return (
    <DisclosurePanel className="lg:hidden">
      <Link
        href="https://github.com/klimabevaegelsen/landbruget.dk/"
        target="_blank"
        rel="noopener noreferrer"
        className="text-sm"
      >
        <Button>Hjælp til</Button>
      </Link>
      <div className="ml-2 flex flex-col gap-6 py-4">
        <div className="text-base font-semibold text-gray-950">Analyser</div>
        {analyserLinks.map(({ href, label }, linkIndex) => (
          <motion.div
            initial={{ opacity: 0, rotateX: -90 }}
            animate={{ opacity: 1, rotateX: 0 }}
            transition={{
              duration: 0.15,
              ease: 'easeInOut',
              rotateX: { duration: 0.3, delay: linkIndex * 0.1 },
            }}
            key={href}
            className="ml-4"
          >
            <Link href={href} className="text-base font-medium text-gray-700">
              {label}
            </Link>
          </motion.div>
        ))}
      </div>
      <div className="absolute left-1/2 w-screen -translate-x-1/2">
        <div className="absolute inset-x-0 top-0 border-t border-black/5" />
      </div>
    </DisclosurePanel>
  );
}

export function Navbar({ banner }: { banner?: React.ReactNode }) {
  const [searchOpen, setSearchOpen] = useState(false);
  return (
    <div className="relative">
      {banner && <div className="relative flex items-center">{banner}</div>}
      <Container className="relative">
        <Disclosure as="header" className={cn(!banner && 'pt-12 sm:pt-10')}>
          <div className="relative flex justify-between gap-8 py-2 lg:px-10">
            <MobileNavButton />
            <div className="relative flex gap-6">
              <div className="my-auto w-[180px]">
                <Link href="/" title="Home">
                  <Logo className="" />
                </Link>
              </div>
            </div>
            <MobileNavSearch onClick={() => setSearchOpen(true)} />
            <GlobalSearch className="hidden lg:flex" defaultOpen={false} />
            <DesktopNav />
          </div>

          <MobileNav />
        </Disclosure>
        <div
          className={cn(
            'absolute top-0 right-0 bottom-0 left-0 z-50 px-6 py-2',
            !searchOpen && 'hidden'
          )}
        >
          <GlobalSearch
            className="lg:hidden"
            parentOpen={searchOpen}
            onClose={() => setSearchOpen(false)}
          />
        </div>
      </Container>
    </div>
  );
}
