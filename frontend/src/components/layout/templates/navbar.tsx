"use client";

import {
  Disclosure,
  DisclosureButton,
  DisclosurePanel,
} from "@headlessui/react";
import { Bars3Icon, MagnifyingGlassIcon } from "@heroicons/react/24/solid";
import { AnimatePresence, motion } from "framer-motion";
import { Logo } from "@/components/layout/templates/logo";
import Link from "next/link";
import { Container } from "../container";
import { cn } from "@/lib/utils";
import { GlobalSearch } from "@/components/global-search";
import { useState } from "react";
import { Button } from "@/components/ui/button";

const links = [
  { href: "/?section=overview", label: "Oversigt" },
  { href: "/?section=explore", label: "Udforsk" },
  { href: "/?section=blog", label: "Blog" },
];

function DesktopNav() {
  return (
    <nav className="relative hidden lg:flex items-center gap-4">
      {links.map(({ href, label }) => (
        <div key={href} className="relative flex">
          <Link
            href={href}
            className="flex items-center px-4 py-3 text-base font-medium text-gray-950 bg-blend-multiply data-hover:bg-black/[2.5%] text-sm"
          >
            {label}
          </Link>
        </div>
      ))}
      <Link href="/?section=help" className="text-sm">
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
      <Link href="/?section=help" className="text-sm">
        <Button>Hjælp til</Button>
      </Link>
      <div className="flex flex-col gap-6 py-4 ml-2">
        {links.map(({ href, label }, linkIndex) => (
          <motion.div
            initial={{ opacity: 0, rotateX: -90 }}
            animate={{ opacity: 1, rotateX: 0 }}
            transition={{
              duration: 0.15,
              ease: "easeInOut",
              rotateX: { duration: 0.3, delay: linkIndex * 0.1 },
            }}
            key={href}
          >
            <Link href={href} className="text-base font-medium text-gray-950">
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
        <Disclosure as="header" className={cn(!banner && "pt-12 sm:pt-10")}>
          <div className="relative flex justify-between gap-8 lg:px-10 py-2">
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
            "absolute py-2 px-6 top-0 left-0 right-0 bottom-0 z-50",
            !searchOpen && "hidden"
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
