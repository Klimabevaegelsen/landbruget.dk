"use client";
import { useEffect, useState } from "react";
import { Input } from "./ui/input";
import { cn } from "@/lib/utils";
import { MagnifyingGlassIcon } from "@heroicons/react/24/solid";
import { motion, AnimatePresence } from "framer-motion";
import React from "react";
import Image from "next/image";
import Link from "next/link";

export function GlobalSearch({
  className,
  defaultOpen,
  parentOpen,
  onClose,
}: {
  className?: string;
  defaultOpen?: boolean;
  parentOpen?: boolean;
  onClose?: () => void;
}) {
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    if (parentOpen) setOpen(parentOpen);
  }, [parentOpen]);

  return (
    <div
      className={cn(
        " relative overflow flex items-center w-full ",
        className,
        !parentOpen && "hidden"
      )}
    >
      <Input
        type="text"
        placeholder="Søg efter CVR, firmanavn eller person"
        value={search}
        onChange={(e) => {
          setSearch(e.target.value);
          setOpen(e.target.value.length > 0);
        }}
        onFocus={() => setOpen(true)}
        className="px-auto h-12"
        endIcon={<MagnifyingGlassIcon className="size-6" />}
      />
      <AnimatePresence>
        {open && (
          <SearchOverlay
            search={search}
            setSearch={setSearch}
            onClose={() => {
              setOpen(false);
              onClose?.();
            }}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

interface SearchResult {
  name: string;
  cvr: string;
  address: string;
  value: string;
  type: string;
  id: string;
}

function SearchOverlay({
  search,
  setSearch,
  onClose,
}: {
  search: string;
  setSearch: (v: string) => void;
  onClose: () => void;
}) {
  // Tabs for categories
  const tabs = ["Alle", "CVR", "Firmanavn", "Person", "Lokation"];
  const [activeTab, setActiveTab] = useState(0);

  const searchResults: SearchResult[] = [
    {
      name: "Komplet Testfarm ApS",
      cvr: "99887766",
      address: "Sønderhøj 14, 8260 Viby J Denmark",
      value: "1234567890",
      type: "company",
      id: "7306b8a2-caad-4db8-a810-d6e58a3cccac",
    },
  ];

  useEffect(() => {
    console.log(activeTab);
  }, [activeTab]);

  // Close on Escape
  React.useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  // Close on click outside
  const overlayRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        overlayRef.current &&
        !overlayRef.current.contains(e.target as Node)
      ) {
        onClose();
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [onClose]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.1 }}
      className="absolute  left-0 top-0 right-0 bottom-0 z-50 bg-white/90 flex flex-col items-center justify-start"
      ref={overlayRef}
    >
      <div className="w-full  h-auto shadow-lg border border-gray-100 rounded-lg">
        <Input
          autoFocus
          type="text"
          placeholder="Søg efter CVR, firmanavn eller person"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-12 rounded-lg border-none focus:ring-0 focus-visible:ring-0"
          endIcon={<MagnifyingGlassIcon className="size-6" />}
        />
        <div className="flex gap-2 mt-0 mb-2 bg-primary-foreground items-stretch overflow-x-auto">
          {tabs.map((tab, i) => (
            <div
              key={tab}
              onClick={() => setActiveTab(i)}
              className={cn(
                "flex-1 px-4 py-4 text-center text-xs cursor-pointer hover:font-semibold",
                activeTab === i &&
                  "border-b-2 border-b-primary font-bold hover:font-bold"
              )}
            >
              {tab}
            </div>
          ))}
        </div>
        <div className="bg-white rounded-xl  mt-2 min-h-[200px] max-h-[400px] overflow-auto">
          {searchResults.map((result) => (
            <SearchResultCard key={result.id} result={result} />
          ))}
        </div>
      </div>
    </motion.div>
  );
}

function SearchResultCard({ result }: { result: SearchResult }) {
  return (
    <Link href={`/company/${result.id}`}>
      <div className="flex  gap-2 items-center justify-between hover:bg-gray-100 p-4 group">
        <div className="flex gap-2 items-center">
          <Image
            src={"/farm-icon.png"}
            alt={result.name}
            width={25}
            height={25}
          />

          <div className="flex-col">
            <div className="text-sm font-medium group-hover:underline">
              {result.name}
            </div>
            <div className="text-xs text-muted-foreground">
              {result.address}
            </div>
          </div>
        </div>
        <div className="flex-col">
          <div className="text-xs text-muted-foreground">CVR: {result.cvr}</div>
        </div>
      </div>
    </Link>
  );
}
