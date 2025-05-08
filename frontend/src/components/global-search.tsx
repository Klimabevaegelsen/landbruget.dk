"use client";
import { useEffect, useState } from "react";
import { Input } from "./ui/input";
import { cn } from "@/lib/utils";
import { MagnifyingGlassIcon } from "@heroicons/react/24/solid";
import { motion, AnimatePresence } from "framer-motion";
import React from "react";
import Image from "next/image";
import Link from "next/link";
import { Button } from "./ui/button";

export function GlobalSearch({
  className,
  defaultOpen,
  parentOpen,
  onClose,
  borderless,
  searchSuggestions,
}: {
  className?: string;
  defaultOpen?: boolean;
  parentOpen?: boolean;
  onClose?: () => void;
  borderless?: boolean;
  searchSuggestions?: string[];
}) {
  const [search, setSearch] = useState("");
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    if (parentOpen) setOpen(parentOpen);
  }, [parentOpen]);

  return (
    <div
      className={cn(
        " relative overflow flex flex-col gap-y-4 items-center w-full ",
        className,
        parentOpen === false && "hidden"
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
      {searchSuggestions && (
        <div className=" flex items-center justify-center gap-x-2">
          {searchSuggestions.map((suggestion) => (
            <Button
              key={suggestion}
              variant="secondary"
              className="bg-white/75 hover:bg-white/90"
              onClick={() => {
                setSearch(suggestion);
                setOpen(true);
              }}
            >
              <MagnifyingGlassIcon />
              {suggestion}
            </Button>
          ))}
        </div>
      )}
      <AnimatePresence>
        {open && (
          <SearchOverlay
            search={search}
            setSearch={setSearch}
            borderless={borderless}
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
  borderless,
}: {
  search: string;
  setSearch: (v: string) => void;
  onClose: () => void;
  borderless?: boolean;
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
      className="absolute  left-0 top-0 right-0 bottom-0 z-50  flex flex-col items-center justify-start"
      ref={overlayRef}
    >
      <div
        className={cn(
          "w-full  h-auto shadow-lg  rounded-lg",
          !borderless && "border border-gray-100"
        )}
      >
        <Input
          autoFocus
          type="text"
          placeholder="Søg efter CVR, firmanavn eller person"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-12 rounded-b-none rounded-t-lg border-none focus:ring-0 focus-visible:ring-0"
          endIcon={<MagnifyingGlassIcon className="size-6" />}
        />
        <div className="flex gap-2   bg-primary-foreground items-stretch overflow-x-auto">
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
        <div className="bg-white rounded-b-lg min-h-[200px] max-h-[400px] overflow-auto">
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
    <Link href={`/virksomhed/${result.id}`}>
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
