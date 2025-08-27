"use client";

import Link from "next/link";
import { ChevronDownIcon } from "lucide-react";
import { Container } from "../container";
import { Logo } from "./logo";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";

const legalLinks = [
  { href: "/om-os", label: "Om Landbruget.dk" },
  { href: "/brugsvilkaar", label: "Brugsvilkår" },
  { href: "/privatlivspolitik", label: "Privatlivspolitik" },
  { href: "/", label: "Kilder" },
  { href: "/", label: "Download" },
];

export function Footer() {
  return (
    <div className="bg-primary-foreground">
      <Container>
        <div className="flex flex-col gap-4 lg:flex-row py-6 justify-between items-center">
          <Logo className="h-[26px]" />

          {/* Desktop Navigation */}
          <div className="hidden lg:flex gap-6">
            {legalLinks.map((link) => (
              <Link
                key={link.href + link.label}
                className="text-sm font-medium hover:underline"
                href={link.href}
              >
                {link.label}
              </Link>
            ))}
          </div>

          {/* Mobile Navigation Dropdown */}
          <div className="flex lg:hidden">
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-auto p-2 text-sm font-medium"
                >
                  Juridisk info
                  <ChevronDownIcon className="ml-1 h-4 w-4" />
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-48 p-2" align="center">
                <div className="flex flex-col space-y-1">
                  {legalLinks.map((link) => (
                    <Link
                      key={link.href + link.label}
                      className="px-3 py-2 text-sm font-medium hover:bg-gray-100 rounded-md transition-colors"
                      href={link.href}
                    >
                      {link.label}
                    </Link>
                  ))}
                </div>
              </PopoverContent>
            </Popover>
          </div>

          <p className="text-sm">© Copyright 2025 Landbruget.dk</p>
        </div>
      </Container>
    </div>
  );
}
