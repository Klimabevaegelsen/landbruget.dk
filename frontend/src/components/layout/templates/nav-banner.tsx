import { CheckBadgeIcon } from "@heroicons/react/24/outline";
import { Container } from "../container";
import Link from "next/link";

export function NavBanner() {
  return (
    <div className="w-full h-10 bg-primary-foreground">
      <Container className="h-full" subclassName="h-full">
        <div className="flex size-full justify-center md:justify-between  ">
          <div className="flex gap-x-6">
            <p className="flex items-center gap-x-1 font-bold text-xs">
              <CheckBadgeIcon strokeWidth={2} className="size-4" />
              Fri adgang og <span className="underline">open source</span>
            </p>
            <p className="flex items-center gap-x-1 font-bold text-xs">
              <CheckBadgeIcon strokeWidth={2} className="size-4" />
              Valideret data
            </p>
            <p className="items-center gap-x-1 font-bold text-xs hidden md:flex">
              <CheckBadgeIcon strokeWidth={2} className="size-4" />
              Månedlig opdatering af data
            </p>
          </div>
          <div className="md:flex gap-x-6 hidden">
            <Link
              href="/?search=kilder"
              className="flex items-center text-xs font-medium hover:underline"
            >
              Kilder
            </Link>
            <Link
              href="/?search=om-os"
              className="flex items-center text-xs font-medium hover:underline"
            >
              Om os
            </Link>
          </div>
        </div>
      </Container>
    </div>
  );
}
