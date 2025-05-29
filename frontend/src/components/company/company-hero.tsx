import { CompanyResponse } from "@/services/supabase/types";
import { Container } from "../layout/container";
import Image from "next/image";
import { Button } from "../ui/button";
import { ArrowLeftIcon, ArrowDownIcon } from "@heroicons/react/24/outline";

export function CompanyHero({ company }: { company: CompanyResponse }) {
  return (
    <Container className="bg-foreground-darker" section>
      <div className="flex flex-col gap-20 md:flex-row">
        <div className="flex w-full flex-col gap-4">
          <div>
            <Button variant="secondary">
              <ArrowLeftIcon strokeWidth={2.5} className="size-3 text-green-900" />
              Tilbage til oversigt
            </Button>
          </div>
          <div className="flex flex-col gap-2">
            <div className="skeleton-item h-12 w-full"></div>
            <div className="skeleton-item h-12 w-full"></div>
            <div className="skeleton-item h-12 w-full"></div>
            <div className="skeleton-item flex h-12 w-full items-center justify-center">
              <span className="text-xs text-gray-400 italic">Placeholder</span>
            </div>
            <div className="skeleton-item h-12 w-full"></div>
            <div className="skeleton-item h-12 w-full"></div>
            <div className="skeleton-item h-12 w-full"></div>
          </div>
          <div>
            <Button>
              <ArrowDownIcon strokeWidth={2.5} className="size-3 text-white" />
              Download data (CSV)
            </Button>
          </div>
        </div>
        <div className="relative w-full">
          <Image
            src={"/img/placeholder/company-map.png"}
            alt={company.metadata.municipality}
            width={1000}
            height={1000}
          />
          <div className="absolute top-0 left-0 flex h-full w-full items-center justify-center">
            <p className="text-2xl font-bold text-green-900">Placeholder for map</p>
          </div>
        </div>
      </div>
    </Container>
  );
}
