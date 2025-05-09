import { CompanyResponse } from "@/services/supabase/types";
import { Container } from "../layout/container";
import Image from "next/image";
import { Button } from "../ui/button";
import { ArrowLeftIcon, ArrowDownIcon } from "@heroicons/react/24/outline";

export function CompanyHero({ company }: { company: CompanyResponse }) {
  return (
    <Container className="bg-foreground-darker " section>
      <div className="flex flex-col  md:flex-row gap-20">
        <div className="flex flex-col  gap-4 w-full ">
          <div>
            <Button variant="secondary">
              <ArrowLeftIcon
                strokeWidth={2.5}
                className="size-3 text-green-900"
              />
              Tilbage til oversigt
            </Button>
          </div>
          <div className="flex flex-col gap-2">
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
            <div className="w-full h-12 skeleton-item"></div>
          </div>
          <div>
            <Button>
              <ArrowDownIcon strokeWidth={2.5} className="size-3 text-white" />
              Download data (CSV)
            </Button>
          </div>
        </div>
        <div className="w-full relative">
          <Image
            src={"/img/placeholder/company-map.png"}
            alt={company.metadata.municipality}
            width={1000}
            height={1000}
          />
          <div className="absolute top-0 left-0 w-full h-full  flex items-center justify-center">
            <p className="text-green-900 text-2xl font-bold">
              Placeholder for map
            </p>
          </div>
        </div>
      </div>
    </Container>
  );
}
