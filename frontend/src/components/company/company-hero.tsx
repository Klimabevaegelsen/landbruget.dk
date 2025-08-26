import { CompanyResponse } from "@/services/supabase/types";
import { Container } from "../layout/container";
import { Button } from "../ui/button";
import { ArrowLeftIcon, ArrowDownIcon } from "@heroicons/react/24/outline";
import { BlockMapChart } from "../pagebuilder/pageBlocks/block-map-chart";

export function CompanyHero({ company }: { company: CompanyResponse }) {
  // Find the company identity and map components from pageBuilder
  const companyIdentity = company.pageBuilder.find(block => block._key === 'company-identity');
  const companyMap = company.pageBuilder.find(block => block._key === 'company-map-overview');

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

          {/* Company Identity Info */}
          <div className="space-y-3">
            {companyIdentity && companyIdentity._type === 'infoCard' && (
              <>
                <h1 className="text-3xl font-bold text-white">
                  {companyIdentity.items.find(item => item.label === 'Navn')?.value || 'Virksomhed'}
                </h1>
                <div className="space-y-2 text-white/90">
                  <p className="text-lg">
                    CVR: {companyIdentity.items.find(item => item.label === 'CVR')?.value}
                  </p>
                  <p className="text-base">
                    {companyIdentity.items.find(item => item.label === 'Adresse')?.value}
                  </p>
                  <p className="text-base">
                    {companyIdentity.items.find(item => item.label === 'Postnummer')?.value} {companyIdentity.items.find(item => item.label === 'By')?.value}
                  </p>
                </div>
              </>
            )}
          </div>

          <div>
            <Button>
              <ArrowDownIcon strokeWidth={2.5} className="size-3 text-white" />
              Download data (CSV)
            </Button>
          </div>
        </div>

        <div className="w-full relative">
          {/* Company Map */}
          {companyMap && companyMap._type === 'mapChart' && (
            <BlockMapChart chart={companyMap} />
          )}
        </div>
      </div>
    </Container>
  );
}
