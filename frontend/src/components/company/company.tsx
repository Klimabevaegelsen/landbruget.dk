import { useEffect } from "react";
import { CompanyResponse } from "@/services/supabase/types";
import { CompanyHero } from "./company-hero";
import { Container } from "../layout/container";
import { PageBuilder } from "../pagebuilder/pagebuilder";
import { useToast } from "@/components/ui/toast";

export function Company({ company }: { company: CompanyResponse }) {
  const { clearLoadingToasts } = useToast();

  // Clear any loading toasts when the company page loads
  useEffect(() => {
    clearLoadingToasts();
  }, [clearLoadingToasts]);

  // Filter out components that are already displayed in the hero section
  const filteredPageBuilder = company.pageBuilder.filter(block =>
    block._key !== 'company-identity' && block._key !== 'company-map-overview'
  );

  return (
    <article>
      <CompanyHero company={company} />
      <Container section>
        <PageBuilder pageBlocks={filteredPageBuilder} />
      </Container>
    </article>
  );
}
