import { CompanyResponse } from "@/services/supabase/types";
import { CompanyHero } from "./company-hero";
import { Container } from "../layout/container";
import { PageBuilder } from "../pagebuilder/pagebuilder";

export function Company({ company }: { company: CompanyResponse }) {
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
