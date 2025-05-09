import { CompanyResponse } from "@/services/supabase/types";
import { CompanyHero } from "./company-hero";
import { Container } from "../layout/container";
import { PageBuilder } from "../pagebuilder/pagebuilder";

export function Company({ company }: { company: CompanyResponse }) {
  return (
    <article>
      <CompanyHero company={company} />
      <Container section>
        <PageBuilder pageBlocks={company.pageBuilder} />
      </Container>
    </article>
  );
}
