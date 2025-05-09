import { Company } from "@/components/company/company";
import { CompanySkeleton } from "@/components/skeleton/templates/company";
import { getCompanyById } from "@/services/supabase/company";
import { notFound } from "next/navigation";
import { Suspense } from "react";

type Props = {
  params: { id: string };
};

export async function CompanyPage({ id }: { id: string }) {
  const company = await getCompanyById(id);

  if (!company) {
    return notFound();
  }

  return <Company company={company} />;
}

export default async function CompanyPageWrapper({ params }: Props) {
  return (
    <Suspense fallback={<CompanySkeleton />}>
      <CompanyPage id={params.id} />
    </Suspense>
  );
}
