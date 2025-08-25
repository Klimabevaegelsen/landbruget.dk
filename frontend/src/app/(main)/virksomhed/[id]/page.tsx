import { Company } from "@/components/company/company";
import { getCompanyById } from "@/services/supabase/company";
import { notFound } from "next/navigation";

type Props = {
  params: Promise<{ id: string }>;
};

export const revalidate = 3600;

export async function generateStaticParams() {
  return [];
}

export default async function CompanyPage({ params }: Props) {
  const { id } = await params;
  const company = await getCompanyById(id);

  if (!company) {
    return notFound();
  }



  return <Company company={company} />;
}
