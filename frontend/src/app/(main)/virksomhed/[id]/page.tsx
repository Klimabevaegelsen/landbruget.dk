import { CompanyProgressive } from '@/components/company/company-progressive';
import { getBasicCompanyById } from '@/services/supabase/company-basic';
import { notFound } from 'next/navigation';

type Props = {
  params: Promise<{ id: string }>;
};

export const revalidate = 3600;

export async function generateStaticParams() {
  return [];
}

export default async function CompanyPage({ params }: Props) {
  const { id } = await params;

  try {
    // Only verify the company exists with basic info
    await getBasicCompanyById(id);
  } catch {
    return notFound();
  }

  return <CompanyProgressive companyId={id} />;
}
