import { CompanyProgressive } from '@/components/company/company-progressive';
import { getBasicCompanyById } from '@/services/data/company-basic';
import { notFound } from 'next/navigation';

interface CompanyPageProps {
  companyId: string;
}

export async function CompanyPage({ companyId }: CompanyPageProps) {
  try {
    await getBasicCompanyById(companyId);
  } catch {
    notFound();
  }

  return <CompanyProgressive companyId={companyId} />;
}
