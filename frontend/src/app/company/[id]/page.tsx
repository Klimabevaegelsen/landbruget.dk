import { CompanyPage as CompanyPageContent } from '@/components/company/company-page';

type Props = {
  params: Promise<{ id: string }>;
};

export const revalidate = 3600;

export async function generateStaticParams() {
  return [];
}

export default async function CompanyAliasPage({ params }: Props) {
  const { id } = await params;

  return <CompanyPageContent companyId={id} />;
}
