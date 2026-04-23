import { CompanySkeleton } from '@/components/skeleton/templates/company';

export default function CompanyAliasLoading() {
  return (
    <div data-testid="company-loading">
      <CompanySkeleton />
    </div>
  );
}
