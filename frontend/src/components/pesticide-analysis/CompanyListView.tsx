'use client';

import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  ChevronLeft,
  ChevronRight,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Building2,
  Beaker,
} from 'lucide-react';
import { CompanySummary } from './types';

interface CompanyListViewProps {
  companies: CompanySummary[];
  totalCount: number;
  currentPage: number;
  limit: number;
  onCompanySelect: (company: CompanySummary) => void;
  onPageChange: (page: number) => void;
  selectedCompany: CompanySummary | null;
  sortBy: 'belastning' | 'applications' | 'area';
  sortOrder: 'asc' | 'desc';
  onSortChange: (
    sortBy: 'belastning' | 'applications' | 'area',
    sortOrder: 'asc' | 'desc'
  ) => void;
}

export function CompanyListView({
  companies,
  totalCount,
  currentPage,
  limit,
  onCompanySelect,
  onPageChange,
  selectedCompany,
  sortBy,
  sortOrder,
  onSortChange,
}: CompanyListViewProps) {
  const totalPages = Math.max(1, Math.ceil(totalCount / limit));
  const startResult = totalCount === 0 ? 0 : (currentPage - 1) * limit + 1;
  const endResult = Math.min(currentPage * limit, totalCount);

  const handleSort = (newSortBy: 'belastning' | 'applications' | 'area') => {
    if (sortBy === newSortBy) {
      // Toggle sort order if same column
      onSortChange(newSortBy, sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      // Default to descending for new column
      onSortChange(newSortBy, 'desc');
    }
  };

  const getSortIcon = (column: 'belastning' | 'applications' | 'area') => {
    if (sortBy !== column) {
      return <ArrowUpDown className="text-muted-foreground h-4 w-4" />;
    }
    return sortOrder === 'asc' ? (
      <ArrowUp className="text-primary h-4 w-4" />
    ) : (
      <ArrowDown className="text-primary h-4 w-4" />
    );
  };

  const formatBelastning = (value: number) => {
    return value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  };

  const getChemicalBadges = (company: CompanySummary) => {
    const badges = [];
    if (company.pfas_belastning > 0) {
      badges.push(
        <Badge key="pfas" variant="destructive" className="text-xs">
          PFAS: {formatBelastning(company.pfas_belastning)}
        </Badge>
      );
    }
    if (company.diquat_belastning > 0) {
      badges.push(
        <Badge key="diquat" variant="destructive" className="text-xs">
          Diquat: {formatBelastning(company.diquat_belastning)}
        </Badge>
      );
    }
    if (company.glyphosate_belastning > 0) {
      badges.push(
        <Badge key="glyphosate" variant="secondary" className="text-xs">
          Glyphosat: {formatBelastning(company.glyphosate_belastning)}
        </Badge>
      );
    }
    return badges;
  };

  return (
    <div className="space-y-4">
      {/* Sort Controls */}
      <div className="flex flex-wrap gap-2 border-b pb-2">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => handleSort('belastning')}
          className="flex items-center gap-1"
        >
          Belastning
          {getSortIcon('belastning')}
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => handleSort('applications')}
          className="flex items-center gap-1"
        >
          Anvendelser
          {getSortIcon('applications')}
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => handleSort('area')}
          className="flex items-center gap-1"
        >
          Areal
          {getSortIcon('area')}
        </Button>
      </div>

      {/* Company List */}
      <div className="space-y-2">
        {companies.length === 0 ? (
          <div className="bg-muted/40 text-muted-foreground rounded-lg border px-4 py-8 text-center text-sm">
            Den aktuelle eksport indeholder kun aggregerede pesticidtal for den
            valgte geografi.
          </div>
        ) : (
          companies.map((company) => (
            <div
              key={company.cvr_number}
              className={cn(
                'cursor-pointer rounded-lg border p-4 transition-all hover:shadow-md',
                selectedCompany?.cvr_number === company.cvr_number
                  ? 'border-primary bg-primary/10'
                  : 'border-border hover:border-border/80'
              )}
              onClick={() => onCompanySelect(company)}
            >
              <div className="mb-2 flex items-start justify-between">
                <div className="flex items-center gap-2">
                  <Building2 className="text-muted-foreground h-4 w-4" />
                  <div>
                    <h3 className="text-foreground text-sm font-semibold">
                      {company.company_name ||
                        `Virksomhed ${company.cvr_number}`}
                    </h3>
                    <p className="text-muted-foreground text-xs">
                      CVR: {company.cvr_number}
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-primary text-lg font-bold">
                    {formatBelastning(company.total_belastning)}
                  </div>
                  <div className="text-muted-foreground text-xs">
                    Belastning
                  </div>
                </div>
              </div>

              <div className="text-muted-foreground mb-2 flex items-center gap-4 text-xs">
                <div className="flex items-center gap-1">
                  <Beaker className="h-3 w-3" />
                  {company.total_applications} anvendelser
                </div>
                <div title="Behandlet areal for seneste år med data">
                  {company.total_treated_area_ha.toLocaleString('da-DK', {
                    maximumFractionDigits: 0,
                  })}{' '}
                  ha (seneste år)
                </div>
              </div>

              {/* Chemical Badges */}
              <div className="flex flex-wrap gap-1">
                {getChemicalBadges(company)}
              </div>

              {/* Years Active */}
              {company.years_active.length > 0 && (
                <div className="text-muted-foreground mt-2 text-xs">
                  Aktiv: {company.years_active.sort((a, b) => b - a).join(', ')}
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Pagination */}
      {totalCount > 0 && (
        <div className="flex items-center justify-between border-t pt-4">
          <div className="text-muted-foreground text-sm">
            Viser {startResult}-{endResult} af {totalCount.toLocaleString()}{' '}
            virksomheder
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => onPageChange(currentPage - 1)}
              disabled={currentPage <= 1}
            >
              <ChevronLeft className="h-4 w-4" />
              Forrige
            </Button>

            <span className="px-2 text-sm">
              Side {currentPage} af {totalPages}
            </span>

            <Button
              variant="outline"
              size="sm"
              onClick={() => onPageChange(currentPage + 1)}
              disabled={currentPage >= totalPages}
            >
              Næste
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
