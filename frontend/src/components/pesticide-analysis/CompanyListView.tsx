'use client';

import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ChevronLeft, ChevronRight, ArrowUpDown, ArrowUp, ArrowDown, Building2, MapPin, Beaker } from 'lucide-react';
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
  onSortChange: (sortBy: 'belastning' | 'applications' | 'area', sortOrder: 'asc' | 'desc') => void;
}

export default function CompanyListView({
  companies,
  totalCount,
  currentPage,
  limit,
  onCompanySelect,
  onPageChange,
  selectedCompany,
  sortBy,
  sortOrder,
  onSortChange
}: CompanyListViewProps) {
  const totalPages = Math.ceil(totalCount / limit);
  const startResult = (currentPage - 1) * limit + 1;
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
      return <ArrowUpDown className="h-4 w-4 text-gray-400" />;
    }
    return sortOrder === 'asc' ?
      <ArrowUp className="h-4 w-4 text-blue-600" /> :
      <ArrowDown className="h-4 w-4 text-blue-600" />;
  };

  const formatBelastning = (value: number) => {
    return value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1
    }  );
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
      <div className="flex flex-wrap gap-2 pb-2 border-b">
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
        {companies.map((company) => (
          <div
            key={company.cvr_number}
            className={`p-4 border rounded-lg cursor-pointer transition-all hover:shadow-md ${
              selectedCompany?.cvr_number === company.cvr_number
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-200 hover:border-gray-300'
            }`}
            onClick={() => onCompanySelect(company)}
          >
            <div className="flex items-start justify-between mb-2">
              <div className="flex items-center gap-2">
                <Building2 className="h-4 w-4 text-gray-500" />
                <div>
                  <h3 className="font-semibold text-sm">
                    {company.company_name || `Virksomhed ${company.cvr_number}`}
                  </h3>
                  <p className="text-xs text-gray-500">CVR: {company.cvr_number}</p>
                </div>
              </div>
              <div className="text-right">
                <div className="text-lg font-bold text-blue-600">
                  {formatBelastning(company.total_belastning)}
                </div>
                <div className="text-xs text-gray-500">Belastning</div>
              </div>
            </div>

            <div className="flex items-center gap-4 text-xs text-gray-600 mb-2">
              <div className="flex items-center gap-1">
                <MapPin className="h-3 w-3" />
                {company.municipality !== 'Municipality TBD' ? company.municipality : 'Ukendt kommune'}
              </div>
              <div className="flex items-center gap-1">
                <Beaker className="h-3 w-3" />
                {company.total_applications} anvendelser
              </div>
              <div>
                {company.total_treated_area_ha.toLocaleString('da-DK', { maximumFractionDigits: 0 })} ha
              </div>
            </div>

            {/* Chemical Badges */}
            <div className="flex flex-wrap gap-1">
              {getChemicalBadges(company)}
            </div>

            {/* Years Active */}
            {company.years_active.length > 0 && (
              <div className="mt-2 text-xs text-gray-500">
                Aktiv: {company.years_active.sort((a, b) => b - a).join(', ')}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between pt-4 border-t">
        <div className="text-sm text-gray-600">
          Viser {startResult}-{endResult} af {totalCount.toLocaleString()} virksomheder
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

          <span className="text-sm px-2">
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
    </div>
  );
}
