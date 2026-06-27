'use client';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Building2,
  MapPin,
  ExternalLink,
  Database,
  ChevronDown,
  ArrowUpDown,
} from 'lucide-react';
import { useState } from 'react';
import { useCompanyNavigation } from '@/hooks/useCompanyNavigation';
import { useCompanyCache } from '@/hooks/useCompanyCache';
import { RankingItem } from '@/lib/rankings';
import { getCategoryColor, getCategoryLabel } from '@/lib/category-utils';

interface RankingTableEnhancedProps {
  id: string;
  title: string;
  category: string;
  description: string;
  unit: string;
  items: RankingItem[];
  last_updated?: string;
  showTop?: number;
  testId?: string;
}

type SortField = 'rank' | 'company_name' | 'value' | 'municipality';
type SortDirection = 'asc' | 'desc';

export function RankingTableEnhanced({
  title,
  category,
  description,
  items,
  showTop = 20,
  testId,
}: RankingTableEnhancedProps) {
  const { navigateToCompany } = useCompanyNavigation();
  const { getCompanyForDisplay } = useCompanyCache();

  const [sortField, setSortField] = useState<SortField>('rank');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [showAll, setShowAll] = useState(false);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  const sortedItems = [...items].sort((a, b) => {
    let aValue: string | number;
    let bValue: string | number;

    switch (sortField) {
      case 'rank':
        aValue = a.rank;
        bValue = b.rank;
        break;
      case 'company_name':
        aValue = a.company_name;
        bValue = b.company_name;
        break;
      case 'value':
        aValue = a.value;
        bValue = b.value;
        break;
      case 'municipality':
        aValue = a.municipality || '';
        bValue = b.municipality || '';
        break;
      default:
        return 0;
    }

    if (typeof aValue === 'string' && typeof bValue === 'string') {
      return sortDirection === 'asc'
        ? aValue.localeCompare(bValue)
        : bValue.localeCompare(aValue);
    }

    return sortDirection === 'asc'
      ? (aValue as number) - (bValue as number)
      : (bValue as number) - (aValue as number);
  });

  const displayItems = showAll ? sortedItems : sortedItems.slice(0, showTop);

  const SortButton = ({
    field,
    children,
  }: {
    field: SortField;
    children: React.ReactNode;
  }) => (
    <Button
      variant="ghost"
      size="sm"
      className="h-8 px-1 text-xs font-medium sm:px-3 sm:text-sm"
      onClick={() => handleSort(field)}
    >
      {children}
      <ArrowUpDown className="ml-1 hidden h-4 w-4 sm:block" />
    </Button>
  );

  return (
    <div
      className="bg-background text-foreground w-full max-w-full overflow-hidden rounded-lg border shadow-sm"
      data-testid={testId}
    >
      <div
        className="flex flex-col space-y-1.5 p-6 pb-4"
        data-testid="ranking-card-header"
      >
        <div className="flex items-start justify-between">
          <div className="space-y-2">
            <h3 className="text-lg font-semibold" data-testid="ranking-title">
              {title}
            </h3>
            <Badge
              variant="outline"
              className={`text-xs ${getCategoryColor(category)}`}
            >
              {getCategoryLabel(category)}
            </Badge>
          </div>
        </div>
        <p
          className="text-muted-foreground text-sm"
          data-testid="ranking-description"
        >
          {description}
        </p>
      </div>

      <div className="p-0" data-testid="ranking-card-content">
        {items.length === 0 ? (
          <div className="px-6 py-8 text-center">
            <p className="text-muted-foreground text-sm">
              Ingen data tilgængelig for denne kategori
            </p>
          </div>
        ) : (
          <>
            <Table className="table-fixed">
              <TableHeader>
                <TableRow className="border-b hover:bg-transparent">
                  <TableHead className="w-12 px-2 sm:w-16 sm:px-4">
                    <SortButton field="rank">Rang</SortButton>
                  </TableHead>
                  <TableHead className="px-2 sm:px-4">
                    <SortButton field="company_name">Virksomhed</SortButton>
                  </TableHead>
                  <TableHead className="hidden md:table-cell">
                    <SortButton field="municipality">Kommune</SortButton>
                  </TableHead>
                  <TableHead className="w-24 px-2 text-right sm:w-32 sm:px-4">
                    <SortButton field="value">Værdi</SortButton>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {displayItems.map((item) => {
                  const isCached =
                    getCompanyForDisplay(item.company_id) !== null;

                  return (
                    <TableRow
                      key={`${item.company_id}-${item.rank}`}
                      data-testid="ranking-row"
                      className="hover:bg-muted/50 cursor-pointer"
                      onClick={() =>
                        navigateToCompany(item.company_id, item.company_name)
                      }
                    >
                      <TableCell className="w-12 px-2 py-3 font-medium sm:w-16 sm:px-4">
                        <div className="bg-muted flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold">
                          {item.rank}
                        </div>
                      </TableCell>

                      <TableCell className="min-w-0 px-2 py-3 whitespace-normal sm:px-4">
                        <div className="min-w-0 space-y-1">
                          <div className="flex min-w-0 items-center space-x-2">
                            <Building2 className="text-muted-foreground h-4 w-4 flex-shrink-0" />
                            <a
                              href={`/company/${item.company_id}`}
                              className="min-w-0 truncate font-medium"
                              data-testid="company-link"
                              onClick={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                                navigateToCompany(
                                  item.company_id,
                                  item.company_name
                                );
                              }}
                            >
                              {item.company_name}
                            </a>
                            <ExternalLink className="text-muted-foreground hidden h-3 w-3 flex-shrink-0 sm:block" />
                            {isCached && (
                              <Database className="text-primary hidden h-3 w-3 flex-shrink-0 sm:block" />
                            )}
                          </div>
                          <div className="text-muted-foreground truncate font-mono text-xs">
                            CVR: {item.cvr_number}
                          </div>
                        </div>
                      </TableCell>

                      <TableCell className="hidden md:table-cell">
                        {item.municipality && (
                          <div className="text-muted-foreground flex items-center space-x-1 text-sm">
                            <MapPin className="h-3 w-3" />
                            <span>{item.municipality}</span>
                          </div>
                        )}
                      </TableCell>

                      <TableCell className="w-24 px-2 py-3 text-right whitespace-normal sm:w-32 sm:px-4">
                        <div className="space-y-1">
                          <div className="text-sm font-semibold break-words sm:text-base">
                            {item.formatted_value}
                          </div>
                          {item.year && (
                            <div className="text-muted-foreground text-xs">
                              {item.year}
                            </div>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>

            {items.length > showTop && (
              <div className="border-t p-4 text-center">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowAll(!showAll)}
                  className="w-full sm:w-auto"
                >
                  {showAll ? (
                    <>
                      Vis mindre
                      <ChevronDown className="ml-2 h-4 w-4 rotate-180" />
                    </>
                  ) : (
                    <>
                      Vis alle {items.length} virksomheder
                      <ChevronDown className="ml-2 h-4 w-4" />
                    </>
                  )}
                </Button>
                <p className="text-muted-foreground mt-2 text-xs">
                  Viser{' '}
                  {showAll
                    ? items.length
                    : Math.min(showTop, items.length)} af{' '}
                  {items.length} virksomheder
                </p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
