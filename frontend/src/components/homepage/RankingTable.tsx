'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
      className="h-8 font-medium"
      onClick={() => handleSort(field)}
    >
      {children}
      <ArrowUpDown className="ml-2 h-4 w-4" />
    </Button>
  );

  return (
    <Card className="w-full" data-testid={testId}>
      <CardHeader className="card-header pb-4">
        <div className="flex items-start justify-between">
          <div className="space-y-2">
            <CardTitle
              className="text-lg font-semibold"
              data-testid="ranking-title"
            >
              {title}
            </CardTitle>
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
      </CardHeader>

      <CardContent className="card-content p-0">
        {items.length === 0 ? (
          <div className="px-6 py-8 text-center">
            <p className="text-muted-foreground text-sm">
              Ingen data tilgængelig for denne kategori
            </p>
          </div>
        ) : (
          <>
            <Table>
              <TableHeader>
                <TableRow className="border-b hover:bg-transparent">
                  <TableHead className="w-16">
                    <SortButton field="rank">Rang</SortButton>
                  </TableHead>
                  <TableHead>
                    <SortButton field="company_name">Virksomhed</SortButton>
                  </TableHead>
                  <TableHead className="hidden md:table-cell">
                    <SortButton field="municipality">Kommune</SortButton>
                  </TableHead>
                  <TableHead className="text-right">
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
                      <TableCell className="font-medium">
                        <div className="bg-muted flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold">
                          {item.rank}
                        </div>
                      </TableCell>

                      <TableCell>
                        <div className="space-y-1">
                          <div className="flex items-center space-x-2">
                            <Building2 className="text-muted-foreground h-4 w-4 flex-shrink-0" />
                            <span
                              className="truncate font-medium"
                              data-testid="company-link"
                            >
                              {item.company_name}
                            </span>
                            <ExternalLink className="text-muted-foreground h-3 w-3" />
                            {isCached && (
                              <Database className="text-primary h-3 w-3" />
                            )}
                          </div>
                          <div className="text-muted-foreground font-mono text-xs">
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

                      <TableCell className="text-right">
                        <div className="space-y-1">
                          <div className="font-semibold">
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
      </CardContent>
    </Card>
  );
}
