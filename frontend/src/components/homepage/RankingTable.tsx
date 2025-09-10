'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Building2, MapPin, ExternalLink, Database } from 'lucide-react';
import { useCompanyNavigation } from '@/hooks/useCompanyNavigation';
import { useCompanyCache } from '@/hooks/useCompanyCache';

interface RankingItem {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  rank: number;
  value: number;
  formatted_value: string;
  year?: number;
}

interface RankingTableProps {
  id: string;
  title: string;
  category: string;
  description: string;
  unit: string;
  items: RankingItem[];
  last_updated?: string;
  showTop?: number; // Show only top N items, default shows all
}

const getRankIcon = (rank: number) => {
  return <span className="text-sm font-medium text-gray-900">{rank}</span>;
};

const getCategoryColor = (category: string) => {
  switch (category) {
    case 'financial':
      return 'bg-green-100 text-green-800 border-green-200';
    case 'field':
      return 'bg-emerald-100 text-emerald-800 border-emerald-200';
    case 'environment':
      return 'bg-red-100 text-red-800 border-red-200';
    case 'animal':
      return 'bg-orange-100 text-orange-800 border-orange-200';
    case 'worker':
      return 'bg-blue-100 text-blue-800 border-blue-200';
    default:
      return 'bg-gray-100 text-gray-800 border-gray-200';
  }
};

const getCategoryLabel = (category: string) => {
  switch (category) {
    case 'financial':
      return 'Økonomi';
    case 'field':
      return 'Landbrugsareal';
    case 'environment':
      return 'Miljø';
    case 'animal':
      return 'Husdyr';
    case 'worker':
      return 'Medarbejdere';
    default:
      return category;
  }
};

export default function RankingTable({
  title,
  category,
  description,
  items,
  showTop = 20,
}: RankingTableProps) {
  const { navigateToCompany } = useCompanyNavigation();
  const { getCompanyForDisplay } = useCompanyCache();
  const displayItems = items.slice(0, showTop);

  return (
    <Card className="w-full">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="space-y-2">
            <CardTitle className="text-lg font-semibold text-gray-900">
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
        <p className="text-muted-foreground text-sm">{description}</p>
      </CardHeader>

      <CardContent className="p-0">
        <div className="divide-border divide-y">
          {displayItems.map((item) => {
            const isCached = getCompanyForDisplay(item.company_id) !== null;

            return (
              <div
                key={`${item.company_id}-${item.rank}`}
                className="hover:bg-muted/50 flex items-center justify-between px-6 py-3 transition-colors"
              >
                <div className="flex min-w-0 flex-1 items-center space-x-3">
                  <div className="flex w-8 flex-shrink-0 justify-center">
                    {getRankIcon(item.rank)}
                  </div>

                  <div className="min-w-0 flex-1">
                    <div className="flex items-center space-x-2">
                      <Building2 className="text-muted-foreground h-4 w-4 flex-shrink-0" />
                      <button
                        onClick={() =>
                          navigateToCompany(item.company_id, item.company_name)
                        }
                        className="text-foreground hover:text-primary truncate text-left text-sm font-medium transition-colors"
                      >
                        {item.company_name}
                      </button>
                      <ExternalLink className="text-muted-foreground h-3 w-3" />
                      {isCached && (
                        <Database className="text-primary h-3 w-3" />
                      )}
                    </div>

                    <div className="mt-1 flex items-center space-x-4">
                      <div className="text-muted-foreground flex items-center space-x-1 text-xs">
                        <span>CVR:</span>
                        <span className="font-mono">{item.cvr_number}</span>
                      </div>
                      {item.municipality && (
                        <div className="text-muted-foreground flex items-center space-x-1 text-xs">
                          <MapPin className="h-3 w-3" />
                          <span>{item.municipality}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                <div className="flex-shrink-0 text-right">
                  <div className="text-foreground text-sm font-semibold">
                    {item.formatted_value}
                  </div>
                  {item.year && (
                    <div className="text-muted-foreground text-xs">
                      {item.year}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {items.length > showTop && (
          <div className="border-t bg-gray-50 px-6 py-3">
            <p className="text-center text-xs text-gray-500">
              Viser top {showTop} af {items.length} virksomheder
            </p>
          </div>
        )}

        {items.length === 0 && (
          <div className="px-6 py-8 text-center">
            <p className="text-sm text-gray-500">
              Ingen data tilgængelig for denne kategori
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
