'use client';

import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { DynamicDataTable } from '@/components/table/dynamic-table';
import { ColumnDef } from '@tanstack/react-table';
import { Building2, ExternalLink } from 'lucide-react';
import Link from 'next/link';
import { RankingItem } from '@/types/rankings';
import { getCategoryColor, getCategoryLabel } from '@/lib/category-utils';

interface IndividualRankingTableProps {
  rankingId: string;
  title: string;
  category: string;
  description: string;
  initialLimit?: number;
}

export default function IndividualRankingTable({
  rankingId,
  title,
  category,
  description,
  initialLimit = 20,
}: IndividualRankingTableProps) {
  const [data, setData] = useState<RankingItem[]>([]);
  const [companyCount, setCompanyCount] = useState<number>(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const columns: ColumnDef<RankingItem>[] = [
    {
      accessorKey: 'rank',
      header: 'Rang',
      cell: ({ row }) => {
        const rank = row.getValue('rank') as number;
        return (
          <div className="text-foreground flex h-8 w-8 items-center justify-center text-sm font-bold">
            {rank}
          </div>
        );
      },
    },
    {
      accessorKey: 'company_name',
      header: 'Virksomhed',
      cell: ({ row }) => {
        const item = row.original;
        return (
          <div className="space-y-1">
            <div className="flex items-center space-x-2">
              <Building2 className="text-muted-foreground h-4 w-4 flex-shrink-0" />
              <Link
                href={`/virksomhed/${item.company_id}`}
                className="text-foreground hover:text-primary truncate text-sm font-medium transition-colors"
              >
                {item.company_name}
              </Link>
              <ExternalLink className="text-muted-foreground h-3 w-3" />
            </div>
            <div className="text-muted-foreground text-xs">
              CVR: {item.cvr_number}
              {item.municipality && ` • ${item.municipality}`}
            </div>
          </div>
        );
      },
    },
    {
      accessorKey: 'formatted_value',
      header: 'Værdi',
      cell: ({ row }) => {
        const item = row.original;
        return (
          <div className="text-right">
            <div className="text-foreground text-sm font-semibold">
              {item.formatted_value}
            </div>
            {item.year && (
              <div className="text-muted-foreground text-xs">{item.year}</div>
            )}
          </div>
        );
      },
    },
  ];

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        category,
        limit: initialLimit.toString(),
        rankingId, // Pass specific ranking ID to get only this ranking
      });

      const response = await fetch(
        `/api/supabase/functions/homepage-rankings?${params}`
      );

      if (!response.ok) {
        throw new Error(`API error: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();

      // Find the specific ranking from the response
      const ranking = result.rankings?.find(
        (r: { id: string }) => r.id === rankingId
      );

      if (ranking) {
        setData(ranking.items || []);
        setCompanyCount(ranking.company_count || 0);
      } else {
        setData([]);
        setCompanyCount(0);
      }
    } catch (err) {
      console.error(`Error fetching ranking ${rankingId}:`, err);
      setError(err instanceof Error ? err.message : 'Failed to load ranking');
    } finally {
      setLoading(false);
    }
  }, [rankingId, category, initialLimit]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  if (loading) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-center py-8">
            <div className="h-6 w-6 animate-spin rounded-full border-b-2 border-gray-400"></div>
            <span className="text-muted-foreground ml-2 text-sm">
              Indlæser...
            </span>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="py-8 text-center">
            <div className="border-destructive/20 bg-destructive/10 rounded-lg border p-4">
              <p className="text-destructive font-medium">
                Fejl ved indlæsning
              </p>
              <p className="text-destructive/80 mt-1 text-sm">{error}</p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="w-full">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="space-y-2">
            <div className="flex items-center gap-3">
              <CardTitle className="text-foreground text-lg font-semibold">
                {title}
              </CardTitle>
              {companyCount > 0 && (
                <Badge variant="secondary" className="text-xs">
                  {companyCount} virksomheder
                </Badge>
              )}
            </div>
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
        {data.length > 0 ? (
          <div className="px-6 pb-6">
            <DynamicDataTable
              columns={columns}
              data={data}
              filterable={false}
            />
          </div>
        ) : (
          <div className="px-6 py-8 text-center">
            <p className="text-muted-foreground text-sm">
              Ingen data tilgængelig for denne rangliste
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
