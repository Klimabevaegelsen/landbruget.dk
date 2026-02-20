'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { Database, ArrowRight, Table, HardDrive, FileSpreadsheet, Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { ManifestData } from '@/types';

export default function HomePage() {
  const [stats, setStats] = useState<{
    datasets: number;
    totalRows: number;
    totalSize: number;
  } | null>(null);
  const [loading, setLoading] = useState(true);

  // R2 base URL - should match your actual R2 bucket
  const R2_BASE_URL = process.env.NEXT_PUBLIC_R2_BASE_URL || 'https://your-r2-bucket.r2.dev';

  useEffect(() => {
    async function loadStats() {
      try {
        const manifestUrl = `${R2_BASE_URL}/manifest.json`;
        const response = await fetch(manifestUrl);

        if (!response.ok) {
          throw new Error('Failed to load manifest');
        }

        const manifest: ManifestData = await response.json();

        // Calculate statistics
        const totalRows = manifest.datasets.reduce((sum, ds) => sum + ds.rowCount, 0);
        const totalSize = manifest.datasets.reduce((sum, ds) => sum + ds.sizeBytes, 0);

        setStats({
          datasets: manifest.datasets.length,
          totalRows,
          totalSize,
        });
      } catch (err) {
        console.error('Error loading statistics:', err);
      } finally {
        setLoading(false);
      }
    }

    loadStats();
  }, [R2_BASE_URL]);

  function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  }

  function formatNumber(num: number): string {
    return new Intl.NumberFormat('da-DK').format(num);
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Hero Section */}
      <div className="border-b bg-gradient-to-b from-background to-muted/20">
        <div className="container mx-auto px-4 py-16 md:py-24">
          <div className="max-w-3xl mx-auto text-center space-y-6">
            <div className="inline-flex items-center gap-3 px-4 py-2 bg-primary/10 text-primary rounded-full text-sm font-semibold mb-4">
              <Database className="h-4 w-4" />
              <span>Danish Agricultural Data</span>
            </div>

            <h1 className="text-4xl md:text-6xl font-bold tracking-tight">
              Explore Agricultural Data
            </h1>

            <p className="text-lg md:text-xl text-muted-foreground max-w-2xl mx-auto">
              Query and analyze comprehensive datasets from Danish agricultural registries.
              Powered by DuckDB-WASM for instant browser-based analytics.
            </p>

            <div className="flex flex-col sm:flex-row gap-4 justify-center pt-6">
              <Link
                href="/explore"
                className={cn(
                  'inline-flex items-center justify-center gap-2',
                  'px-6 py-3 rounded-lg font-semibold text-base',
                  'bg-primary text-primary-foreground hover:bg-primary/90',
                  'transition-all shadow-lg hover:shadow-xl',
                  'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2'
                )}
              >
                <span>Start Exploring</span>
                <ArrowRight className="h-5 w-5" />
              </Link>

              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className={cn(
                  'inline-flex items-center justify-center gap-2',
                  'px-6 py-3 rounded-lg font-semibold text-base',
                  'bg-secondary text-secondary-foreground hover:bg-secondary/80',
                  'transition-all',
                  'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2'
                )}
              >
                <span>About Landbruget.dk</span>
              </a>
            </div>
          </div>
        </div>
      </div>

      {/* Statistics Section */}
      <div className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 max-w-4xl mx-auto">
          <StatCard
            icon={Table}
            label="Datasets"
            value={loading ? '...' : stats ? formatNumber(stats.datasets) : 'N/A'}
            description="Available tables"
          />
          <StatCard
            icon={FileSpreadsheet}
            label="Total Rows"
            value={loading ? '...' : stats ? formatNumber(stats.totalRows) : 'N/A'}
            description="Across all datasets"
          />
          <StatCard
            icon={HardDrive}
            label="Data Size"
            value={loading ? '...' : stats ? formatBytes(stats.totalSize) : 'N/A'}
            description="Compressed Parquet"
          />
        </div>
      </div>

      {/* Features Section */}
      <div className="container mx-auto px-4 py-16">
        <div className="max-w-5xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">
            Powerful Data Exploration
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            <FeatureCard
              icon={Sparkles}
              title="Natural Language Queries"
              description="Ask questions in plain language and get SQL queries generated automatically using AI."
            />
            <FeatureCard
              icon={Database}
              title="Browser-Based Analytics"
              description="No downloads required. Query millions of rows directly in your browser using DuckDB-WASM."
            />
            <FeatureCard
              icon={Table}
              title="Multiple Data Sources"
              description="Access data from CVR, CHR, BFE registries and more, all joinable and ready to explore."
            />
            <FeatureCard
              icon={FileSpreadsheet}
              title="Export to CSV"
              description="Download your query results as CSV files for further analysis in Excel, Python, or R."
            />
          </div>
        </div>
      </div>

      {/* CTA Section */}
      <div className="border-t bg-muted/30">
        <div className="container mx-auto px-4 py-16">
          <div className="max-w-3xl mx-auto text-center space-y-6">
            <h2 className="text-3xl font-bold">
              Ready to dive in?
            </h2>
            <p className="text-lg text-muted-foreground">
              Start exploring Danish agricultural data with powerful SQL queries and AI-assisted analysis.
            </p>
            <Link
              href="/explore"
              className={cn(
                'inline-flex items-center justify-center gap-2',
                'px-6 py-3 rounded-lg font-semibold text-base',
                'bg-primary text-primary-foreground hover:bg-primary/90',
                'transition-all shadow-lg hover:shadow-xl',
                'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2'
              )}
            >
              <span>Launch Explorer</span>
              <ArrowRight className="h-5 w-5" />
            </Link>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="border-t">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center text-sm text-muted-foreground">
            <p>
              Part of{' '}
              <a
                href="https://landbruget.dk"
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-foreground hover:text-primary transition-colors"
              >
                Landbruget.dk
              </a>
              {' '}- Organizing information about the Danish agricultural sector
            </p>
            <p className="mt-2 text-xs">
              Data updates weekly. All datasets are publicly available under open data licenses.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface StatCardProps {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string;
  description: string;
}

function StatCard({ icon: Icon, label, value, description }: StatCardProps) {
  return (
    <div className="rounded-lg border bg-card p-6 shadow-sm hover:shadow-md transition-shadow">
      <div className="flex items-center gap-3 mb-3">
        <div className="p-2 bg-primary/10 rounded-lg">
          <Icon className="h-5 w-5 text-primary" />
        </div>
        <span className="text-sm font-semibold text-muted-foreground">{label}</span>
      </div>
      <div className="text-3xl font-bold mb-1">{value}</div>
      <p className="text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

interface FeatureCardProps {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  description: string;
}

function FeatureCard({ icon: Icon, title, description }: FeatureCardProps) {
  return (
    <div className="flex gap-4">
      <div className="flex-shrink-0">
        <div className="p-3 bg-primary/10 rounded-lg">
          <Icon className="h-6 w-6 text-primary" />
        </div>
      </div>
      <div>
        <h3 className="text-lg font-semibold mb-2">{title}</h3>
        <p className="text-muted-foreground">{description}</p>
      </div>
    </div>
  );
}
