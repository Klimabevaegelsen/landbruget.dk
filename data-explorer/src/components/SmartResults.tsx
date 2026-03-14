'use client';

import { Renderer } from '@json-render/react';
import { registry } from '@/lib/render/registry';
import { Loader2 } from 'lucide-react';
import type { Spec } from '@/lib/render/spec-utils';

interface SmartResultsProps {
  spec: Spec | null;
  loading: boolean;
  error: string | null;
}

export function SmartResults({ spec, loading, error }: SmartResultsProps) {
  if (loading) {
    return (
      <div className="bg-card flex items-center gap-3 rounded-lg border p-6">
        <Loader2 className="text-primary h-5 w-5 animate-spin" />
        <span className="text-muted-foreground text-sm">
          Generating visualization...
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-amber-500/50 bg-amber-500/10 px-4 py-3">
        <p className="text-sm text-amber-800 dark:text-amber-200">
          Could not generate visualization: {error}
        </p>
        <p className="mt-1 text-xs text-amber-700 dark:text-amber-300">
          Showing table view instead.
        </p>
      </div>
    );
  }

  if (!spec) return null;

  return (
    <div className="bg-card rounded-lg border p-6">
      <Renderer
        spec={spec}
        registry={registry}
        fallback={({ element }) => (
          <div className="text-muted-foreground rounded border border-dashed p-4 text-sm">
            Unknown component: {element.type}
          </div>
        )}
      />
    </div>
  );
}
