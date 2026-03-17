"use client";

import { Renderer } from "@json-render/react";
import { registry } from "@/lib/render/registry";
import { Loader2, AlertTriangle } from "lucide-react";
import type { Spec } from "@/lib/render/spec-utils";

interface SmartResultsProps {
  spec: Spec | null;
  loading: boolean;
  error: string | null;
}

export function SmartResults({ spec, loading, error }: SmartResultsProps) {
  if (loading) {
    return (
      <div className="flex items-center gap-3 py-6">
        <Loader2
          className="h-4 w-4 flex-shrink-0 animate-spin"
          style={{ color: "var(--primary)" }}
        />
        <span
          className="text-[11px] tracking-wider uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          Generating visualization…
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="border-l-2 py-2 pl-4" style={{ borderColor: "var(--destructive)" }}>
        <div className="flex items-start gap-2.5">
          <AlertTriangle
            className="mt-0.5 h-3.5 w-3.5 flex-shrink-0"
            style={{ color: "var(--destructive)" }}
          />
          <div>
            <p className="text-xs leading-relaxed" style={{ color: "var(--destructive)" }}>
              Could not generate visualization: {error}
            </p>
            <p className="mt-0.5 text-[11px]" style={{ color: "var(--muted-foreground)" }}>
              Showing table view instead.
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (!spec) return null;

  return (
    <div>
      <div className="mb-4">
        <span
          className="text-[10px] font-semibold tracking-[0.2em] uppercase"
          style={{ color: "var(--muted-foreground)" }}
        >
          Visualization
        </span>
      </div>
      <Renderer
        spec={spec}
        registry={registry}
        fallback={({ element }) => (
          <div
            className="border-l-2 py-2 pl-4 text-[11px]"
            style={{
              borderColor: "var(--border)",
              color: "var(--muted-foreground)",
            }}
          >
            Unknown component: {element.type}
          </div>
        )}
      />
    </div>
  );
}
