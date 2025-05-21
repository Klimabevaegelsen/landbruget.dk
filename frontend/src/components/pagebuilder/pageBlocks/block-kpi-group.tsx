import { KPIGroup } from "@/services/supabase/types";

export function BlockKpiGroup({ kpiGroup }: { kpiGroup: KPIGroup }) {
  return (
    <div className="grid grid-cols-2 gap-3">
      {kpiGroup.kpis.map((kpi, index) => (
        <div
          key={`${kpiGroup._key}-${index}`}
          className="rounded bg-primary-foreground p-4 flex flex-col gap-2"
        >
          <label className="text-sm font-medium">{kpi.label}</label>
          <p className="text-2xl font-bold text-green-900">{kpi.value}</p>
        </div>
      ))}
    </div>
  );
}
