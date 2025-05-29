import { KPIGroup } from "@/services/supabase/types";

export function BlockKpiGroup({ kpiGroup }: { kpiGroup: KPIGroup }) {
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {kpiGroup.kpis.map((kpi, index) => (
        <div
          key={`${kpiGroup._key}-${index}`}
          className="bg-primary-foreground flex flex-col gap-2 rounded p-4"
        >
          <label className="text-sm font-medium">{kpi.label}</label>
          <p className="text-xl font-bold text-green-900 md:text-2xl">{kpi.value}</p>
        </div>
      ))}
    </div>
  );
}
