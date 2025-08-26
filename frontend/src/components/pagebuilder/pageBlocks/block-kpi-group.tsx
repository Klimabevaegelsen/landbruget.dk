import { KPIGroup } from "@/services/supabase/types";
import { shouldShowPlaceholder } from "./chart-utils";
import { PlaceholderChart } from "./placeholder-chart";

export function BlockKpiGroup({ kpiGroup }: { kpiGroup: KPIGroup }) {
  // Check if this KPI group should show a placeholder
  const placeholderDataType = shouldShowPlaceholder(kpiGroup._key);
  if (placeholderDataType) {
    return <PlaceholderChart title={kpiGroup.title} dataType={placeholderDataType} />;
  }
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {kpiGroup.kpis.map((kpi, index) => (
        <div
          key={`${kpiGroup._key}-${index}`}
          className="rounded bg-primary-foreground p-4 flex flex-col gap-2"
        >
          <label className="text-sm font-medium">{kpi.label}</label>
          <p className="text-xl md:text-2xl font-bold text-green-900">
            {kpi.value}
          </p>
        </div>
      ))}
    </div>
  );
}
