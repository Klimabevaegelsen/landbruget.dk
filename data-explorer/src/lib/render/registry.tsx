"use client";

import { defineRegistry } from "@json-render/react";
import { shadcnComponents } from "@json-render/shadcn";
import { catalog } from "./catalog";
import { BarChartView } from "@/components/charts/BarChartView";
import { LineChartView } from "@/components/charts/LineChartView";
import { PieChartView } from "@/components/charts/PieChartView";
import { KPICard } from "@/components/charts/KPICard";
import { DataTableView } from "@/components/charts/DataTableView";
import { MapView } from "@/components/charts/MapView";

export const { registry } = defineRegistry(catalog, {
  components: {
    // shadcn built-in components
    Card: shadcnComponents.Card,
    Stack: shadcnComponents.Stack,
    Grid: shadcnComponents.Grid,
    Tabs: shadcnComponents.Tabs,
    Separator: shadcnComponents.Separator,
    Heading: shadcnComponents.Heading,
    Text: shadcnComponents.Text,
    Badge: shadcnComponents.Badge,
    Alert: shadcnComponents.Alert,
    Progress: shadcnComponents.Progress,
    Table: shadcnComponents.Table,

    // Custom chart components
    BarChart: ({ props }) => (
      <BarChartView
        data={props.data as Record<string, unknown>[]}
        xKey={props.xKey}
        yKey={props.yKey}
        title={props.title}
        color={props.color}
        height={props.height}
      />
    ),
    LineChart: ({ props }) => (
      <LineChartView
        data={props.data as Record<string, unknown>[]}
        xKey={props.xKey}
        yKeys={props.yKeys}
        title={props.title}
        height={props.height}
      />
    ),
    PieChart: ({ props }) => (
      <PieChartView
        data={props.data as Record<string, unknown>[]}
        nameKey={props.nameKey}
        valueKey={props.valueKey}
        title={props.title}
        height={props.height}
      />
    ),
    KPICard: ({ props }) => (
      <KPICard
        label={props.label}
        value={props.value}
        format={props.format}
        description={props.description}
      />
    ),
    DataTable: ({ props }) => (
      <DataTableView
        data={props.data as Record<string, unknown>[]}
        columns={props.columns}
        pageSize={props.pageSize}
      />
    ),
    MapView: ({ props }) => (
      <MapView
        data={props.data as Record<string, unknown>[]}
        latKey={props.latKey}
        lngKey={props.lngKey}
        labelKey={props.labelKey}
        popupKeys={props.popupKeys}
        title={props.title}
        height={props.height}
      />
    ),
  },
});
