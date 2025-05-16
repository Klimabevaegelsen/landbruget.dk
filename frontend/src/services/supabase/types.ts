// Metadata types
export interface Metadata {
  api_version: string;
  generated_at: string;
  config_version: string;
  data_updated_at: string | null;
}

export interface CompanyMetadata extends Metadata {
  company_id: string;
  company_cvr: string;
  municipality: string;
}

// Common types
export interface InfoCardItem {
  label: string;
  value: string;
}

export interface Column {
  key: string;
  label: string;
  column: string;
  isFilterable?: boolean;
  isHidden?: boolean;
  format?: string;
}

export interface KPI {
  key: string;
  label: string;
  value: string;
}

export interface ChartAxis {
  label: string;
  values: (string | number)[];
}

export interface ChartSeries {
  name: string;
  data: number[];
  type?: string;
  yAxis?: string;
}

export interface ChartData {
  xAxis: ChartAxis;
  series: ChartSeries[];
  yAxis: {
    label: string;
    values?: (string | number)[];
  };
}

export interface GeoJSONFeature {
  type: "Feature";
  geometry: {
    type: string;
    crs: {
      type: string;
      properties: {
        name: string;
      };
    };
    coordinates: number[][][];
  };
  properties: Record<string, string | number | boolean>;
}

export interface GeoJSONLayer {
  name: string;
  type: string;
  style: string;
  data: {
    type: "FeatureCollection";
    features: GeoJSONFeature[];
  };
}

export interface MapData {
  center: [number, number];
  zoom: number;
  layers: GeoJSONLayer[];
}

export interface TimelineEvent {
  date: string;
  description: string;
  event_type?: string;
}

export interface TimelineConfig {
  groupByColumns: string[];
  filterColumns: string[];
}

// Base interface for data grids
export interface BaseDataGrid {
  _key: string;
  title: string;
  rows: Record<string, string | number | boolean>[];
  columns: Column[];
  allowFiltering: boolean;
  isCollapsible?: boolean;
}

// Component types
export interface InfoCard {
  _key: string;
  _type: "infoCard";
  title: string;
  items: InfoCardItem[];
}

export interface DataGrid extends BaseDataGrid {
  _type: "dataGrid";
}

export interface KPIGroup {
  _key: string;
  _type: "kpiGroup";
  title: string;
  kpis: KPI[];
}

export interface MapChart {
  _key: string;
  _type: "mapChart";
  title: string;
  data: MapData;
}

export interface BarChart {
  _key: string;
  _type: "barChart";
  title: string;
  data: ChartData;
}

export interface StackedBarChart {
  _key: string;
  _type: "stackedBarChart";
  title: string;
  data: ChartData;
}

export interface HorizontalStackedBarChart {
  _key: string;
  _type: "horizontalStackedBarChart";
  title: string;
  data: ChartData;
}

export interface ComboChart {
  _key: string;
  _type: "comboChart";
  title: string;
  data: ChartData;
}

export interface Timeline {
  _key: string;
  _type: "timeline";
  title: string;
  events: TimelineEvent[];
  config: TimelineConfig;
}

export interface IteratedSection {
  _key: string;
  _type: "iteratedSection";
  title: string;
  iterationConfig: {
    layout: string;
    titleField: string;
  };
  sections: {
    title: string;
    layout: string;
    content: PageBuilderItem[];
  }[];
}

// Union type for all page builder items
export type PageBuilderItem =
  | InfoCard
  | DataGrid
  | KPIGroup
  | MapChart
  | BarChart
  | StackedBarChart
  | HorizontalStackedBarChart
  | ComboChart
  | Timeline
  | IteratedSection;

// Main response type
export interface PageBuilderResponse {
  metadata: Metadata;
  pageBuilder: PageBuilderItem[];
}

export interface CompanyResponse {
  metadata: CompanyMetadata;
  pageBuilder: PageBuilderItem[];
}
