'use client';

import { useState } from 'react';
import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from '@/components/ui/resizable';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import {
  Building2,
  Wheat,
  Map,
  BarChart3,
  Table2,
  Calendar,
  MapPin,
  TrendingUp,
  Activity,
  Droplets,
  Sprout,
} from 'lucide-react';

interface Company {
  id: string;
  name: string;
  cvr: string;
  municipality: string;
  fieldCount: number;
  totalArea: number;
  organicFields: number;
  riskLevel: 'low' | 'medium' | 'high';
}

interface FieldDetails {
  id: string;
  name: string;
  area: number;
  cropType: string;
  organic: boolean;
  lastTreatment?: string;
  riskLevel: 'low' | 'medium' | 'high';
  coordinates: [number, number];
}

// Mock data
const mockCompanies: Company[] = [
  {
    id: '1',
    name: 'Dansk Landbrug A/S',
    cvr: '12345678',
    municipality: 'Hovedgård Kommune',
    fieldCount: 15,
    totalArea: 234.5,
    organicFields: 3,
    riskLevel: 'low',
  },
  {
    id: '2',
    name: 'Økologisk Gård ApS',
    cvr: '87654321',
    municipality: 'Roskilde Kommune',
    fieldCount: 8,
    totalArea: 156.2,
    organicFields: 8,
    riskLevel: 'low',
  },
  {
    id: '3',
    name: 'Storgård Landbrug',
    cvr: '11223344',
    municipality: 'Aarhus Kommune',
    fieldCount: 42,
    totalArea: 567.8,
    organicFields: 12,
    riskLevel: 'medium',
  },
];

const mockFieldDetails: FieldDetails = {
  id: 'field-123',
  name: 'Mark 12-4-567',
  area: 15.2,
  cropType: 'Vinterhvede',
  organic: false,
  lastTreatment: '2024-08-15',
  riskLevel: 'low',
  coordinates: [55.6761, 12.5683],
};

export type ViewMode = 'map' | 'chart' | 'table' | 'calendar';

interface AgriculturalDashboardProps {
  className?: string;
  defaultLayout?: number[];
}

export function AgriculturalDashboard({
  className,
  defaultLayout = [25, 50, 25],
}: AgriculturalDashboardProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('map');
  const [selectedCompany, setSelectedCompany] = useState<Company | null>(
    mockCompanies[0]
  );

  const getRiskBadgeVariant = (risk: 'low' | 'medium' | 'high') => {
    switch (risk) {
      case 'low':
        return 'default';
      case 'medium':
        return 'secondary';
      case 'high':
        return 'destructive';
      default:
        return 'default';
    }
  };

  // const getRiskColor = (risk: 'low' | 'medium' | 'high') => {
  //   switch (risk) {
  //     case 'low':
  //       return 'text-green-600';
  //     case 'medium':
  //       return 'text-yellow-600';
  //     case 'high':
  //       return 'text-red-600';
  //     default:
  //       return 'text-gray-600';
  //   }
  // };

  return (
    <div className={className}>
      <ResizablePanelGroup direction="horizontal" className="min-h-screen">
        {/* Left Panel - Companies List */}
        <ResizablePanel
          defaultSize={defaultLayout[0]}
          minSize={20}
          maxSize={40}
        >
          <div className="flex h-full flex-col">
            <div className="border-b p-4">
              <h2 className="flex items-center gap-2 text-lg font-semibold">
                <Building2 className="h-5 w-5" />
                Virksomheder
              </h2>
              <p className="text-muted-foreground mt-1 text-sm">
                {mockCompanies.length} virksomheder fundet
              </p>
            </div>

            <ScrollArea className="flex-1">
              <div className="space-y-3 p-4">
                {mockCompanies.map((company) => (
                  <Card
                    key={company.id}
                    className={`cursor-pointer transition-all hover:shadow-md ${
                      selectedCompany?.id === company.id
                        ? 'ring-primary ring-2'
                        : ''
                    }`}
                    onClick={() => setSelectedCompany(company)}
                  >
                    <CardContent className="p-4">
                      <div className="space-y-2">
                        <div className="flex items-start justify-between">
                          <h3 className="text-sm leading-tight font-medium">
                            {company.name}
                          </h3>
                          <Badge
                            variant={getRiskBadgeVariant(company.riskLevel)}
                            className="text-xs"
                          >
                            {company.riskLevel}
                          </Badge>
                        </div>

                        <div className="text-muted-foreground space-y-1 text-xs">
                          <div className="flex items-center gap-1">
                            <Building2 className="h-3 w-3" />
                            CVR: {company.cvr}
                          </div>
                          <div className="flex items-center gap-1">
                            <MapPin className="h-3 w-3" />
                            {company.municipality}
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <div className="flex items-center gap-1">
                            <Wheat className="h-3 w-3" />
                            {company.fieldCount} marker
                          </div>
                          <div className="flex items-center gap-1">
                            <Activity className="h-3 w-3" />
                            {company.totalArea} ha
                          </div>
                          <div className="col-span-2 flex items-center gap-1">
                            <Sprout className="text-primary h-3 w-3" />
                            {company.organicFields} økologiske marker
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </ScrollArea>
          </div>
        </ResizablePanel>

        <ResizableHandle withHandle />

        {/* Center Panel - Main Content */}
        <ResizablePanel defaultSize={defaultLayout[1]} minSize={40}>
          <div className="flex h-full flex-col">
            {/* View Mode Toggle */}
            <div className="flex items-center justify-between border-b p-4">
              <h2 className="text-lg font-semibold">
                {selectedCompany ? selectedCompany.name : 'Vælg virksomhed'}
              </h2>

              <ToggleGroup
                type="single"
                value={viewMode}
                onValueChange={(value: ViewMode) => value && setViewMode(value)}
              >
                <ToggleGroupItem value="map" aria-label="Kortvisning" size="sm">
                  <Map className="h-4 w-4" />
                </ToggleGroupItem>
                <ToggleGroupItem
                  value="chart"
                  aria-label="Grafvisning"
                  size="sm"
                >
                  <BarChart3 className="h-4 w-4" />
                </ToggleGroupItem>
                <ToggleGroupItem
                  value="table"
                  aria-label="Tabelvisning"
                  size="sm"
                >
                  <Table2 className="h-4 w-4" />
                </ToggleGroupItem>
                <ToggleGroupItem
                  value="calendar"
                  aria-label="Kalendervisning"
                  size="sm"
                >
                  <Calendar className="h-4 w-4" />
                </ToggleGroupItem>
              </ToggleGroup>
            </div>

            {/* Main Content Area */}
            <div className="flex-1 p-4">
              {selectedCompany ? (
                <div className="bg-muted/20 flex h-full items-center justify-center rounded-lg border-2 border-dashed">
                  <div className="space-y-2 text-center">
                    {viewMode === 'map' && (
                      <>
                        <Map className="text-muted-foreground mx-auto h-12 w-12" />
                        <h3 className="text-lg font-medium">Kortvisning</h3>
                        <p className="text-muted-foreground text-sm">
                          Interaktivt kort med {selectedCompany.fieldCount}{' '}
                          marker
                        </p>
                      </>
                    )}
                    {viewMode === 'chart' && (
                      <>
                        <BarChart3 className="text-muted-foreground mx-auto h-12 w-12" />
                        <h3 className="text-lg font-medium">Grafvisning</h3>
                        <p className="text-muted-foreground text-sm">
                          Analyser og tendenser for {selectedCompany.name}
                        </p>
                      </>
                    )}
                    {viewMode === 'table' && (
                      <>
                        <Table2 className="text-muted-foreground mx-auto h-12 w-12" />
                        <h3 className="text-lg font-medium">Tabelvisning</h3>
                        <p className="text-muted-foreground text-sm">
                          Detaljerede data for alle marker
                        </p>
                      </>
                    )}
                    {viewMode === 'calendar' && (
                      <>
                        <Calendar className="text-muted-foreground mx-auto h-12 w-12" />
                        <h3 className="text-lg font-medium">Kalendervisning</h3>
                        <p className="text-muted-foreground text-sm">
                          Tidslinje for behandlinger og aktiviteter
                        </p>
                      </>
                    )}
                  </div>
                </div>
              ) : (
                <div className="text-muted-foreground flex h-full items-center justify-center">
                  <div className="space-y-2 text-center">
                    <Building2 className="mx-auto h-12 w-12" />
                    <p>Vælg en virksomhed for at se detaljer</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </ResizablePanel>

        <ResizableHandle withHandle />

        {/* Right Panel - Field Details */}
        <ResizablePanel
          defaultSize={defaultLayout[2]}
          minSize={20}
          maxSize={40}
        >
          <div className="flex h-full flex-col">
            <div className="border-b p-4">
              <h2 className="flex items-center gap-2 text-lg font-semibold">
                <Wheat className="h-5 w-5" />
                Mark Detaljer
              </h2>
            </div>

            <ScrollArea className="flex-1">
              <div className="space-y-4 p-4">
                {selectedCompany ? (
                  <Card>
                    <CardHeader className="pb-3">
                      <CardTitle className="flex items-center justify-between text-base">
                        {mockFieldDetails.name}
                        <Badge
                          variant={getRiskBadgeVariant(
                            mockFieldDetails.riskLevel
                          )}
                        >
                          {mockFieldDetails.riskLevel}
                        </Badge>
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                      <div className="grid grid-cols-1 gap-3 text-sm">
                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground">Areal:</span>
                          <span className="font-medium">
                            {mockFieldDetails.area} ha
                          </span>
                        </div>

                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground">
                            Afgrøde:
                          </span>
                          <span className="font-medium">
                            {mockFieldDetails.cropType}
                          </span>
                        </div>

                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground">
                            Økologisk:
                          </span>
                          <Badge
                            variant={
                              mockFieldDetails.organic ? 'default' : 'secondary'
                            }
                            className="text-xs"
                          >
                            {mockFieldDetails.organic ? 'Ja' : 'Nej'}
                          </Badge>
                        </div>

                        {mockFieldDetails.lastTreatment && (
                          <div className="flex items-center justify-between">
                            <span className="text-muted-foreground">
                              Sidst behandlet:
                            </span>
                            <span className="text-xs font-medium">
                              {new Date(
                                mockFieldDetails.lastTreatment
                              ).toLocaleDateString('da-DK')}
                            </span>
                          </div>
                        )}

                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground">
                            Koordinater:
                          </span>
                          <span className="font-mono text-xs">
                            {mockFieldDetails.coordinates[0].toFixed(4)},{' '}
                            {mockFieldDetails.coordinates[1].toFixed(4)}
                          </span>
                        </div>
                      </div>

                      <div className="space-y-2 border-t pt-3">
                        <h4 className="flex items-center gap-2 text-sm font-medium">
                          <TrendingUp className="h-4 w-4" />
                          Seneste Aktivitet
                        </h4>
                        <div className="text-muted-foreground space-y-2 text-xs">
                          <div className="flex items-center gap-2">
                            <Droplets className="h-3 w-3" />
                            <span>Pesticidbehandling - 15. aug 2024</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Sprout className="h-3 w-3" />
                            <span>Såning - 2. maj 2024</span>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ) : (
                  <div className="text-muted-foreground py-8 text-center">
                    <Wheat className="mx-auto mb-2 h-8 w-8" />
                    <p className="text-sm">Vælg virksomhed for detaljer</p>
                  </div>
                )}
              </div>
            </ScrollArea>
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
    </div>
  );
}
