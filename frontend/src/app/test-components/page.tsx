'use client';

import { useState } from 'react';
import { GlobalSearch } from '@/components/ui/global-search';
import {
  AgriculturalBreadcrumb,
  NavigationPath,
} from '@/components/ui/agricultural-breadcrumb';
import { AgriculturalDashboard } from '@/components/ui/agricultural-dashboard';
import {
  CompanyHoverCard,
  FieldHoverCard,
} from '@/components/ui/company-hover-card';
import { NotificationDemo } from '@/components/ui/agricultural-notifications';
import {
  AgriculturalDatePicker,
  PesticideDatePicker,
  DateRange,
} from '@/components/ui/agricultural-date-picker';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Toaster } from '@/components/ui/sonner';

const testPath: NavigationPath = {
  municipality: {
    id: '101',
    name: 'Københavns Kommune',
    code: '101',
  },
  company: {
    id: '12345678',
    name: 'Dansk Landbrug A/S',
    cvr: '12345678',
  },
  field: {
    id: '12-4-567',
    name: 'Mark 12-4-567',
    blockId: '12',
    fieldId: '567',
  },
  currentPage: {
    name: 'Pesticid Analyse',
    type: 'analysis',
  },
};

// Mock data for testing
const mockCompany = {
  id: '12345678',
  name: 'Dansk Landbrug A/S',
  cvr: '12345678',
  municipality: 'Hovedgård Kommune',
  municipalityCode: '101',
  fieldCount: 15,
  totalArea: 234.5,
  organicFields: 3,
  riskLevel: 'low' as const,
  lastActivity: '2024-09-10',
  complianceScore: 92,
  primaryCrop: 'Vinterhvede',
  establishedYear: 1998,
};

const mockField = {
  id: '12-4-567',
  name: 'Mark 12-4-567',
  area: 15.2,
  cropType: 'Vinterhvede',
  organic: false,
  lastTreatment: '2024-08-15',
  riskLevel: 'low' as const,
  coordinates: [55.6761, 12.5683] as [number, number],
  blockId: '12',
  fieldId: '567',
  soilType: 'Lerjord',
  companyName: 'Dansk Landbrug A/S',
};

export default function TestComponentsPage() {
  const [dateRange, setDateRange] = useState<DateRange>({
    from: undefined,
    to: undefined,
  });

  return (
    <div className="bg-background min-h-screen">
      <Toaster />
      <div className="container mx-auto space-y-8 p-6">
        <h1 className="text-3xl font-bold">Midday-Inspired Components Test</h1>

        {/* Global Search Test */}
        <Card>
          <CardHeader>
            <CardTitle>Global Search Component</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Test the global search with Cmd+K (Mac) or Ctrl+K
                (Windows/Linux)
              </p>
              <GlobalSearch />
            </div>
          </CardContent>
        </Card>

        {/* Breadcrumb Test */}
        <Card>
          <CardHeader>
            <CardTitle>Agricultural Breadcrumb Navigation</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Hierarchical navigation: Danmark → Kommune → Virksomhed → Mark →
                Analysis
              </p>
              <AgriculturalBreadcrumb path={testPath} />
            </div>
          </CardContent>
        </Card>

        {/* Hover Card Test */}
        <Card>
          <CardHeader>
            <CardTitle>Hover Cards for Rich Previews</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Hover over the buttons below to see rich preview cards
              </p>
              <div className="flex gap-4">
                <CompanyHoverCard company={mockCompany}>
                  <Button variant="outline">
                    <Badge className="mr-2">CVR</Badge>
                    Hover for Company Details
                  </Button>
                </CompanyHoverCard>

                <FieldHoverCard field={mockField}>
                  <Button variant="outline">
                    <Badge className="mr-2">Mark</Badge>
                    Hover for Field Details
                  </Button>
                </FieldHoverCard>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Date Picker Test */}
        <Card>
          <CardHeader>
            <CardTitle>Agricultural Date Selection</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Context-aware date pickers with agricultural season presets
              </p>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                <div>
                  <label className="mb-2 block text-sm font-medium">
                    General Analysis Period
                  </label>
                  <AgriculturalDatePicker
                    dateRange={dateRange}
                    onDateRangeChange={setDateRange}
                    context="analysis"
                  />
                </div>
                <div>
                  <label className="mb-2 block text-sm font-medium">
                    Pesticide Application Period
                  </label>
                  <PesticideDatePicker
                    dateRange={dateRange}
                    onDateRangeChange={setDateRange}
                  />
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Notification Test */}
        <Card>
          <CardHeader>
            <CardTitle>Modern Agricultural Notifications</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Context-aware notifications with Sonner integration
              </p>
              <NotificationDemo />
            </div>
          </CardContent>
        </Card>

        {/* Resizable Dashboard Test */}
        <Card>
          <CardHeader>
            <CardTitle>Resizable Agricultural Dashboard</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="h-[600px]">
              <AgriculturalDashboard />
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
