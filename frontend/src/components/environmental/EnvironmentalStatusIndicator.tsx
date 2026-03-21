import React from 'react';
import {
  CheckCircleIcon,
  ExclamationTriangleIcon,
  WrenchScrewdriverIcon,
} from '@heroicons/react/24/outline';

interface EnvironmentalStatusProps {
  status: 'compliant' | 'action_required' | 'water_covered' | 'unknown';
  label: string;
  hectares?: number;
  percentage?: number;
  className?: string;
}

const STATUS_CONFIG = {
  compliant: {
    icon: CheckCircleIcon,
    color: 'text-organic',
    bgColor: 'bg-organic/10',
    borderColor: 'border-organic/20',
    label: 'Gennemført',
    description: 'Miljøkrav er opfyldt',
  },
  action_required: {
    icon: ExclamationTriangleIcon,
    color: 'text-conventional',
    bgColor: 'bg-conventional/10',
    borderColor: 'border-conventional/20',
    label: 'Kræver Handling',
    description: 'Miljøtiltag påkrævet',
  },
  water_covered: {
    icon: CheckCircleIcon,
    color: 'text-bnbo',
    bgColor: 'bg-bnbo/10',
    borderColor: 'border-bnbo/20',
    label: 'Areal til Klima- eller Miljøprojekter',
    description: 'Ansøgt, planlagt eller udtaget til projekter',
  },
  unknown: {
    icon: WrenchScrewdriverIcon,
    color: 'text-muted-foreground',
    bgColor: 'bg-muted',
    borderColor: 'border-border',
    label: 'Ukendt Status',
    description: 'Status ikke fastlagt',
  },
} as const;

export function EnvironmentalStatusIndicator({
  status,
  label,
  hectares,
  percentage,
  className = '',
}: EnvironmentalStatusProps) {
  const config = STATUS_CONFIG[status];
  const Icon = config.icon;

  return (
    <div
      className={`flex items-center rounded-lg border p-3 ${config.bgColor} ${config.borderColor} ${className}`}
    >
      <Icon className={`h-5 w-5 ${config.color} mr-3`} />
      <div className="flex-1">
        <div className="flex items-center justify-between">
          <span className="text-foreground text-sm font-medium">{label}</span>
          {percentage !== undefined && (
            <span className={`text-sm font-semibold ${config.color}`}>
              {percentage.toFixed(1)}%
            </span>
          )}
        </div>
        <div className="mt-1 flex items-center justify-between">
          <span className="text-muted-foreground text-xs">
            {config.description}
          </span>
          {hectares !== undefined && (
            <span className="text-muted-foreground text-xs">
              {hectares.toLocaleString('da-DK', { maximumFractionDigits: 1 })}{' '}
              ha
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

interface EnvironmentalComplianceOverviewProps {
  data: {
    totalProblematicHa: number;
    totalDealtWithHa: number;
    compliancePercentage: number;
    waterCoveragePercentage: number;
  };
  className?: string;
}

export function EnvironmentalComplianceOverview({
  data,
  className = '',
}: EnvironmentalComplianceOverviewProps) {
  const totalHa = data.totalProblematicHa + data.totalDealtWithHa;

  // Determine overall compliance status
  const getOverallStatus = () => {
    if (data.compliancePercentage >= 95) return 'compliant';
    if (data.compliancePercentage >= 70) return 'water_covered';
    return 'action_required';
  };

  const overallStatus = getOverallStatus();

  return (
    <div className={`space-y-4 ${className}`}>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <EnvironmentalStatusIndicator
          status={overallStatus}
          label="Samlet Miljøoverholdelse"
          hectares={data.totalDealtWithHa}
          percentage={data.compliancePercentage}
        />

        <EnvironmentalStatusIndicator
          status={
            data.waterCoveragePercentage > 50
              ? 'water_covered'
              : 'action_required'
          }
          label="Areal til Klima- eller Miljøprojekter"
          hectares={
            data.totalProblematicHa * (data.waterCoveragePercentage / 100)
          }
          percentage={data.waterCoveragePercentage}
        />
      </div>

      <div className="border-border bg-background rounded-lg border p-4">
        <h4 className="text-foreground mb-3 text-sm font-medium">
          Miljøområde Fordeling
        </h4>

        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Problematiske områder</span>
            <span className="text-conventional font-medium">
              {data.totalProblematicHa.toLocaleString('da-DK', {
                maximumFractionDigits: 1,
              })}{' '}
              ha
            </span>
          </div>

          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Håndterede områder</span>
            <span className="font-medium text-green-600">
              {data.totalDealtWithHa.toLocaleString('da-DK', {
                maximumFractionDigits: 1,
              })}{' '}
              ha
            </span>
          </div>

          <div className="flex items-center justify-between border-t pt-2 text-sm">
            <span className="text-foreground font-medium">
              Total miljøområde
            </span>
            <span className="text-foreground font-semibold">
              {totalHa.toLocaleString('da-DK', { maximumFractionDigits: 1 })} ha
            </span>
          </div>
        </div>

        {/* Progress bar */}
        <div className="mt-4">
          <div className="text-muted-foreground mb-1 flex items-center justify-between text-xs">
            <span>Overholdelsesgrad</span>
            <span>{data.compliancePercentage.toFixed(1)}%</span>
          </div>
          <div className="bg-muted h-2 w-full rounded-full">
            <div
              className={`h-2 rounded-full ${
                data.compliancePercentage >= 95
                  ? 'bg-organic'
                  : data.compliancePercentage >= 70
                    ? 'bg-bnbo'
                    : 'bg-conventional'
              }`}
              style={
                {
                  width: `${Math.min(data.compliancePercentage, 100)}%`,
                } as React.CSSProperties
              }
            />
          </div>
        </div>
      </div>
    </div>
  );
}
