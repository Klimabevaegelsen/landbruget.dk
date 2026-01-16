'use client';

import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from '@/components/ui/hover-card';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import {
  Building2,
  MapPin,
  Wheat,
  Activity,
  Sprout,
  TrendingUp,
  Calendar,
  Shield,
} from 'lucide-react';

interface CompanyData {
  id: string;
  name: string;
  cvr: string;
  municipality: string;
  municipalityCode?: string;
  fieldCount: number;
  totalArea: number;
  organicFields: number;
  riskLevel: 'low' | 'medium' | 'high';
  lastActivity?: string;
  complianceScore?: number;
  primaryCrop?: string;
  establishedYear?: number;
  avatar?: string;
}

interface CompanyHoverCardProps {
  company: CompanyData;
  children: React.ReactNode;
}

export function CompanyHoverCard({ company, children }: CompanyHoverCardProps) {
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

  const getComplianceColor = (score: number) => {
    if (score >= 90) return 'text-green-600';
    if (score >= 70) return 'text-yellow-600';
    return 'text-red-600';
  };

  return (
    <HoverCard>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent className="w-96" side="right">
        <div className="space-y-4">
          {/* Header with Avatar and Basic Info */}
          <div className="flex items-start gap-4">
            <Avatar className="h-12 w-12">
              <AvatarImage src={company.avatar} alt={company.name} />
              <AvatarFallback className="bg-primary/10 text-primary font-semibold">
                {company.name
                  .split(' ')
                  .map((n) => n[0])
                  .join('')
                  .slice(0, 2)}
              </AvatarFallback>
            </Avatar>

            <div className="flex-1 space-y-1">
              <h3 className="text-base leading-tight font-semibold">
                {company.name}
              </h3>
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <Building2 className="h-3 w-3" />
                <span>CVR: {company.cvr}</span>
                {company.establishedYear && (
                  <>
                    <span>•</span>
                    <span>Est. {company.establishedYear}</span>
                  </>
                )}
              </div>
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <MapPin className="h-3 w-3" />
                <span>{company.municipality}</span>
                {company.municipalityCode && (
                  <span className="text-xs">({company.municipalityCode})</span>
                )}
              </div>
            </div>

            <Badge
              variant={getRiskBadgeVariant(company.riskLevel)}
              className="text-xs"
            >
              {company.riskLevel.toUpperCase()}
            </Badge>
          </div>

          {/* Key Metrics */}
          <div className="grid grid-cols-2 gap-4 border-y py-3">
            <div className="space-y-1">
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <Wheat className="h-4 w-4" />
                <span>Marker</span>
              </div>
              <div className="text-lg font-semibold">{company.fieldCount}</div>
            </div>

            <div className="space-y-1">
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <Activity className="h-4 w-4" />
                <span>Total Areal</span>
              </div>
              <div className="text-lg font-semibold">
                {company.totalArea} ha
              </div>
            </div>

            <div className="space-y-1">
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <Sprout className="h-4 w-4 text-green-600" />
                <span>Økologisk</span>
              </div>
              <div className="text-lg font-semibold text-green-600">
                {company.organicFields}
              </div>
            </div>

            <div className="space-y-1">
              <div className="text-muted-foreground flex items-center gap-2 text-sm">
                <TrendingUp className="h-4 w-4" />
                <span>Primær Afgrøde</span>
              </div>
              <div className="text-sm font-medium">
                {company.primaryCrop || 'Vinterhvede'}
              </div>
            </div>
          </div>

          {/* Compliance Score */}
          {company.complianceScore !== undefined && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="text-muted-foreground flex items-center gap-2 text-sm">
                  <Shield className="h-4 w-4" />
                  <span>Compliance Score</span>
                </div>
                <span
                  className={`text-sm font-semibold ${getComplianceColor(company.complianceScore)}`}
                >
                  {company.complianceScore}%
                </span>
              </div>
              <Progress value={company.complianceScore} className="h-2" />
            </div>
          )}

          {/* Last Activity */}
          {company.lastActivity && (
            <div className="text-muted-foreground flex items-center gap-2 border-t pt-2 text-sm">
              <Calendar className="h-4 w-4" />
              <span>Sidst aktiv: {company.lastActivity}</span>
            </div>
          )}

          {/* Organic Percentage */}
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Økologisk andel:</span>
            <div className="flex items-center gap-2">
              <Progress
                value={(company.organicFields / company.fieldCount) * 100}
                className="h-2 w-16"
              />
              <span className="font-medium text-green-600">
                {Math.round((company.organicFields / company.fieldCount) * 100)}
                %
              </span>
            </div>
          </div>
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}

// Field hover card component
interface FieldData {
  id: string;
  name: string;
  area: number;
  cropType: string;
  organic: boolean;
  lastTreatment?: string;
  riskLevel: 'low' | 'medium' | 'high';
  coordinates: [number, number];
  blockId?: string;
  fieldId?: string;
  soilType?: string;
  slope?: number;
  companyName?: string;
}

interface FieldHoverCardProps {
  field: FieldData;
  children: React.ReactNode;
}

export function FieldHoverCard({ field, children }: FieldHoverCardProps) {
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

  return (
    <HoverCard>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent className="w-80" side="top">
        <div className="space-y-3">
          {/* Header */}
          <div className="flex items-start justify-between">
            <div className="space-y-1">
              <h3 className="flex items-center gap-2 text-base font-semibold">
                <Wheat className="h-4 w-4" />
                {field.name}
              </h3>
              {field.companyName && (
                <p className="text-muted-foreground text-sm">
                  {field.companyName}
                </p>
              )}
            </div>
            <Badge
              variant={getRiskBadgeVariant(field.riskLevel)}
              className="text-xs"
            >
              {field.riskLevel.toUpperCase()}
            </Badge>
          </div>

          {/* Key Information */}
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="space-y-1">
              <span className="text-muted-foreground">Areal:</span>
              <span className="font-medium">{field.area} ha</span>
            </div>

            <div className="space-y-1">
              <span className="text-muted-foreground">Afgrøde:</span>
              <span className="font-medium">{field.cropType}</span>
            </div>

            <div className="space-y-1">
              <span className="text-muted-foreground">Økologisk:</span>
              <Badge
                variant={field.organic ? 'default' : 'secondary'}
                className="text-xs"
              >
                {field.organic ? 'Ja' : 'Nej'}
              </Badge>
            </div>

            {field.soilType && (
              <div className="space-y-1">
                <span className="text-muted-foreground">Jordtype:</span>
                <span className="font-medium">{field.soilType}</span>
              </div>
            )}
          </div>

          {/* Block/Field ID */}
          {(field.blockId || field.fieldId) && (
            <div className="text-muted-foreground flex items-center gap-2 text-sm">
              <span>ID:</span>
              <code className="bg-muted rounded px-1.5 py-0.5 font-mono text-xs">
                {field.blockId}-{field.fieldId}
              </code>
            </div>
          )}

          {/* Coordinates */}
          <div className="text-muted-foreground flex items-center gap-2 text-sm">
            <MapPin className="h-3 w-3" />
            <code className="font-mono text-xs">
              {field.coordinates[0].toFixed(4)},{' '}
              {field.coordinates[1].toFixed(4)}
            </code>
          </div>

          {/* Last Treatment */}
          {field.lastTreatment && (
            <div className="text-muted-foreground flex items-center gap-2 border-t pt-2 text-sm">
              <Calendar className="h-4 w-4" />
              <span>
                Sidst behandlet:{' '}
                {new Date(field.lastTreatment).toLocaleDateString('da-DK')}
              </span>
            </div>
          )}
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}
