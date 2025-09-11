'use client';

import {
  Breadcrumb,
  BreadcrumbList,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb';
import {
  Home,
  MapPin,
  Building2,
  Wheat,
  Factory,
  TrendingUp,
} from 'lucide-react';

export interface NavigationPath {
  country?: {
    name: string;
    code: string;
  };
  region?: {
    id: string;
    name: string;
  };
  municipality?: {
    id: string;
    name: string;
    code?: string;
  };
  company?: {
    id: string;
    name: string;
    cvr?: string;
  };
  field?: {
    id: string;
    name: string;
    blockId?: string;
    fieldId?: string;
  };
  analysis?: {
    id: string;
    name: string;
    type: string;
  };
  currentPage?: {
    name: string;
    type: 'dashboard' | 'analysis' | 'settings' | 'documentation';
  };
}

interface AgriculturalBreadcrumbProps {
  path: NavigationPath;
  className?: string;
}

export function AgriculturalBreadcrumb({
  path,
  className,
}: AgriculturalBreadcrumbProps) {
  const getIcon = (type: string) => {
    const icons = {
      country: <Home className="h-4 w-4" />,
      region: <MapPin className="h-4 w-4" />,
      municipality: <MapPin className="h-4 w-4" />,
      company: <Building2 className="h-4 w-4" />,
      field: <Wheat className="h-4 w-4" />,
      analysis: <TrendingUp className="h-4 w-4" />,
      factory: <Factory className="h-4 w-4" />,
    };
    return icons[type as keyof typeof icons] || <Home className="h-4 w-4" />;
  };

  return (
    <Breadcrumb className={className}>
      <BreadcrumbList>
        {/* Always start with Denmark */}
        <BreadcrumbItem>
          <BreadcrumbLink
            href="/"
            className="hover:text-primary flex items-center gap-2 transition-colors"
          >
            {getIcon('country')}
            {path.country?.name || '🇩🇰 Danmark'}
          </BreadcrumbLink>
        </BreadcrumbItem>

        {/* Region level (if present) */}
        {path.region && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink
                href={`/regioner/${path.region.id}`}
                className="hover:text-primary flex items-center gap-2 transition-colors"
              >
                {getIcon('region')}
                {path.region.name}
              </BreadcrumbLink>
            </BreadcrumbItem>
          </>
        )}

        {/* Municipality level */}
        {path.municipality && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink
                href={`/kommuner/${path.municipality.id}`}
                className="hover:text-primary flex items-center gap-2 transition-colors"
              >
                {getIcon('municipality')}
                {path.municipality.name}
                {path.municipality.code && (
                  <span className="text-muted-foreground text-xs">
                    ({path.municipality.code})
                  </span>
                )}
              </BreadcrumbLink>
            </BreadcrumbItem>
          </>
        )}

        {/* Company level */}
        {path.company && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink
                href={`/virksomheder/${path.company.id}`}
                className="hover:text-primary flex items-center gap-2 transition-colors"
              >
                {getIcon('company')}
                <span className="max-w-[200px] truncate">
                  {path.company.name}
                </span>
                {path.company.cvr && (
                  <span className="text-muted-foreground text-xs">
                    CVR: {path.company.cvr}
                  </span>
                )}
              </BreadcrumbLink>
            </BreadcrumbItem>
          </>
        )}

        {/* Field level */}
        {path.field && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink
                href={`/marker/${path.field.id}`}
                className="hover:text-primary flex items-center gap-2 transition-colors"
              >
                {getIcon('field')}
                <span className="max-w-[150px] truncate">
                  {path.field.name}
                </span>
                {(path.field.blockId || path.field.fieldId) && (
                  <span className="text-muted-foreground text-xs">
                    {path.field.blockId}-{path.field.fieldId}
                  </span>
                )}
              </BreadcrumbLink>
            </BreadcrumbItem>
          </>
        )}

        {/* Analysis level */}
        {path.analysis && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink
                href={`/analyser/${path.analysis.id}`}
                className="hover:text-primary flex items-center gap-2 transition-colors"
              >
                {getIcon('analysis')}
                <span className="max-w-[200px] truncate">
                  {path.analysis.name}
                </span>
                <span className="text-muted-foreground text-xs capitalize">
                  {path.analysis.type}
                </span>
              </BreadcrumbLink>
            </BreadcrumbItem>
          </>
        )}

        {/* Current page (final breadcrumb) */}
        {path.currentPage && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage className="flex items-center gap-2 font-medium">
                {getIcon(path.currentPage.type)}
                {path.currentPage.name}
              </BreadcrumbPage>
            </BreadcrumbItem>
          </>
        )}
      </BreadcrumbList>
    </Breadcrumb>
  );
}

// Example usage components for different contexts
export function CompanyBreadcrumb({
  municipalityId,
  municipalityName,
  companyId,
  companyName,
  companyCVR,
  currentPage,
}: {
  municipalityId: string;
  municipalityName: string;
  companyId: string;
  companyName: string;
  companyCVR?: string;
  currentPage?: string;
}) {
  const path: NavigationPath = {
    municipality: {
      id: municipalityId,
      name: municipalityName,
    },
    company: {
      id: companyId,
      name: companyName,
      cvr: companyCVR,
    },
    ...(currentPage && {
      currentPage: {
        name: currentPage,
        type: 'dashboard' as const,
      },
    }),
  };

  return <AgriculturalBreadcrumb path={path} />;
}

export function FieldBreadcrumb({
  municipalityId,
  municipalityName,
  companyId,
  companyName,
  fieldId,
  fieldName,
  blockId,
  fieldNumber,
  currentPage,
}: {
  municipalityId: string;
  municipalityName: string;
  companyId: string;
  companyName: string;
  fieldId: string;
  fieldName: string;
  blockId?: string;
  fieldNumber?: string;
  currentPage?: string;
}) {
  const path: NavigationPath = {
    municipality: {
      id: municipalityId,
      name: municipalityName,
    },
    company: {
      id: companyId,
      name: companyName,
    },
    field: {
      id: fieldId,
      name: fieldName,
      blockId: blockId,
      fieldId: fieldNumber,
    },
    ...(currentPage && {
      currentPage: {
        name: currentPage,
        type: 'analysis' as const,
      },
    }),
  };

  return <AgriculturalBreadcrumb path={path} />;
}
