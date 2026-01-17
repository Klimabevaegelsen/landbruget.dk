import { Skeleton } from '@/components/ui/skeleton';

export default function MarkanalyseLoading() {
  return (
    <div className="flex h-screen">
      {/* Desktop Sidebar Skeleton */}
      <div className="bg-card hidden w-80 border-r lg:block">
        <div className="border-b p-4">
          <Skeleton className="mb-2 h-6 w-32" />
          <Skeleton className="h-4 w-48" />
        </div>
        <div className="space-y-4 p-4">
          <div className="space-y-2">
            <Skeleton className="h-4 w-20" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
          <div className="space-y-2">
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        </div>
      </div>

      {/* Main Map Area Skeleton */}
      <div className="bg-background relative flex-1">
        {/* Mobile Menu Button */}
        <div className="absolute top-4 left-4 z-10 lg:hidden">
          <Skeleton className="h-12 w-12 rounded-full" />
        </div>

        {/* Year Slider */}
        <div className="absolute top-4 right-4 left-4 z-10 lg:right-4 lg:left-auto">
          <Skeleton className="h-12 w-full lg:w-64" />
        </div>

        {/* Map Loading */}
        <div className="flex h-full items-center justify-center">
          <div className="space-y-4 text-center">
            <div className="border-primary mx-auto h-12 w-12 animate-spin rounded-full border-b-2 border-t-transparent"></div>
            <p className="text-muted-foreground">Indlæser kort...</p>
          </div>
        </div>
      </div>
    </div>
  );
}
