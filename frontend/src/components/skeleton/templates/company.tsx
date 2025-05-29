import { Container } from "@/components/layout/container";

export function CompanySkeleton() {
  return (
    <Container
      className="flex w-full flex-col items-center justify-center py-12"
      subclassName="mx-0 w-full"
    >
      <div className="flex w-full animate-pulse flex-col gap-8">
        <div className="flex flex-col gap-8 md:flex-row">
          <div className="skeleton-item flex h-80 w-full items-center justify-center">
            <p className="text-xl font-bold text-gray-500">Henter data</p>
          </div>
          <div className="skeleton-item h-80 w-full"></div>
        </div>
        <div className="flex gap-8">
          <div className="w-1/4">
            <div className="skeleton-item h-full w-full"></div>
          </div>
          <div className="flex w-3/4 flex-col gap-8">
            <div className="skeleton-item h-80 w-full"></div>
            <div className="skeleton-item h-80 w-full"></div>
            <div className="skeleton-item h-80 w-full"></div>
            <div className="skeleton-item h-80 w-full"></div>
          </div>
        </div>
      </div>
    </Container>
  );
}
