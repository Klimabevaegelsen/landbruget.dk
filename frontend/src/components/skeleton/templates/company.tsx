import { Container } from "@/components/layout/container";

export function CompanySkeleton() {
  return (
    <Container
      className=" flex flex-col w-full justify-center items-center py-12"
      subclassName="mx-0 w-full"
    >
      <div className="flex w-full flex-col gap-8 animate-pulse">
        <div className="flex flex-col md:flex-row gap-8">
          <div className="w-full h-80 skeleton-item flex items-center justify-center">
            <p className="text-gray-500 text-xl font-bold">Henter data</p>
          </div>
          <div className="w-full h-80 skeleton-item"></div>
        </div>
        <div className="flex gap-8">
          <div className="w-1/4">
            <div className="w-full h-full skeleton-item"></div>
          </div>
          <div className="w-3/4 flex flex-col gap-8">
            <div className="w-full h-80 skeleton-item"></div>
            <div className="w-full h-80 skeleton-item"></div>
            <div className="w-full h-80 skeleton-item"></div>
            <div className="w-full h-80 skeleton-item"></div>
          </div>
        </div>
      </div>
    </Container>
  );
}
