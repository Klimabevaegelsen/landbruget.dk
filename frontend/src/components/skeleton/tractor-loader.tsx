import Image from "next/image";
import { Container } from "../layout/container";

export function TractorLoader() {
  return (
    <Container className="bg-primary-foreground">
      <div className="flex flex-col items-center h-screen">
        <div className="text-4xl font-display font-bold my-12 relative">
          <div className="overflow-hidden whitespace-nowrap flex">
            Henter Data
            <div className="animate-typing overflow-hidden whitespace-nowrap border-r-4 border-r-primary">
              ...
            </div>
          </div>
        </div>
        <div className="relative bg-[#C1EAFE] p-5 md:p-20 overflow-hidden animate-morph">
          <Image
            src="/img/placeholder/tractor.gif"
            alt="Tractor"
            width={1400}
            height={1400}
            className="object-cover"
          />
        </div>
      </div>
    </Container>
  );
}
