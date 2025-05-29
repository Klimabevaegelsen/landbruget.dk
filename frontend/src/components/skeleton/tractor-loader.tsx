import Image from "next/image";
import { Container } from "../layout/container";

export function TractorLoader() {
  return (
    <Container className="bg-primary-foreground">
      <div className="flex h-screen flex-col items-center">
        <div className="font-display relative my-12 text-4xl font-bold">
          <div className="flex overflow-hidden whitespace-nowrap">
            Henter Data
            <div className="animate-typing border-r-primary overflow-hidden border-r-4 whitespace-nowrap">
              ...
            </div>
          </div>
        </div>
        <div className="animate-morph relative overflow-hidden bg-[#C1EAFE] p-5 md:p-20">
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
