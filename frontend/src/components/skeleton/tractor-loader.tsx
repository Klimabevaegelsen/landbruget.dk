import Image from "next/image";
import { Container } from "../layout/container";

export function TractorLoader() {
  return (
    <Container className="bg-primary-foreground">
      <div className="flex flex-col items-center justify-center h-screen">
        <div className="text-4xl font-display font-bold my-12 relative">
          <span className="animate-typing overflow-hidden whitespace-nowrap border-r-4 border-r-primary inline-block">
            Henter Data
          </span>
        </div>
        <div className="relative bg-[#C1EAFE] rounded-[30%_70%_70%_30%_/_30%_30%_70%_70%] p-5 md:p-20 overflow-hidden transition-all duration-500 hover:rounded-[60%_40%_30%_70%_/_60%_30%_70%_40%]">
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
