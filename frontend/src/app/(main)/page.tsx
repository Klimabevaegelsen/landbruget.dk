import { Container } from "@/components/layout/container";
import Hero from "@/components/page-sections/hero";
import { Metadata } from "next";

export const metadata: Metadata = {
  title: "Landbruget.dk",
  description: "Dansk landbrugsdata - samlet ét sted",
};

export default function Home() {
  return (
    <div>
      <Container className="bg-primary-darker">
        <Hero />
      </Container>
      <Container className="">
        <div className="flex flex-col  min-h-[500px] items-center justify-center ">
          <p className="text-4xl">🚜</p>
          <h1 className="italic">Cool section to come </h1>
        </div>
      </Container>
    </div>
  );
}
