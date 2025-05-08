import { Container } from "@/components/layout/container";

export default function CompanyPage({ params }: { params: { id: string } }) {
  const { id } = params;
  return (
    <div>
      <Container className="min-h-[85vh] flex flex-col justify-center items-center">
        <p className="text-4xl text-center">🧑‍🌾</p>
        <h1>CompanyPage {id}</h1>
      </Container>
    </div>
  );
}
