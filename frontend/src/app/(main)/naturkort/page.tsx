import { NaturKortPage } from "@/components/naturkort/naturkort";
import { Container } from "@/components/layout/container";

export default function UdtagningsKortPage() {
    return (
        <Container>
            <h1 className="text-2xl font bold mb-4">Effektiv udtagning af landbrugsjord</h1>
            <NaturKortPage />
        </Container>       
    );
}