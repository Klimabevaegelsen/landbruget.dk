import { UdtagningsKort, CriteriaSelector } from "@/components/udtagningskort/udtagningskort";
import { Container } from "@/components/layout/container";

export default function UdtagningsKortPage() {
    return (
        <Container>
            <h1 className="text-2xl font bold mb-4">Effektiv udtagning af landbrugsjord</h1>
            <div className="grid grid-cols-5 gap-2">
                <div className="col-span-4">
                    <UdtagningsKort />
                </div>
                <div className="col-span-1">
                    <CriteriaSelector />
                </div>
            </div>
        </Container>       
    );
}