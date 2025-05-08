import { Container } from "../container";

export function NavBanner() {
  return (
    <div className="w-full h-10 bg-primary-foreground">
      <Container className="flex h-full place-content-between">
        <div className="flex h-full items-center">
          <p>Banner</p>
        </div>
      </Container>
    </div>
  );
}
