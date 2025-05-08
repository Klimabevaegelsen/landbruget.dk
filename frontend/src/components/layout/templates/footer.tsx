import Link from "next/link";
import { Container } from "../container";
import { Logo } from "./logo";

export function Footer() {
  return (
    <div className="bg-primary-foreground">
      <Container>
        <div className="flex flex-col gap-4 lg:flex-row py-6 justify-between items-center">
          <Logo className="h-[26px]" />
          <div className="flex  gap-4">
            <Link className="text-sm font-medium" href="/">
              Om Landbruget.dk
            </Link>
            <Link className="text-sm font-medium" href="/">
              Kilder
            </Link>
            <Link className="text-sm font-medium" href="/">
              Download
            </Link>
          </div>
          <p className="text-sm">© Copyright 2025 Landbruget.dk</p>
        </div>
      </Container>
    </div>
  );
}
