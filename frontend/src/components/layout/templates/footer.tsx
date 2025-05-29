import Link from "next/link";
import { Container } from "../container";
import { Logo } from "./logo";

export function Footer() {
  return (
    <div className="bg-primary-foreground">
      <Container>
        <div className="flex flex-col items-center justify-between gap-4 py-6 lg:flex-row">
          <Logo className="h-[26px]" />
          <div className="flex gap-6">
            <Link className="text-sm font-medium hover:underline" href="/">
              Om Landbruget.dk
            </Link>
            <Link className="text-sm font-medium hover:underline" href="/">
              Kilder
            </Link>
            <Link className="text-sm font-medium hover:underline" href="/">
              Download
            </Link>
          </div>
          <p className="text-sm">© Copyright 2025 Landbruget.dk</p>
        </div>
      </Container>
    </div>
  );
}
