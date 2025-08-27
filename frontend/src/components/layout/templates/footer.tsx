import Link from "next/link";
import { Container } from "../container";
import { Logo } from "./logo";

export function Footer() {
  return (
    <div className="bg-primary-foreground">
      <Container>
        <div className="flex flex-col gap-4 lg:flex-row py-6 justify-between items-center">
          <Logo className="h-[26px]" />

          {/* Navigation Links */}
          <div className="flex flex-col gap-2 lg:flex-row lg:gap-6 items-center">
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-2 text-center">
              <Link className="text-sm font-medium hover:underline" href="/om-os">
                Om Landbruget.dk
              </Link>
              <Link className="text-sm font-medium hover:underline" href="/brugsvilkaar">
                Brugsvilkår
              </Link>
              <Link className="text-sm font-medium hover:underline" href="/privatlivspolitik">
                Privatlivspolitik
              </Link>
            </div>
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-2 text-center">
              <Link className="text-sm font-medium hover:underline" href="/">
                Kilder
              </Link>
              <Link className="text-sm font-medium hover:underline" href="/">
                Download
              </Link>
            </div>
          </div>

          <p className="text-sm">© Copyright 2025 Landbruget.dk</p>
        </div>
      </Container>
    </div>
  );
}
