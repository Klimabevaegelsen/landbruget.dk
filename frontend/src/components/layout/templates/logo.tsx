import Image from "next/image";
import { clsx } from "clsx";

export function Logo({ className }: { className?: string }) {
  return (
    <Image
      src="/img/logo.png"
      alt="Landbruget.dk logo"
      width={361 / 2}
      height={54 / 2}
      className={clsx(className, "overflow-visible")}
      priority
    />
  );
}

export function Mark({ className }: { className?: string }) {
  return (
    <Image
      src="/img/logo.png"
      alt="Landbruget.dk mark"
      width={34}
      height={34}
      className={className}
      priority
    />
  );
}
