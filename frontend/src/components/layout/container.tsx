import { cn } from "@/lib/utils";

export function Container({
  className,
  subclassName,
  children,
  section,
}: {
  className?: string;
  children: React.ReactNode;
  subclassName?: string;
  section?: boolean;
}) {
  return (
    <div className={cn(className, "px-6 lg:px-8")}>
      <div
        className={cn(
          "mx-auto max-w-4xl lg:max-w-7xl",
          section && "py-14",
          subclassName
        )}
      >
        {children}
      </div>
    </div>
  );
}
