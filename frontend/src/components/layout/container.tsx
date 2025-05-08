import { cn } from "@/lib/utils";

export function Container({
  className,
  subclassName,
  children,
}: {
  className?: string;
  children: React.ReactNode;
  subclassName?: string;
}) {
  return (
    <div className={cn(className, "px-6 lg:px-8")}>
      <div className={cn(subclassName, "mx-auto max-w-4xl lg:max-w-7xl")}>
        {children}
      </div>
    </div>
  );
}
