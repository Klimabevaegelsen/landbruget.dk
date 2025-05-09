export function BlockContainer({
  children,
  title,
}: {
  children: React.ReactNode;
  title: string;
}) {
  return (
    <div className="flex flex-col gap-3">
      <h2 className="text-2xl font-bold">{title}</h2>
      {children}
    </div>
  );
}
