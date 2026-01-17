export function LoadingSpinner() {
  return (
    <div className="flex h-full w-full items-center justify-center">
      <div className="border-primary h-12 w-12 animate-spin rounded-full border-b-2"></div>
    </div>
  );
}
