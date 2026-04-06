import { PageBuilderItem } from '@/services/data/types';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function BlockPlaceholder({ block }: { block: PageBuilderItem }) {
  // Don't render anything for unknown block types to avoid showing debug labels
  return null;
}
