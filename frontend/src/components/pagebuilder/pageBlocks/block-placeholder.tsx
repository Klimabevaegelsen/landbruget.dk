import { PageBuilderItem } from "@/services/supabase/types";
import { JsonRender } from "@/components/common/json-render";

export function BlockPlaceholder({ block }: { block: PageBuilderItem }) {
  return (
    <JsonRender
      json={JSON.parse(JSON.stringify(block))}
      title={`Component ${block._type} placeholder (data)`}
    />
  );
}
