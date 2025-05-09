import { PageBuilderItem } from "@/services/supabase/types";
import { BlockContainer } from "./block-container";
import { JsonRender } from "@/components/common/json-render";

export function PlaceholderBlock({ block }: { block: PageBuilderItem }) {
  return (
    <BlockContainer title={block.title}>
      <JsonRender
        json={JSON.parse(JSON.stringify(block))}
        title={`Component ${block._type} placeholder (data)`}
      />
    </BlockContainer>
  );
}
