import type { MapRef } from "@vis.gl/react-maplibre";
import type { MapLib } from "@vis.gl/react-maplibre/types/lib";

declare module "@vis.gl/react-maplibre" {
  export interface MapContextValue {
    mapLib: MapLib | null;
    map: MapRef | null;
  }

  export const MapContext: React.Context<MapContextValue>;

  // Override the Map component to use the correct context type
  export const Map: React.ForwardRefExoticComponent<
    MapProps & React.RefAttributes<MapRef>
  >;
}
