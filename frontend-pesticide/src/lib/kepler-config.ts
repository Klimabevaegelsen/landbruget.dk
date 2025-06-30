import { generateColorScale, BNBO_COLORS, BBR_COLORS } from './color-schemes';

interface KeplerConfigOptions {
  selectedYear: number;
  showPesticides: boolean;
  showPFAS: boolean;
  showBNBO: boolean;
  showBBR: boolean;
  cumulativeMode: boolean;
}

export function generateKeplerConfig(options: KeplerConfigOptions) {
  const {
    selectedYear,
    showPesticides,
    showPFAS,
    showBNBO,
    showBBR,
    cumulativeMode
  } = options;

  return {
    version: 'v1',
    config: {
      visState: {
        filters: [
          {
            dataId: 'h3_data',
            id: 'year_filter',
            name: ['year'],
            type: 'range',
            value: cumulativeMode ? [2020, selectedYear] : [selectedYear, selectedYear],
            enlarged: false,
            plotType: 'histogram',
            yAxis: null
          }
        ],
        layers: [
          // H3 Heatmap Layer
          {
            id: 'h3_heatmap',
            type: 'hexagonId',
            config: {
              dataId: 'h3_data',
              label: showPFAS ? 'PFAS Exposure (grams)' : 'Pesticide Load (kg/ha)',
              color: showPFAS ? 
                generateColorScale('pfas_grams', 'Reds') : 
                generateColorScale('pesticide_load', 'Blues'),
              columns: {
                hex_id: 'h3_id'
              },
              isVisible: true,
              visConfig: {
                opacity: 0.7,
                coverage: 1,
                enable3d: true,
                elevationScale: showPFAS ? 100 : 50,
                elevationRange: [0, 500],
                colorRange: showPFAS ? {
                  name: 'PFAS Red Scale',
                  type: 'sequential',
                  category: 'Uber',
                  colors: ['#fff5f0', '#fee0d2', '#fcbba1', '#fc9272', '#fb6a4a', '#ef3b2c', '#cb181d', '#99000d']
                } : {
                  name: 'Pesticide Blue Scale',
                  type: 'sequential',
                  category: 'Uber',
                  colors: ['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6', '#4292c6', '#2171b5', '#084594']
                },
                sizeRange: [0, 10],
                radiusRange: [0, 50]
              }
            }
          },
          // BNBO Areas Layer
          {
            id: 'bnbo_areas',
            type: 'geojson',
            config: {
              dataId: 'bnbo_data',
              label: 'BNBO Protected Areas',
              color: [46, 134, 89], // Default green
              columns: {
                geojson: 'geometry'
              },
              isVisible: showBNBO,
              visConfig: {
                opacity: 0.4,
                stroked: true,
                strokeWidth: 1,
                strokeColor: [255, 255, 255],
                filled: true,
                enable3d: false,
                wireframe: false,
                colorRange: {
                  name: 'BNBO Status Colors',
                  type: 'qualitative',
                  category: 'Custom',
                  colors: Object.values(BNBO_COLORS)
                }
              }
            }
          },
          // BBR Buildings Layer
          {
            id: 'bbr_buildings',
            type: 'point',
            config: {
              dataId: 'bbr_data',
              label: 'Buildings',
              color: [74, 144, 226], // Default blue
              columns: {
                lat: 'lat',
                lng: 'lng',
                altitude: null
              },
              isVisible: showBBR,
              visConfig: {
                radius: 3,
                opacity: 0.8,
                outline: false,
                thickness: 2,
                strokeColor: null,
                colorRange: {
                  name: 'Building Type Colors',
                  type: 'qualitative',
                  category: 'Custom',
                  colors: Object.values(BBR_COLORS)
                },
                strokeColorRange: {
                  name: 'Global Warming',
                  type: 'sequential',
                  category: 'Uber',
                  colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']
                },
                radiusRange: [1, 10],
                filled: true
              }
            }
          }
        ],
        interactionConfig: {
          tooltip: {
            fieldsToShow: {
              h3_data: [
                { name: 'h3_id', format: null },
                { name: 'year', format: null },
                { name: 'total_pesticide_load', format: '.2f' },
                { name: 'total_pfas_grams', format: '.2f' },
                { name: 'field_count', format: null },
                { name: 'agricultural_area_ha', format: '.2f' }
              ],
              bnbo_data: [
                { name: 'bnbo_id', format: null },
                { name: 'status_description', format: null },
                { name: 'area_ha', format: '.2f' }
              ],
              bbr_data: [
                { name: 'bbr_id', format: null },
                { name: 'building_type', format: null },
                { name: 'construction_year', format: null },
                { name: 'floor_area', format: '.1f' }
              ]
            },
            compareMode: false,
            compareType: 'absolute',
            enabled: true
          },
          brush: {
            size: 0.5,
            enabled: false
          },
          geocoder: {
            enabled: false
          },
          coordinate: {
            enabled: false
          }
        },
        layerBlending: 'normal',
        splitMaps: [],
        animationConfig: {
          currentTime: null,
          speed: 1
        }
      },
      mapState: {
        bearing: 0,
        dragRotate: false,
        latitude: 56.26392,  // Denmark center
        longitude: 9.501785,
        pitch: 0,
        zoom: 7,
        isSplit: false
      },
      mapStyle: {
        styleType: 'dark',
        topLayerGroups: {},
        visibleLayerGroups: {
          label: true,
          road: true,
          border: false,
          building: true,
          water: true,
          land: true,
          '3d building': false
        },
        threeDBuildingColor: [9.665468314072013, 17.18305478057247, 31.1442867897876],
        mapStyles: {}
      }
    }
  };
}

export const DEFAULT_KEPLER_CONFIG = {
  version: 'v1',
  config: {
    visState: {
      filters: [],
      layers: [],
      interactionConfig: {
        tooltip: {
          fieldsToShow: {},
          enabled: true
        },
        brush: {
          size: 0.5,
          enabled: false
        }
      },
      layerBlending: 'normal',
      splitMaps: [],
      animationConfig: {
        currentTime: null,
        speed: 1
      }
    },
    mapState: {
      bearing: 0,
      dragRotate: false,
      latitude: 56.26392,
      longitude: 9.501785,
      pitch: 0,
      zoom: 7,
      isSplit: false
    },
    mapStyle: {
      styleType: 'dark',
      topLayerGroups: {},
      visibleLayerGroups: {
        label: true,
        road: true,
        border: false,
        building: true,
        water: true,
        land: true,
        '3d building': false
      },
      threeDBuildingColor: [9.665468314072013, 17.18305478057247, 31.1442867897876],
      mapStyles: {}
    }
  }
};

// Layer configuration presets
export const LAYER_PRESETS = {
  H3_PESTICIDE: {
    id: 'h3_pesticide',
    type: 'hexagonId',
    colorField: 'total_pesticide_load',
    colorScale: 'Blues',
    elevationScale: 50
  },
  H3_PFAS: {
    id: 'h3_pfas',
    type: 'hexagonId',
    colorField: 'total_pfas_grams',
    colorScale: 'Reds',
    elevationScale: 100
  },
  BNBO_PROTECTED: {
    id: 'bnbo_protected',
    type: 'geojson',
    colorField: 'status_code',
    opacity: 0.4
  },
  BBR_BUILDINGS: {
    id: 'bbr_buildings',
    type: 'point',
    colorField: 'building_type',
    radius: 3
  }
}; 