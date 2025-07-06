(globalThis.TURBOPACK = globalThis.TURBOPACK || []).push([typeof document === "object" ? document.currentScript : undefined, {

"[project]/src/stores/map-store.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "BNBO_STATUS_CONFIG": (()=>BNBO_STATUS_CONFIG),
    "DATA_MODE_CONFIG": (()=>DATA_MODE_CONFIG),
    "getComputedLayerVisibility": (()=>getComputedLayerVisibility),
    "useAvailableResolutions": (()=>useAvailableResolutions),
    "useAvailableYearOptions": (()=>useAvailableYearOptions),
    "useAvailableYears": (()=>useAvailableYears),
    "useBearing": (()=>useBearing),
    "useCenter": (()=>useCenter),
    "useDataState": (()=>useDataState),
    "useError": (()=>useError),
    "useIsLoading": (()=>useIsLoading),
    "useIsLoadingTiles": (()=>useIsLoadingTiles),
    "useIsLoadingYear": (()=>useIsLoadingYear),
    "useLayerVisibility": (()=>useLayerVisibility),
    "useLoadingMessage": (()=>useLoadingMessage),
    "useLoadingState": (()=>useLoadingState),
    "useMapStore": (()=>useMapStore),
    "useMapViewState": (()=>useMapViewState),
    "usePitch": (()=>usePitch),
    "useSelectedDataMode": (()=>useSelectedDataMode),
    "useSelectedYear": (()=>useSelectedYear),
    "useShowBNBOLayer": (()=>useShowBNBOLayer),
    "useShowBasemap": (()=>useShowBasemap),
    "useShowControls": (()=>useShowControls),
    "useShowTooltip": (()=>useShowTooltip),
    "useTooltipData": (()=>useTooltipData),
    "useTooltipPosition": (()=>useTooltipPosition),
    "useTooltipState": (()=>useTooltipState),
    "useZoom": (()=>useZoom)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-client] (ecmascript)");
var _s = __turbopack_context__.k.signature(), _s1 = __turbopack_context__.k.signature(), _s2 = __turbopack_context__.k.signature(), _s3 = __turbopack_context__.k.signature(), _s4 = __turbopack_context__.k.signature(), _s5 = __turbopack_context__.k.signature(), _s6 = __turbopack_context__.k.signature(), _s7 = __turbopack_context__.k.signature(), _s8 = __turbopack_context__.k.signature(), _s9 = __turbopack_context__.k.signature(), _s10 = __turbopack_context__.k.signature(), _s11 = __turbopack_context__.k.signature(), _s12 = __turbopack_context__.k.signature(), _s13 = __turbopack_context__.k.signature(), _s14 = __turbopack_context__.k.signature(), _s15 = __turbopack_context__.k.signature(), _s16 = __turbopack_context__.k.signature(), _s17 = __turbopack_context__.k.signature(), _s18 = __turbopack_context__.k.signature(), _s19 = __turbopack_context__.k.signature(), _s20 = __turbopack_context__.k.signature(), _s21 = __turbopack_context__.k.signature(), _s22 = __turbopack_context__.k.signature(), _s23 = __turbopack_context__.k.signature(), _s24 = __turbopack_context__.k.signature();
;
;
const DEFAULT_CENTER = [
    10.0,
    56.0
]; // Center of Denmark
const DEFAULT_ZOOM = 7;
const DEFAULT_YEAR = 2023;
const DEFAULT_DATA_MODE = 'pesticide_total';
// Zoom thresholds for layer switching - minimal overlap to prevent gaps
const KOMMUNE_MAX_ZOOM = 8.2;
const H3_MIN_ZOOM = 8.1;
// H3 resolution based on zoom level
const getH3ResolutionForZoom = (zoom)=>{
    if (zoom >= 14) return 10;
    if (zoom >= 12) return 9;
    if (zoom >= 10) return 8;
    return 7;
};
const useMapStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["devtools"])((set, get)=>({
        // Initial state
        zoom: DEFAULT_ZOOM,
        center: DEFAULT_CENTER,
        bearing: 0,
        pitch: 0,
        selectedYear: DEFAULT_YEAR,
        selectedDataMode: DEFAULT_DATA_MODE,
        showBNBOLayer: true,
        showBasemap: true,
        isLoading: false,
        isLoadingYear: false,
        isLoadingTiles: false,
        loadingMessage: '',
        error: null,
        availableYears: [],
        availableYearOptions: [],
        availableResolutions: [
            7,
            8,
            9,
            10
        ],
        showControls: true,
        showTooltip: false,
        tooltipData: null,
        tooltipPosition: {
            x: 0,
            y: 0
        },
        // Map view actions
        setZoom: (zoom)=>set({
                zoom
            }),
        setCenter: (center)=>set({
                center
            }),
        setBearing: (bearing)=>set({
                bearing
            }),
        setPitch: (pitch)=>set({
                pitch
            }),
        setViewState: (viewState)=>set(viewState),
        // Data selection actions
        setSelectedYear: (selectedYear)=>set({
                selectedYear,
                isLoadingYear: true
            }),
        setSelectedDataMode: (selectedDataMode)=>set({
                selectedDataMode
            }),
        // Layer visibility actions
        setShowBNBOLayer: (showBNBOLayer)=>set({
                showBNBOLayer
            }),
        setShowBasemap: (showBasemap)=>set({
                showBasemap
            }),
        toggleBNBOLayer: ()=>set((state)=>({
                    showBNBOLayer: !state.showBNBOLayer
                })),
        // Loading state actions
        setIsLoading: (isLoading)=>set({
                isLoading
            }),
        setIsLoadingYear: (isLoadingYear)=>set({
                isLoadingYear
            }),
        setIsLoadingTiles: (isLoadingTiles)=>set({
                isLoadingTiles
            }),
        setLoadingMessage: (loadingMessage)=>set({
                loadingMessage
            }),
        // Error state actions
        setError: (error)=>set({
                error
            }),
        clearError: ()=>set({
                error: null
            }),
        // Data availability actions
        setAvailableYears: (availableYears)=>{
            // When years are set, also update year options to include 'total'
            const availableYearOptions = [
                ...availableYears,
                'total'
            ];
            set({
                availableYears,
                availableYearOptions
            });
        },
        setAvailableYearOptions: (availableYearOptions)=>set({
                availableYearOptions
            }),
        setAvailableResolutions: (availableResolutions)=>set({
                availableResolutions
            }),
        // UI actions
        setShowControls: (showControls)=>set({
                showControls
            }),
        setShowTooltip: (showTooltip)=>set({
                showTooltip
            }),
        setTooltipData: (tooltipData)=>set({
                tooltipData
            }),
        setTooltipPosition: (tooltipPosition)=>set({
                tooltipPosition
            }),
        showTooltipWithData: (data, position)=>{
            set({
                showTooltip: true,
                tooltipData: data,
                tooltipPosition: position
            });
        },
        hideTooltip: ()=>{
            set({
                showTooltip: false,
                tooltipData: null
            });
        },
        // Utility actions
        resetToDefaults: ()=>{
            set({
                zoom: DEFAULT_ZOOM,
                center: DEFAULT_CENTER,
                bearing: 0,
                pitch: 0,
                selectedYear: DEFAULT_YEAR,
                selectedDataMode: DEFAULT_DATA_MODE,
                showBNBOLayer: true,
                showBasemap: true,
                isLoading: false,
                isLoadingYear: false,
                isLoadingTiles: false,
                loadingMessage: '',
                error: null,
                showControls: true,
                showTooltip: false,
                tooltipData: null,
                tooltipPosition: {
                    x: 0,
                    y: 0
                }
            });
        }
    }), {
    name: 'map-store',
    partialize: (state)=>({
            // Persist only essential state
            zoom: state.zoom,
            center: state.center,
            selectedYear: state.selectedYear,
            selectedDataMode: state.selectedDataMode,
            showBNBOLayer: state.showBNBOLayer,
            showControls: state.showControls
        })
}));
const getComputedLayerVisibility = (zoom)=>({
        shouldShowKommune: zoom <= KOMMUNE_MAX_ZOOM,
        shouldShowH3: zoom >= H3_MIN_ZOOM,
        currentH3Resolution: getH3ResolutionForZoom(zoom)
    });
const DATA_MODE_CONFIG = {
    pesticide_total: {
        label: 'Total Pesticide Load',
        description: 'Total pesticide load intensity per hectare',
        h3Field: 'pesticide_belastning_per_ha',
        kommuneField: 'pesticide_belastning_per_ha',
        unit: 'kg/ha',
        colorScale: 'white-red'
    },
    pfas: {
        label: 'PFAS Intensity',
        description: 'PFAS-containing pesticide intensity per hectare',
        h3Field: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
        kommuneField: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
        unit: 'g/ha',
        colorScale: 'white-red'
    },
    diquat: {
        label: 'Diquat Intensity',
        description: 'Diquat-containing pesticide intensity per hectare',
        h3Field: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
        kommuneField: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
        unit: 'g/ha',
        colorScale: 'white-red'
    },
    glyphosate: {
        label: 'Glyphosate Intensity',
        description: 'Glyphosate-containing pesticide intensity per hectare',
        h3Field: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha',
        kommuneField: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha',
        unit: 'g/ha',
        colorScale: 'white-red'
    }
};
const BNBO_STATUS_CONFIG = {
    'Action Required': {
        color: '#ff6b6b',
        label: 'Action Required',
        description: 'Areas requiring immediate action'
    },
    'Completed': {
        color: '#51cf66',
        label: 'Completed',
        description: 'Areas where action has been completed'
    },
    'Unknown': {
        color: '#868e96',
        label: 'Unknown',
        description: 'Areas with unknown status'
    }
};
const useZoom = ()=>{
    _s();
    return useMapStore({
        "useZoom.useMapStore": (state)=>state.zoom
    }["useZoom.useMapStore"]);
};
_s(useZoom, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useCenter = ()=>{
    _s1();
    return useMapStore({
        "useCenter.useMapStore": (state)=>state.center
    }["useCenter.useMapStore"]);
};
_s1(useCenter, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useBearing = ()=>{
    _s2();
    return useMapStore({
        "useBearing.useMapStore": (state)=>state.bearing
    }["useBearing.useMapStore"]);
};
_s2(useBearing, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const usePitch = ()=>{
    _s3();
    return useMapStore({
        "usePitch.useMapStore": (state)=>state.pitch
    }["usePitch.useMapStore"]);
};
_s3(usePitch, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useSelectedYear = ()=>{
    _s4();
    return useMapStore({
        "useSelectedYear.useMapStore": (state)=>state.selectedYear
    }["useSelectedYear.useMapStore"]);
};
_s4(useSelectedYear, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useSelectedDataMode = ()=>{
    _s5();
    return useMapStore({
        "useSelectedDataMode.useMapStore": (state)=>state.selectedDataMode
    }["useSelectedDataMode.useMapStore"]);
};
_s5(useSelectedDataMode, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useAvailableYears = ()=>{
    _s6();
    return useMapStore({
        "useAvailableYears.useMapStore": (state)=>state.availableYears
    }["useAvailableYears.useMapStore"]);
};
_s6(useAvailableYears, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useAvailableYearOptions = ()=>{
    _s7();
    return useMapStore({
        "useAvailableYearOptions.useMapStore": (state)=>state.availableYearOptions
    }["useAvailableYearOptions.useMapStore"]);
};
_s7(useAvailableYearOptions, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useAvailableResolutions = ()=>{
    _s8();
    return useMapStore({
        "useAvailableResolutions.useMapStore": (state)=>state.availableResolutions
    }["useAvailableResolutions.useMapStore"]);
};
_s8(useAvailableResolutions, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowBNBOLayer = ()=>{
    _s9();
    return useMapStore({
        "useShowBNBOLayer.useMapStore": (state)=>state.showBNBOLayer
    }["useShowBNBOLayer.useMapStore"]);
};
_s9(useShowBNBOLayer, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowBasemap = ()=>{
    _s10();
    return useMapStore({
        "useShowBasemap.useMapStore": (state)=>state.showBasemap
    }["useShowBasemap.useMapStore"]);
};
_s10(useShowBasemap, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoading = ()=>{
    _s11();
    return useMapStore({
        "useIsLoading.useMapStore": (state)=>state.isLoading
    }["useIsLoading.useMapStore"]);
};
_s11(useIsLoading, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoadingYear = ()=>{
    _s12();
    return useMapStore({
        "useIsLoadingYear.useMapStore": (state)=>state.isLoadingYear
    }["useIsLoadingYear.useMapStore"]);
};
_s12(useIsLoadingYear, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoadingTiles = ()=>{
    _s13();
    return useMapStore({
        "useIsLoadingTiles.useMapStore": (state)=>state.isLoadingTiles
    }["useIsLoadingTiles.useMapStore"]);
};
_s13(useIsLoadingTiles, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useLoadingMessage = ()=>{
    _s14();
    return useMapStore({
        "useLoadingMessage.useMapStore": (state)=>state.loadingMessage
    }["useLoadingMessage.useMapStore"]);
};
_s14(useLoadingMessage, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useError = ()=>{
    _s15();
    return useMapStore({
        "useError.useMapStore": (state)=>state.error
    }["useError.useMapStore"]);
};
_s15(useError, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowControls = ()=>{
    _s16();
    return useMapStore({
        "useShowControls.useMapStore": (state)=>state.showControls
    }["useShowControls.useMapStore"]);
};
_s16(useShowControls, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowTooltip = ()=>{
    _s17();
    return useMapStore({
        "useShowTooltip.useMapStore": (state)=>state.showTooltip
    }["useShowTooltip.useMapStore"]);
};
_s17(useShowTooltip, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useTooltipData = ()=>{
    _s18();
    return useMapStore({
        "useTooltipData.useMapStore": (state)=>state.tooltipData
    }["useTooltipData.useMapStore"]);
};
_s18(useTooltipData, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useTooltipPosition = ()=>{
    _s19();
    return useMapStore({
        "useTooltipPosition.useMapStore": (state)=>state.tooltipPosition
    }["useTooltipPosition.useMapStore"]);
};
_s19(useTooltipPosition, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useMapViewState = ()=>{
    _s20();
    const zoom = useZoom();
    const center = useCenter();
    const bearing = useBearing();
    const pitch = usePitch();
    return {
        zoom,
        center,
        bearing,
        pitch
    };
};
_s20(useMapViewState, "UgxXP0nHga0ZRG28hQjNYKUeIaY=", false, function() {
    return [
        useZoom,
        useCenter,
        useBearing,
        usePitch
    ];
});
const useDataState = ()=>{
    _s21();
    const selectedYear = useSelectedYear();
    const selectedDataMode = useSelectedDataMode();
    const availableYears = useAvailableYears();
    const availableYearOptions = useAvailableYearOptions();
    const availableResolutions = useAvailableResolutions();
    return {
        selectedYear,
        selectedDataMode,
        availableYears,
        availableYearOptions,
        availableResolutions
    };
};
_s21(useDataState, "Mh5qtpm+XwztK2E8QDnalHpOt0k=", false, function() {
    return [
        useSelectedYear,
        useSelectedDataMode,
        useAvailableYears,
        useAvailableYearOptions,
        useAvailableResolutions
    ];
});
const useLayerVisibility = ()=>{
    _s22();
    const zoom = useZoom();
    const showBNBOLayer = useShowBNBOLayer();
    const showBasemap = useShowBasemap();
    const computed = getComputedLayerVisibility(zoom);
    return {
        showBNBOLayer,
        showBasemap,
        ...computed
    };
};
_s22(useLayerVisibility, "XUgWOAQAPrCwTQKLmeWmXLKgFHc=", false, function() {
    return [
        useZoom,
        useShowBNBOLayer,
        useShowBasemap
    ];
});
const useLoadingState = ()=>{
    _s23();
    const isLoading = useIsLoading();
    const isLoadingYear = useIsLoadingYear();
    const isLoadingTiles = useIsLoadingTiles();
    const loadingMessage = useLoadingMessage();
    const error = useError();
    return {
        isLoading,
        isLoadingYear,
        isLoadingTiles,
        loadingMessage,
        error
    };
};
_s23(useLoadingState, "ik6VbIBVYUirotiPuBBfnGe59cA=", false, function() {
    return [
        useIsLoading,
        useIsLoadingYear,
        useIsLoadingTiles,
        useLoadingMessage,
        useError
    ];
});
const useTooltipState = ()=>{
    _s24();
    const showTooltip = useShowTooltip();
    const tooltipData = useTooltipData();
    const tooltipPosition = useTooltipPosition();
    return {
        showTooltip,
        tooltipData,
        tooltipPosition
    };
};
_s24(useTooltipState, "STrPZnizyzIARKPCu+euDHEFu8c=", false, function() {
    return [
        useShowTooltip,
        useTooltipData,
        useTooltipPosition
    ];
});
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
// PMTiles Discovery Service - Browser Compatible Version
// This service handles discovery of PMTiles files from GCS bucket
__turbopack_context__.s({
    "pmtilesDiscovery": (()=>pmtilesDiscovery)
});
class PMTilesDiscoveryService {
    cache = new Map();
    baseUrl = 'https://storage.googleapis.com/landbrugsdata-raw-data';
    // Discover available data by checking GCS bucket structure
    async getDataAvailability() {
        const cacheKey = 'data_availability';
        if (this.cache.has(cacheKey)) {
            return this.cache.get(cacheKey);
        }
        try {
            // For now, return known structure - in practice, this would query GCS API
            const availability = {
                years: [
                    2015,
                    2016,
                    2017,
                    2018,
                    2019,
                    2020,
                    2021,
                    2022,
                    2023
                ],
                resolutions: [
                    7,
                    8,
                    9,
                    10
                ],
                latestYear: 2023,
                latestResolution: 10
            };
            this.cache.set(cacheKey, availability);
            return availability;
        } catch (error) {
            console.warn('Failed to discover data availability, using fallback:', error);
            // Fallback to known structure
            const fallback = {
                years: [
                    2023
                ],
                resolutions: [
                    7,
                    8,
                    9,
                    10
                ],
                latestYear: 2023,
                latestResolution: 10
            };
            return fallback;
        }
    }
    async discoverBasemapTiles() {
        return `${this.baseUrl}/pmtiles/protomaps_denmark.pmtiles`;
    }
    async discoverLatestBNBOTiles() {
        return `${this.baseUrl}/pmtiles/bnbo_areas.pmtiles`;
    }
    async discoverLatestH3Tiles(year, resolution) {
        const cacheKey = `h3_${year}_${resolution}`;
        if (this.cache.has(cacheKey)) {
            return this.cache.get(cacheKey);
        }
        try {
            const pattern = `gold/pmtiles/h3_pfas_${year}_res${resolution}`;
            const latestTimestamp = await this._discoverLatestTimestamp(pattern);
            const url = `${this.baseUrl}/${pattern}/${latestTimestamp}/h3_pfas_${year}_res${resolution}.pmtiles`;
            this.cache.set(cacheKey, url);
            return url;
        } catch (error) {
            console.error(`Failed to discover H3 tiles for ${year} res${resolution}:`, error);
            throw error;
        }
    }
    async discoverLatestKommuneTiles(year) {
        const cacheKey = `kommune_${year}`;
        if (this.cache.has(cacheKey)) {
            return this.cache.get(cacheKey);
        }
        try {
            const pattern = `gold/pmtiles/kommune_pfas_${year}`;
            const latestTimestamp = await this._discoverLatestTimestamp(pattern);
            const url = `${this.baseUrl}/${pattern}/${latestTimestamp}/kommune_pfas_${year}.pmtiles`;
            this.cache.set(cacheKey, url);
            return url;
        } catch (error) {
            console.error(`Failed to discover kommune tiles for ${year}:`, error);
            throw error;
        }
    }
    async _discoverLatestTimestamp(pattern) {
        try {
            // Try to list directory contents via GCS JSON API with timeout
            const listUrl = `https://storage.googleapis.com/storage/v1/b/landbrugsdata-raw-data/o?prefix=${pattern}/&delimiter=/`;
            const controller = new AbortController();
            const timeoutId = setTimeout(()=>controller.abort(), 5000); // 5 second timeout
            const response = await fetch(listUrl, {
                signal: controller.signal
            });
            clearTimeout(timeoutId);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const data = await response.json();
            if (data.prefixes && data.prefixes.length > 0) {
                // Extract timestamps from prefixes like "gold/pmtiles/h3_pfas_2023_res10/20250705_181521/"
                const timestamps = data.prefixes.map((prefix)=>{
                    const parts = prefix.split('/');
                    return parts[parts.length - 2]; // Get the timestamp part
                }).filter((ts)=>/^\d{8}_\d{6}$/.test(ts)) // Validate timestamp format
                .sort().reverse(); // Latest first
                if (timestamps.length > 0) {
                    return timestamps[0];
                }
            }
            throw new Error('No timestamps found');
        } catch (error) {
            console.warn(`Failed to discover timestamp for ${pattern}:`, error);
            throw error;
        }
    }
    async getAvailableYears() {
        const availability = await this.getDataAvailability();
        return availability.years;
    }
    async getAvailableResolutions() {
        const availability = await this.getDataAvailability();
        return availability.resolutions;
    }
    // Helper method to get PMTiles URL for a specific type
    async getPMTilesUrl(type) {
        switch(type){
            case 'basemap':
                return this.discoverBasemapTiles();
            case 'bnbo':
                return this.discoverLatestBNBOTiles();
            default:
                throw new Error(`Unknown PMTiles type: ${type}`);
        }
    }
    // Get all URLs for a specific year
    async getYearUrls(year) {
        const [basemap, bnbo] = await Promise.all([
            this.discoverBasemapTiles(),
            this.discoverLatestBNBOTiles()
        ]);
        // Get H3 URLs for all resolutions
        const h3Urls = {};
        const resolutions = await this.getAvailableResolutions();
        for (const resolution of resolutions){
            const key = `${year}_${resolution}`;
            try {
                h3Urls[key] = await this.discoverLatestH3Tiles(year, resolution);
            } catch (error) {
                console.warn(`Failed to get H3 URL for ${year} res${resolution}:`, error);
            }
        }
        // Get kommune URL
        const kommuneUrls = {};
        try {
            kommuneUrls[year.toString()] = await this.discoverLatestKommuneTiles(year);
        } catch (error) {
            console.warn(`Failed to get kommune URL for ${year}:`, error);
        }
        return {
            basemap,
            h3: h3Urls,
            kommune: kommuneUrls,
            bnbo
        };
    }
    // Clear cache to force re-discovery
    clearCache() {
        this.cache.clear();
    }
    // Test if a URL is accessible
    async testUrl(url) {
        try {
            const response = await fetch(url, {
                method: 'HEAD'
            });
            return response.ok;
        } catch (error) {
            console.warn(`URL test failed for ${url}:`, error);
            return false;
        }
    }
    // Get URLs directly without validation
    async discoverAndValidateUrls(year, resolution) {
        try {
            const [basemapUrl, bnboUrl, h3Url, kommuneUrl] = await Promise.all([
                this.discoverBasemapTiles(),
                this.discoverLatestBNBOTiles(),
                this.discoverLatestH3Tiles(year, resolution),
                this.discoverLatestKommuneTiles(year)
            ]);
            return {
                basemap: basemapUrl,
                bnbo: bnboUrl,
                h3: h3Url,
                kommune: kommuneUrl
            };
        } catch (error) {
            console.error('URL discovery failed:', error);
            return {
                h3: null,
                kommune: null,
                basemap: null,
                bnbo: null
            };
        }
    }
}
const pmtilesDiscovery = new PMTilesDiscoveryService();
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/map/PMTilesMap.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "PMTilesMap": (()=>PMTilesMap)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$react$2d$error$2d$boundary$2f$dist$2f$react$2d$error$2d$boundary$2e$development$2e$esm$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/react-error-boundary/dist/react-error-boundary.development.esm.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
;
;
;
// Dynamic imports for browser-only modules
const loadMapLibreAndPMTiles = async ()=>{
    if ("TURBOPACK compile-time falsy", 0) {
        "TURBOPACK unreachable";
    }
    console.log('🔄 Loading MapLibre and PMTiles...');
    const [maplibregl, { Protocol }] = await Promise.all([
        __turbopack_context__.r("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-client] (ecmascript, async loader)")(__turbopack_context__.i),
        __turbopack_context__.r("[project]/node_modules/pmtiles/dist/index.js [app-client] (ecmascript, async loader)")(__turbopack_context__.i)
    ]);
    console.log('✅ MapLibre and PMTiles loaded successfully');
    // Register PMTiles protocol
    let protocolRegistered = false;
    if (!protocolRegistered) {
        const protocol = new Protocol();
        maplibregl.default.addProtocol('pmtiles', protocol.tile);
        protocolRegistered = true;
        console.log('✅ PMTiles protocol registered');
    }
    return maplibregl.default;
};
const PMTilesMapInner = ({ className = 'w-full h-full' })=>{
    _s();
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    const map = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    const [mapLibre, setMapLibre] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    const [mapLoaded, setMapLoaded] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [pmtilesUrls, setPmtilesUrls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({});
    // Store state
    const { zoom, center, bearing, pitch } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapViewState"])();
    const { selectedYear, selectedDataMode, availableYears } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const showBNBOLayer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])({
        "PMTilesMapInner.useMapStore[showBNBOLayer]": (state)=>state.showBNBOLayer
    }["PMTilesMapInner.useMapStore[showBNBOLayer]"]);
    const showBasemap = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])({
        "PMTilesMapInner.useMapStore[showBasemap]": (state)=>state.showBasemap
    }["PMTilesMapInner.useMapStore[showBasemap]"]);
    const { isLoading, error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"])();
    // Compute layer visibility based on zoom (stable) - use centralized function
    const layerVisibility = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getComputedLayerVisibility"])(zoom);
    const shouldShowKommune = layerVisibility.shouldShowKommune;
    const shouldShowH3 = layerVisibility.shouldShowH3;
    const currentH3Resolution = layerVisibility.currentH3Resolution;
    // Get property name based on data mode - using actual property names from tooltip
    const getPropertyName = (mode)=>{
        switch(mode){
            case 'pfas':
                return 'pfas_grams'; // Based on tooltip data
            case 'diquat':
                return 'diquat_grams'; // Based on tooltip data
            case 'glyphosate':
                return 'glyphosate_grams'; // Based on tooltip data
            default:
                return 'pesticide_load'; // Based on tooltip data
        }
    };
    // Get current property name for styling
    const currentPropertyName = getPropertyName(selectedDataMode);
    // Store actions
    const { setViewState, setIsLoading, setError, clearError, showTooltipWithData, hideTooltip, setAvailableYears } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Load MapLibre and PMTiles
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            let mounted = true;
            const initMapLibre = {
                "PMTilesMapInner.useEffect.initMapLibre": async ()=>{
                    try {
                        setIsLoading(true);
                        const mapLibreInstance = await loadMapLibreAndPMTiles();
                        if (mounted && mapLibreInstance) {
                            setMapLibre(mapLibreInstance);
                        }
                    } catch (error) {
                        console.error('Error loading MapLibre:', error);
                        if (mounted) {
                            setError('Failed to load mapping library');
                        }
                    } finally{
                        if (mounted) {
                            setIsLoading(false);
                        }
                    }
                }
            }["PMTilesMapInner.useEffect.initMapLibre"];
            initMapLibre();
            return ({
                "PMTilesMapInner.useEffect": ()=>{
                    mounted = false;
                }
            })["PMTilesMapInner.useEffect"];
        }
    }["PMTilesMapInner.useEffect"], []);
    // Load PMTiles URLs
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            console.log('🔄 PMTiles useEffect triggered - year:', selectedYear, 'resolution:', currentH3Resolution, 'zoom:', zoom);
            let mounted = true;
            const loadPMTilesUrls = {
                "PMTilesMapInner.useEffect.loadPMTilesUrls": async ()=>{
                    try {
                        setIsLoading(true);
                        console.log('🔍 Starting PMTiles URL discovery for:', {
                            selectedYear,
                            currentH3Resolution
                        });
                        // Discover and validate URLs
                        const [validatedUrls, years] = await Promise.all([
                            __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].discoverAndValidateUrls(selectedYear, currentH3Resolution),
                            __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getAvailableYears()
                        ]);
                        console.log('🔍 PMTiles URL discovery results:', validatedUrls);
                        if (mounted) {
                            // Only set URLs that are valid
                            const urls = {};
                            if (validatedUrls.basemap) urls.basemap = validatedUrls.basemap;
                            if (validatedUrls.kommune) urls.kommune = validatedUrls.kommune;
                            if (validatedUrls.h3) urls.h3 = validatedUrls.h3;
                            if (validatedUrls.bnbo) urls.bnbo = validatedUrls.bnbo;
                            setPmtilesUrls(urls);
                            // Remove setAvailableYears to prevent potential loop
                            // setAvailableYears(years)
                            // Log what we found
                            console.log('🗺️ PMTiles URLs discovered:', {
                                basemap: validatedUrls.basemap ? '✅' : '❌',
                                kommune: validatedUrls.kommune ? '✅' : '❌',
                                h3: validatedUrls.h3 ? '✅' : '❌',
                                bnbo: validatedUrls.bnbo ? '✅' : '❌'
                            });
                            // Log actual URLs for debugging
                            console.log('📍 Actual URLs:', {
                                basemap: validatedUrls.basemap,
                                kommune: validatedUrls.kommune,
                                h3: validatedUrls.h3,
                                bnbo: validatedUrls.bnbo
                            });
                            // Test URL accessibility
                            if (validatedUrls.basemap) {
                                console.log('🔗 Testing basemap URL accessibility...');
                                __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].testUrl(validatedUrls.basemap).then({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (isAccessible)=>{
                                        console.log('🔗 Basemap URL accessible:', isAccessible);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]).catch({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (err)=>{
                                        console.error('🔗 Basemap URL test failed:', err);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]);
                            }
                            if (validatedUrls.h3) {
                                console.log('🔗 Testing H3 URL accessibility...');
                                __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].testUrl(validatedUrls.h3).then({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (isAccessible)=>{
                                        console.log('🔗 H3 URL accessible:', isAccessible);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]).catch({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (err)=>{
                                        console.error('🔗 H3 URL test failed:', err);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]);
                            }
                            if (validatedUrls.kommune) {
                                console.log('🔗 Testing Kommune URL accessibility...');
                                __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].testUrl(validatedUrls.kommune).then({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (isAccessible)=>{
                                        console.log('🔗 Kommune URL accessible:', isAccessible);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]).catch({
                                    "PMTilesMapInner.useEffect.loadPMTilesUrls": (err)=>{
                                        console.error('🔗 Kommune URL test failed:', err);
                                    }
                                }["PMTilesMapInner.useEffect.loadPMTilesUrls"]);
                            }
                            if (!validatedUrls.basemap) {
                                console.error('❌ No basemap URL available');
                                setError('Basemap not available');
                            } else {
                                clearError();
                            }
                        }
                    } catch (error) {
                        console.error('❌ Error loading PMTiles URLs:', error);
                        if (mounted) {
                            setError('Failed to load data sources');
                        }
                    } finally{
                        if (mounted) {
                            setIsLoading(false);
                        }
                    }
                }
            }["PMTilesMapInner.useEffect.loadPMTilesUrls"];
            loadPMTilesUrls();
            return ({
                "PMTilesMapInner.useEffect": ()=>{
                    mounted = false;
                }
            })["PMTilesMapInner.useEffect"];
        }
    }["PMTilesMapInner.useEffect"], [
        selectedYear,
        currentH3Resolution
    ]);
    // Initialize map
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            console.log('🔍 Map initialization check:', {
                mapLibre: !!mapLibre,
                mapContainer: !!mapContainer.current,
                basemapUrl: pmtilesUrls.basemap,
                pmtilesUrls: pmtilesUrls
            });
            if (!mapLibre || !mapContainer.current || !pmtilesUrls.basemap) {
                console.log('⏳ Map initialization skipped - missing dependencies:', {
                    mapLibre: !!mapLibre,
                    mapContainer: !!mapContainer.current,
                    basemapUrl: !!pmtilesUrls.basemap,
                    allUrls: Object.keys(pmtilesUrls)
                });
                return;
            }
            try {
                console.log('🚀 Creating map with URLs:', pmtilesUrls);
                // Validate URLs before creating map
                const requiredSources = [
                    'basemap'
                ];
                const optionalSources = [
                    'kommune',
                    'h3',
                    'bnbo'
                ];
                console.log('🔍 Validating required sources:', requiredSources.map({
                    "PMTilesMapInner.useEffect": (src)=>({
                            source: src,
                            hasUrl: !!pmtilesUrls[src],
                            url: pmtilesUrls[src]
                        })
                }["PMTilesMapInner.useEffect"]));
                console.log('🔍 Validating optional sources:', optionalSources.map({
                    "PMTilesMapInner.useEffect": (src)=>({
                            source: src,
                            hasUrl: !!pmtilesUrls[src],
                            url: pmtilesUrls[src]
                        })
                }["PMTilesMapInner.useEffect"]));
                // Create sources object with only available URLs
                const sources = {};
                if (pmtilesUrls.basemap) {
                    sources.basemap = {
                        type: 'vector',
                        url: `pmtiles://${pmtilesUrls.basemap}`
                    };
                    console.log('✅ Added basemap source');
                }
                if (pmtilesUrls.kommune) {
                    sources.kommune = {
                        type: 'vector',
                        url: `pmtiles://${pmtilesUrls.kommune}`
                    };
                    console.log('✅ Added kommune source');
                }
                if (pmtilesUrls.h3) {
                    sources.h3 = {
                        type: 'vector',
                        url: `pmtiles://${pmtilesUrls.h3}`
                    };
                    console.log('✅ Added h3 source');
                }
                if (pmtilesUrls.bnbo) {
                    sources.bnbo = {
                        type: 'vector',
                        url: `pmtiles://${pmtilesUrls.bnbo}`
                    };
                    console.log('✅ Added bnbo source:', pmtilesUrls.bnbo);
                } else {
                    console.log('❌ No BNBO URL found in pmtilesUrls:', pmtilesUrls);
                }
                console.log('🗺️ Final sources configuration:', sources);
                // Create layers array with only layers for available sources
                // Layer order matters: layers added later appear on top
                // Order: basemap (bottom) -> kommune -> h3 -> bnbo (top)
                const layers = [];
                // Always add basemap layer if available (bottom layer)
                if (sources.basemap) {
                    layers.push({
                        id: 'basemap-fill',
                        type: 'fill',
                        source: 'basemap',
                        'source-layer': 'earth',
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': '#f8f8f8',
                            'fill-opacity': 1
                        }
                    }, {
                        id: 'basemap-water',
                        type: 'fill',
                        source: 'basemap',
                        'source-layer': 'water',
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': '#a8d1e8',
                            'fill-opacity': 1
                        }
                    }, {
                        id: 'basemap-landuse',
                        type: 'fill',
                        source: 'basemap',
                        'source-layer': 'landuse',
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
                                'match',
                                [
                                    'get',
                                    'kind'
                                ],
                                'park',
                                '#c8e6c9',
                                'forest',
                                '#a5d6a7',
                                'residential',
                                '#f5f5f5',
                                'commercial',
                                '#e8e8e8',
                                'industrial',
                                '#d6d6d6',
                                'farmland',
                                '#e8f5e8',
                                '#f0f0f0'
                            ],
                            'fill-opacity': 0.6
                        }
                    }, {
                        id: 'basemap-buildings',
                        type: 'fill',
                        source: 'basemap',
                        'source-layer': 'buildings',
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': '#d0d0d0',
                            'fill-opacity': 0.8
                        }
                    }, {
                        id: 'basemap-buildings-stroke',
                        type: 'line',
                        source: 'basemap',
                        'source-layer': 'buildings',
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#b0b0b0',
                            'line-width': 0.5,
                            'line-opacity': 0.5
                        }
                    }, {
                        id: 'basemap-roads-minor',
                        type: 'line',
                        source: 'basemap',
                        'source-layer': 'roads',
                        filter: [
                            'in',
                            [
                                'get',
                                'kind'
                            ],
                            [
                                'literal',
                                [
                                    'minor_road',
                                    'path'
                                ]
                            ]
                        ],
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'zoom'
                                ],
                                8,
                                0.5,
                                12,
                                1,
                                16,
                                2
                            ],
                            'line-opacity': 0.8
                        }
                    }, {
                        id: 'basemap-roads-major',
                        type: 'line',
                        source: 'basemap',
                        'source-layer': 'roads',
                        filter: [
                            'in',
                            [
                                'get',
                                'kind'
                            ],
                            [
                                'literal',
                                [
                                    'highway',
                                    'major_road'
                                ]
                            ]
                        ],
                        layout: {
                            visibility: showBasemap ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'zoom'
                                ],
                                6,
                                1,
                                10,
                                2,
                                14,
                                4,
                                16,
                                6
                            ],
                            'line-opacity': 1
                        }
                    });
                    console.log('✅ Added basemap layers with buildings and roads');
                }
                // Add kommune layers if available
                if (sources.kommune) {
                    layers.push({
                        id: 'kommune-fill',
                        type: 'fill',
                        source: 'kommune',
                        'source-layer': `kommune_pfas_${selectedYear}`,
                        layout: {
                            visibility: shouldShowKommune ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'get',
                                    currentPropertyName
                                ],
                                0,
                                'transparent',
                                0.1,
                                '#fee5d9',
                                1,
                                '#fcbba1',
                                5,
                                '#fc9272',
                                10,
                                '#fb6a4a',
                                20,
                                '#ef3b2c',
                                50,
                                '#cb181d',
                                100,
                                '#99000d'
                            ],
                            'fill-opacity': 0.8
                        }
                    }, {
                        id: 'kommune-stroke',
                        type: 'line',
                        source: 'kommune',
                        'source-layer': `kommune_pfas_${selectedYear}`,
                        layout: {
                            visibility: shouldShowKommune ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 0.5,
                            'line-opacity': 0.5
                        }
                    });
                    console.log('✅ Added kommune layers');
                }
                // Add H3 layers if available
                if (sources.h3) {
                    layers.push({
                        id: 'h3-fill',
                        type: 'fill',
                        source: 'h3',
                        'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`,
                        layout: {
                            visibility: shouldShowH3 ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'get',
                                    currentPropertyName
                                ],
                                0,
                                'transparent',
                                0.1,
                                '#fee5d9',
                                1,
                                '#fcbba1',
                                5,
                                '#fc9272',
                                10,
                                '#fb6a4a',
                                20,
                                '#ef3b2c',
                                50,
                                '#cb181d',
                                100,
                                '#99000d'
                            ],
                            'fill-opacity': 0.7
                        }
                    }, {
                        id: 'h3-stroke',
                        type: 'line',
                        source: 'h3',
                        'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`,
                        layout: {
                            visibility: shouldShowH3 ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 0.2,
                            'line-opacity': 0.3
                        }
                    });
                    console.log('✅ Added H3 layers');
                }
                // Add BNBO layers if available - ALWAYS VISIBLE and ON TOP
                if (sources.bnbo) {
                    layers.push({
                        id: 'bnbo-fill',
                        type: 'fill',
                        source: 'bnbo',
                        'source-layer': 'bnbo',
                        layout: {
                            visibility: 'visible' // Always visible
                        },
                        paint: {
                            'fill-color': [
                                'case',
                                [
                                    'has',
                                    'status_category'
                                ],
                                [
                                    'match',
                                    [
                                        'get',
                                        'status_category'
                                    ],
                                    'Action Required',
                                    '#ff6b6b',
                                    'Completed',
                                    '#51cf66',
                                    'Unknown',
                                    '#868e96',
                                    '#cccccc'
                                ],
                                [
                                    'has',
                                    'status'
                                ],
                                [
                                    'match',
                                    [
                                        'get',
                                        'status'
                                    ],
                                    'Action Required',
                                    '#ff6b6b',
                                    'Completed',
                                    '#51cf66',
                                    'Unknown',
                                    '#868e96',
                                    '#cccccc'
                                ],
                                '#ff00ff' // Bright magenta fallback to make any BNBO areas visible
                            ],
                            'fill-opacity': 0.8 // Increased opacity to make them more visible
                        }
                    }, {
                        id: 'bnbo-stroke',
                        type: 'line',
                        source: 'bnbo',
                        'source-layer': 'bnbo',
                        layout: {
                            visibility: 'visible' // Always visible
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 1,
                            'line-opacity': 0.8
                        }
                    });
                    console.log('✅ Added BNBO layers (always visible and on top)');
                    console.log('🔍 BNBO source configuration:', sources.bnbo);
                } else {
                    console.log('❌ No BNBO source available for layers');
                }
                console.log('🗺️ Final layers configuration:', layers.map({
                    "PMTilesMapInner.useEffect": (l)=>({
                            id: l.id,
                            source: l.source
                        })
                }["PMTilesMapInner.useEffect"]));
                map.current = new mapLibre.Map({
                    container: mapContainer.current,
                    style: {
                        version: 8,
                        sources: sources,
                        layers: layers
                    },
                    center: center,
                    zoom: zoom,
                    bearing: bearing,
                    pitch: pitch,
                    maxZoom: 15,
                    minZoom: 4,
                    maxBounds: [
                        [
                            7.0,
                            54.0
                        ],
                        [
                            13.0,
                            58.0
                        ]
                    ]
                });
                // Add controls
                map.current.addControl(new mapLibre.NavigationControl(), 'top-right');
                map.current.addControl(new mapLibre.ScaleControl(), 'bottom-left');
                // Map event handlers
                map.current.on('load', {
                    "PMTilesMapInner.useEffect": ()=>{
                        console.log('🎉 Map loaded successfully!');
                        // Debug: Check available sources and layers
                        const style = map.current.getStyle();
                        console.log('📊 Map style sources:', Object.keys(style.sources));
                        console.log('📊 Map style layers:', style.layers.map({
                            "PMTilesMapInner.useEffect": (l)=>({
                                    id: l.id,
                                    type: l.type,
                                    source: l.source,
                                    'source-layer': l['source-layer']
                                })
                        }["PMTilesMapInner.useEffect"]));
                        // Debug: Try to get source data and inspect tiles
                        setTimeout({
                            "PMTilesMapInner.useEffect": ()=>{
                                if (!map.current) return;
                                console.log(`📊 Current zoom: ${map.current.getZoom()}, center:`, map.current.getCenter());
                                // Try to inspect each source more thoroughly
                                const sources = [
                                    'basemap',
                                    'kommune',
                                    'h3',
                                    'bnbo'
                                ];
                                sources.forEach({
                                    "PMTilesMapInner.useEffect": (sourceId)=>{
                                        if (!map.current) return;
                                        const source = map.current.getSource(sourceId);
                                        if (source) {
                                            console.log(`📊 Source ${sourceId}:`, source);
                                            // Check if source has loaded tiles
                                            if (source._tiles) {
                                                console.log(`📊 ${sourceId} has ${Object.keys(source._tiles).length} loaded tiles`);
                                            }
                                            // Try to query all features from this source
                                            try {
                                                if (!map.current) return;
                                                const allFeatures = map.current.querySourceFeatures(sourceId);
                                                console.log(`📊 ${sourceId} total features: ${allFeatures.length}`);
                                                if (allFeatures.length > 0) {
                                                    const sampleFeature = allFeatures[0];
                                                    console.log(`📊 ${sourceId} sample feature:`, sampleFeature);
                                                    console.log(`📊 ${sourceId} sample properties:`, sampleFeature.properties);
                                                    console.log(`📊 ${sourceId} source-layer:`, sampleFeature.sourceLayer);
                                                }
                                            } catch (e) {
                                                console.log(`📊 Could not query ${sourceId} features:`, e);
                                            }
                                        }
                                    }
                                }["PMTilesMapInner.useEffect"]);
                                // Also try to get all rendered features at current view
                                try {
                                    if (!map.current) return;
                                    const allRenderedFeatures = map.current.queryRenderedFeatures();
                                    console.log(`📊 Total rendered features in view: ${allRenderedFeatures.length}`);
                                    if (allRenderedFeatures.length > 0) {
                                        const sourceLayerCounts = allRenderedFeatures.reduce({
                                            "PMTilesMapInner.useEffect.sourceLayerCounts": (acc, f)=>{
                                                const key = `${f.source}:${f.sourceLayer}`;
                                                acc[key] = (acc[key] || 0) + 1;
                                                return acc;
                                            }
                                        }["PMTilesMapInner.useEffect.sourceLayerCounts"], {});
                                        console.log(`📊 Rendered features by source:layer:`, sourceLayerCounts);
                                        // Check specifically for BNBO features
                                        const bnboFeatures = allRenderedFeatures.filter({
                                            "PMTilesMapInner.useEffect.bnboFeatures": (f)=>f.source === 'bnbo'
                                        }["PMTilesMapInner.useEffect.bnboFeatures"]);
                                        if (bnboFeatures.length > 0) {
                                            console.log(`🛡️ Found ${bnboFeatures.length} BNBO features:`, bnboFeatures.slice(0, 3));
                                        } else {
                                            console.log(`🛡️ No BNBO features found in rendered features`);
                                        }
                                    }
                                } catch (e) {
                                    console.log(`📊 Could not query rendered features:`, e);
                                }
                            }
                        }["PMTilesMapInner.useEffect"], 3000);
                        setMapLoaded(true);
                        clearError();
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('move', {
                    "PMTilesMapInner.useEffect": ()=>{
                        if (!map.current) return;
                        const { lng, lat } = map.current.getCenter();
                        const zoom = map.current.getZoom();
                        const bearing = map.current.getBearing();
                        const pitch = map.current.getPitch();
                        setViewState({
                            center: [
                                lng,
                                lat
                            ],
                            zoom,
                            bearing,
                            pitch
                        });
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('click', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        const features = map.current?.queryRenderedFeatures(e.point, {
                            layers: [
                                'kommune-fill',
                                'h3-fill',
                                'bnbo-fill'
                            ]
                        });
                        if (features && features.length > 0) {
                            const feature = features[0];
                            showTooltipWithData(feature.properties, {
                                x: e.point.x,
                                y: e.point.y
                            });
                        } else {
                            hideTooltip();
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('mousemove', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        const features = map.current?.queryRenderedFeatures(e.point, {
                            layers: [
                                'kommune-fill',
                                'h3-fill',
                                'bnbo-fill'
                            ]
                        });
                        if (features && features.length > 0) {
                            const feature = features[0];
                            showTooltipWithData(feature.properties, {
                                x: e.point.x,
                                y: e.point.y
                            });
                            if (map.current) {
                                map.current.getCanvas().style.cursor = 'pointer';
                            }
                        } else {
                            hideTooltip();
                            if (map.current) {
                                map.current.getCanvas().style.cursor = '';
                            }
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('mouseleave', {
                    "PMTilesMapInner.useEffect": ()=>{
                        hideTooltip();
                        if (map.current) {
                            map.current.getCanvas().style.cursor = '';
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('error', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        console.error('❌ Map error:', e);
                        console.error('❌ Map error details:', {
                            type: e.type,
                            error: e.error,
                            sourceId: e.sourceId,
                            tile: e.tile,
                            target: e.target,
                            originalTarget: e.originalTarget,
                            message: e.message,
                            stack: e.stack
                        });
                        // Safely log error properties without circular references
                        const safeErrorProps = {
                            type: e.type,
                            message: e.message,
                            sourceId: e.sourceId,
                            errorMessage: e.error?.message,
                            errorStack: e.error?.stack,
                            url: e.url
                        };
                        console.error('❌ Safe error object:', safeErrorProps);
                        // Try to get more specific error information
                        let errorMessage = 'Unknown map error';
                        if (e.error && e.error.message) {
                            errorMessage = e.error.message;
                        } else if (e.message) {
                            errorMessage = e.message;
                        } else if (e.sourceId) {
                            errorMessage = `Source error: ${e.sourceId}`;
                        }
                        setError(`Map loading error: ${errorMessage}`);
                    }
                }["PMTilesMapInner.useEffect"]);
                // Add more specific error handlers
                map.current.on('sourceerror', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        console.error('❌ Source error:', e);
                        console.error('❌ Source error details:', {
                            sourceId: e.sourceId,
                            error: e.error,
                            url: e.url,
                            message: e.message
                        });
                        setError(`Source loading error: ${e.sourceId}`);
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('styleerror', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        console.error('❌ Style error:', e);
                        console.error('❌ Style error details:', {
                            error: e.error,
                            message: e.message
                        });
                        setError(`Style error: ${e.error?.message || 'Unknown style error'}`);
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('sourcedata', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        if (e.isSourceLoaded) {
                            console.log(`📊 Source loaded: ${e.sourceId}`, e);
                        } else if (e.dataType === 'source') {
                            console.log(`📊 Source data loading: ${e.sourceId}`, e);
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                map.current.on('sourcedataloading', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        console.log(`📊 Source loading: ${e.sourceId}`, e);
                    }
                }["PMTilesMapInner.useEffect"]);
                // Add data event handler to track tile loading
                map.current.on('data', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        if (e.dataType === 'source') {
                            console.log(`📊 Data event for source: ${e.sourceId}`, {
                                dataType: e.dataType,
                                isSourceLoaded: e.isSourceLoaded,
                                sourceDataType: e.sourceDataType
                            });
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                // Add tile events to track individual tile loading
                map.current.on('dataloading', {
                    "PMTilesMapInner.useEffect": (e)=>{
                        if (e.dataType === 'source') {
                            console.log(`📊 Data loading for source: ${e.sourceId}`);
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
            } catch (err) {
                console.error('❌ Error initializing map:', err);
                setError('Failed to initialize map');
            }
            return ({
                "PMTilesMapInner.useEffect": ()=>{
                    if (map.current) {
                        map.current.remove();
                        map.current = null;
                    }
                }
            })["PMTilesMapInner.useEffect"];
        }
    }["PMTilesMapInner.useEffect"], [
        mapLibre,
        pmtilesUrls
    ]);
    // Update layer visibility based on zoom
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            if (!map.current || !mapLoaded) return;
            try {
                const layerVisibility = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getComputedLayerVisibility"])(zoom);
                // Helper function to safely update layer visibility
                const updateLayerVisibility = {
                    "PMTilesMapInner.useEffect.updateLayerVisibility": (layerId, visible)=>{
                        if (map.current && map.current.getLayer(layerId)) {
                            map.current.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                            console.log(`✅ Updated ${layerId} visibility to ${visible ? 'visible' : 'none'}`);
                        } else {
                            console.log(`⚠️ Layer ${layerId} not found, skipping visibility update`);
                        }
                    }
                }["PMTilesMapInner.useEffect.updateLayerVisibility"];
                // Update Kommune layer visibility
                updateLayerVisibility('kommune-fill', layerVisibility.shouldShowKommune);
                updateLayerVisibility('kommune-stroke', layerVisibility.shouldShowKommune);
                // Update H3 layer visibility
                updateLayerVisibility('h3-fill', layerVisibility.shouldShowH3);
                updateLayerVisibility('h3-stroke', layerVisibility.shouldShowH3);
            // BNBO layers are always visible - no need to update visibility
            // updateLayerVisibility('bnbo-fill', true) // Always visible
            // updateLayerVisibility('bnbo-stroke', true) // Always visible
            } catch (error) {
                console.warn('Error updating layer visibility:', error);
            }
        }
    }["PMTilesMapInner.useEffect"], [
        zoom,
        mapLoaded
    ]);
    // Update basemap visibility when showBasemap changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            if (!map.current || !mapLoaded) return;
            try {
                console.log('🗺️ Updating basemap visibility:', showBasemap);
                // Helper function to safely update layer visibility
                const updateLayerVisibility = {
                    "PMTilesMapInner.useEffect.updateLayerVisibility": (layerId, visible)=>{
                        if (map.current && map.current.getLayer(layerId)) {
                            map.current.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                            console.log(`✅ Updated ${layerId} visibility to ${visible ? 'visible' : 'none'}`);
                        } else {
                            console.log(`⚠️ Layer ${layerId} not found, skipping visibility update`);
                        }
                    }
                }["PMTilesMapInner.useEffect.updateLayerVisibility"];
                // Update all basemap layers
                const basemapLayers = [
                    'basemap-fill',
                    'basemap-water',
                    'basemap-landuse',
                    'basemap-buildings',
                    'basemap-buildings-stroke',
                    'basemap-roads-minor',
                    'basemap-roads-major'
                ];
                basemapLayers.forEach({
                    "PMTilesMapInner.useEffect": (layerId)=>{
                        updateLayerVisibility(layerId, showBasemap);
                    }
                }["PMTilesMapInner.useEffect"]);
            } catch (error) {
                console.warn('Error updating basemap visibility:', error);
            }
        }
    }["PMTilesMapInner.useEffect"], [
        showBasemap,
        mapLoaded
    ]);
    // Update paint expressions when data mode changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            if (!map.current || !mapLoaded) return;
            try {
                console.log('🎨 Updating paint expressions for data mode:', selectedDataMode, 'property:', currentPropertyName);
                // Update Kommune layer paint
                if (map.current.getLayer('kommune-fill')) {
                    const newPaint = [
                        'interpolate',
                        [
                            'linear'
                        ],
                        [
                            'get',
                            currentPropertyName
                        ],
                        0,
                        'transparent',
                        0.1,
                        '#fee5d9',
                        1,
                        '#fcbba1',
                        5,
                        '#fc9272',
                        10,
                        '#fb6a4a',
                        20,
                        '#ef3b2c',
                        50,
                        '#cb181d',
                        100,
                        '#99000d'
                    ];
                    map.current.setPaintProperty('kommune-fill', 'fill-color', newPaint);
                    console.log('✅ Updated kommune-fill paint property');
                } else {
                    console.log('⚠️ kommune-fill layer not found, skipping paint update');
                }
                // Update H3 layer paint
                if (map.current.getLayer('h3-fill')) {
                    const newPaint = [
                        'interpolate',
                        [
                            'linear'
                        ],
                        [
                            'get',
                            currentPropertyName
                        ],
                        0,
                        'transparent',
                        0.1,
                        '#fee5d9',
                        1,
                        '#fcbba1',
                        5,
                        '#fc9272',
                        10,
                        '#fb6a4a',
                        20,
                        '#ef3b2c',
                        50,
                        '#cb181d',
                        100,
                        '#99000d'
                    ];
                    map.current.setPaintProperty('h3-fill', 'fill-color', newPaint);
                    console.log('✅ Updated h3-fill paint property');
                } else {
                    console.log('⚠️ h3-fill layer not found, skipping paint update');
                }
                // Debug: Try to get some feature data to see what properties are available
                setTimeout({
                    "PMTilesMapInner.useEffect": ()=>{
                        try {
                            const features = map.current.queryRenderedFeatures();
                            if (features.length > 0) {
                                const sampleFeature = features[0];
                                console.log('🔍 Sample feature properties:', sampleFeature.properties);
                                console.log('🔍 Current property value:', sampleFeature.properties[currentPropertyName]);
                            }
                        } catch (e) {
                            console.log('🔍 Could not query features for debugging:', e);
                        }
                    }
                }["PMTilesMapInner.useEffect"], 1000);
            } catch (error) {
                console.warn('Error updating paint expressions:', error);
            }
        }
    }["PMTilesMapInner.useEffect"], [
        selectedDataMode,
        currentPropertyName,
        mapLoaded
    ]);
    // Update source-layer names when year or resolution changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            if (!map.current || !mapLoaded) return;
            try {
                console.log('🔄 Updating source-layer names for year:', selectedYear, 'resolution:', currentH3Resolution);
                // Remove existing layers
                const layersToUpdate = [
                    'kommune-fill',
                    'kommune-stroke',
                    'h3-fill',
                    'h3-stroke'
                ];
                layersToUpdate.forEach({
                    "PMTilesMapInner.useEffect": (layerId)=>{
                        if (map.current && map.current.getLayer(layerId)) {
                            map.current.removeLayer(layerId);
                            console.log(`🗑️ Removed layer: ${layerId}`);
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                // Re-add Kommune layers with correct source-layer name (only if source exists)
                // Add before BNBO layers to ensure BNBO stays on top
                if (map.current.getSource('kommune')) {
                    const kommuneSourceLayer = `kommune_pfas_${selectedYear}`;
                    map.current.addLayer({
                        id: 'kommune-fill',
                        type: 'fill',
                        source: 'kommune',
                        'source-layer': kommuneSourceLayer,
                        layout: {
                            visibility: shouldShowKommune ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'get',
                                    currentPropertyName
                                ],
                                0,
                                'transparent',
                                0.1,
                                '#fee5d9',
                                1,
                                '#fcbba1',
                                5,
                                '#fc9272',
                                10,
                                '#fb6a4a',
                                20,
                                '#ef3b2c',
                                50,
                                '#cb181d',
                                100,
                                '#99000d'
                            ],
                            'fill-opacity': 0.8
                        }
                    }, 'bnbo-fill') // Add before BNBO fill layer
                    ;
                    map.current.addLayer({
                        id: 'kommune-stroke',
                        type: 'line',
                        source: 'kommune',
                        'source-layer': kommuneSourceLayer,
                        layout: {
                            visibility: shouldShowKommune ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 0.5,
                            'line-opacity': 0.5
                        }
                    }, 'bnbo-fill') // Add before BNBO fill layer
                    ;
                    console.log('✅ Re-added kommune layers with source-layer:', kommuneSourceLayer);
                } else {
                    console.log('⚠️ Kommune source not found, skipping layer re-addition');
                }
                // Re-add H3 layers with correct source-layer name (only if source exists)
                // Add before BNBO layers to ensure BNBO stays on top
                if (map.current.getSource('h3')) {
                    const h3SourceLayer = `h3_pfas_${selectedYear}_res${currentH3Resolution}`;
                    map.current.addLayer({
                        id: 'h3-fill',
                        type: 'fill',
                        source: 'h3',
                        'source-layer': h3SourceLayer,
                        layout: {
                            visibility: shouldShowH3 ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
                                'interpolate',
                                [
                                    'linear'
                                ],
                                [
                                    'get',
                                    currentPropertyName
                                ],
                                0,
                                'transparent',
                                0.1,
                                '#fee5d9',
                                1,
                                '#fcbba1',
                                5,
                                '#fc9272',
                                10,
                                '#fb6a4a',
                                20,
                                '#ef3b2c',
                                50,
                                '#cb181d',
                                100,
                                '#99000d'
                            ],
                            'fill-opacity': 0.7
                        }
                    }, 'bnbo-fill') // Add before BNBO fill layer
                    ;
                    map.current.addLayer({
                        id: 'h3-stroke',
                        type: 'line',
                        source: 'h3',
                        'source-layer': h3SourceLayer,
                        layout: {
                            visibility: shouldShowH3 ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 0.2,
                            'line-opacity': 0.3
                        }
                    }, 'bnbo-fill') // Add before BNBO fill layer
                    ;
                    console.log('✅ Re-added H3 layers with source-layer:', h3SourceLayer);
                } else {
                    console.log('⚠️ H3 source not found, skipping layer re-addition');
                }
            } catch (error) {
                console.warn('Error updating source-layer names:', error);
            }
        }
    }["PMTilesMapInner.useEffect"], [
        selectedYear,
        currentH3Resolution,
        mapLoaded,
        shouldShowKommune,
        shouldShowH3,
        currentPropertyName
    ]);
    // Show loading state
    if (!mapLibre || isLoading) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative flex items-center justify-center bg-gray-900 ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1097,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm",
                        children: "Loading map..."
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1098,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 1096,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 1095,
            columnNumber: 7
        }, this);
    }
    // Show error state
    if (error) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative flex items-center justify-center bg-gray-900 ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-4xl mb-4",
                        children: "⚠️"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1109,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm mb-2",
                        children: "Map Error"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1110,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-xs",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1111,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: clearError,
                        className: "mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 1112,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 1108,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 1107,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `relative ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                ref: mapContainer,
                className: "w-full h-full"
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 1125,
                columnNumber: 7
            }, this),
            !mapLoaded && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute inset-0 flex items-center justify-center bg-gray-900/80",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-center",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-2"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 1131,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "text-white text-sm",
                            children: "Initializing map..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 1132,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1130,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 1129,
                columnNumber: 9
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 1124,
        columnNumber: 5
    }, this);
};
_s(PMTilesMapInner, "+xSnCskZJI2iNWAmgczg/bfZv30=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapViewState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"]
    ];
});
_c = PMTilesMapInner;
// Error boundary component
const MapErrorFallback = ({ error, resetErrorBoundary })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "flex items-center justify-center h-full bg-gray-900",
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "text-center",
            children: [
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-red-400 text-6xl mb-4",
                    children: "⚠️"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1152,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                    className: "text-white text-xl font-semibold mb-2",
                    children: "Map Error"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1153,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                    className: "text-gray-400 mb-4",
                    children: error.message
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1154,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: resetErrorBoundary,
                    className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                    children: "Reload Map"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1155,
                    columnNumber: 7
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 1151,
            columnNumber: 5
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 1150,
        columnNumber: 3
    }, this);
_c1 = MapErrorFallback;
const PMTilesMap = (props)=>{
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$react$2d$error$2d$boundary$2f$dist$2f$react$2d$error$2d$boundary$2e$development$2e$esm$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["ErrorBoundary"], {
        FallbackComponent: MapErrorFallback,
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(PMTilesMapInner, {
            ...props
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 1169,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 1168,
        columnNumber: 5
    }, this);
};
_c2 = PMTilesMap;
var _c, _c1, _c2;
__turbopack_context__.k.register(_c, "PMTilesMapInner");
__turbopack_context__.k.register(_c1, "MapErrorFallback");
__turbopack_context__.k.register(_c2, "PMTilesMap");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/controls/DataModeSelector.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "DataModeSelector": (()=>DataModeSelector),
    "default": (()=>__TURBOPACK__default__export__)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
const ColorScaleLegend = ({ mode })=>{
    const config = __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["DATA_MODE_CONFIG"][mode];
    // Generate color scale for visualization
    const colorStops = [
        {
            value: 0,
            color: 'rgba(255, 255, 255, 0.8)'
        },
        {
            value: 1,
            color: 'rgba(255, 200, 200, 0.8)'
        },
        {
            value: 5,
            color: 'rgba(255, 150, 150, 0.8)'
        },
        {
            value: 10,
            color: 'rgba(255, 100, 100, 0.8)'
        },
        {
            value: 20,
            color: 'rgba(255, 50, 50, 0.8)'
        },
        {
            value: 50,
            color: 'rgba(255, 0, 0, 0.8)'
        },
        {
            value: 100,
            color: 'rgba(200, 0, 0, 0.8)'
        },
        {
            value: 200,
            color: 'rgba(150, 0, 0, 0.8)'
        },
        {
            value: 500,
            color: 'rgba(100, 0, 0, 0.8)'
        },
        {
            value: 1000,
            color: 'rgba(50, 0, 0, 0.8)'
        }
    ];
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "mt-3 p-3 bg-gray-50 rounded-lg",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-sm font-medium text-gray-900 mb-2",
                children: [
                    config.label,
                    " Scale"
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 30,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "relative h-4 rounded mb-2",
                style: {
                    background: `linear-gradient(to right, ${colorStops.map((stop)=>stop.color).join(', ')})`
                },
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "absolute inset-0 border border-gray-300 rounded"
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                    lineNumber: 38,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 35,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex justify-between text-xs text-gray-600",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "0"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 43,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Low"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 44,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Medium"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 45,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "High"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 46,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: [
                            "1000+ ",
                            config.unit
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 47,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 42,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "mt-2 text-xs text-gray-500",
                children: config.description
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 50,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
        lineNumber: 29,
        columnNumber: 5
    }, this);
};
_c = ColorScaleLegend;
const DataModeSelector = ({ className = '', variant = 'sidebar' })=>{
    _s();
    const { selectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { setSelectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    const modes = [
        {
            key: 'pesticide_total',
            label: 'Total Pesticide',
            shortLabel: 'Total',
            description: 'All pesticide applications combined',
            color: 'text-gray-700'
        },
        {
            key: 'pfas',
            label: 'PFAS',
            shortLabel: 'PFAS',
            description: 'PFAS-containing pesticides only',
            color: 'text-red-600'
        },
        {
            key: 'diquat',
            label: 'Diquat',
            shortLabel: 'Diquat',
            description: 'Diquat-containing pesticides only',
            color: 'text-blue-600'
        },
        {
            key: 'glyphosate',
            label: 'Glyphosate',
            shortLabel: 'Glyphosate',
            description: 'Glyphosate-containing pesticides only',
            color: 'text-green-600'
        }
    ];
    // Minimal top bar version inspired by London Underground Live
    if (variant === 'topbar') {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `flex items-center space-x-1 ${className}`,
            children: modes.map((mode)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: ()=>setSelectedDataMode(mode.key),
                    className: `px-3 py-1.5 text-sm font-medium rounded-md transition-all duration-200 ${selectedDataMode === mode.key ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'}`,
                    children: mode.shortLabel
                }, mode.key, false, {
                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                    lineNumber: 100,
                    columnNumber: 11
                }, this))
        }, void 0, false, {
            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
            lineNumber: 98,
            columnNumber: 7
        }, this);
    }
    // Original sidebar version
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `bg-white rounded-lg shadow-lg border border-gray-200 p-4 ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                className: "text-lg font-semibold text-gray-900 mb-4",
                children: "Data Mode"
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 119,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: modes.map((mode)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>setSelectedDataMode(mode.key),
                        className: `w-full text-left p-3 rounded-lg border transition-all duration-200 ${selectedDataMode === mode.key ? 'border-blue-500 bg-blue-50 shadow-sm' : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'}`,
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center justify-between",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "flex-1",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: `font-medium ${mode.color} ${selectedDataMode === mode.key ? 'text-blue-700' : ''}`,
                                            children: mode.label
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                            lineNumber: 134,
                                            columnNumber: 17
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "text-sm text-gray-600 mt-1",
                                            children: mode.description
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                            lineNumber: 139,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                    lineNumber: 133,
                                    columnNumber: 15
                                }, this),
                                selectedDataMode === mode.key && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "ml-3",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "w-2 h-2 bg-blue-500 rounded-full"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                        lineNumber: 146,
                                        columnNumber: 19
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                    lineNumber: 145,
                                    columnNumber: 17
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 132,
                            columnNumber: 13
                        }, this)
                    }, mode.key, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 123,
                        columnNumber: 11
                    }, this))
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 121,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(ColorScaleLegend, {
                mode: selectedDataMode
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 155,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-sm text-yellow-800",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "font-medium mb-1",
                            children: "Note:"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 160,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: "Data visualization switches automatically between Kommune (low zoom) and H3 cell (high zoom) layers. Zoom in for detailed field-level analysis."
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 161,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                    lineNumber: 159,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 158,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
        lineNumber: 118,
        columnNumber: 5
    }, this);
};
_s(DataModeSelector, "xBMq78RmiF31eyAX5NUYB4Avjvk=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"]
    ];
});
_c1 = DataModeSelector;
const __TURBOPACK__default__export__ = DataModeSelector;
var _c, _c1;
__turbopack_context__.k.register(_c, "ColorScaleLegend");
__turbopack_context__.k.register(_c1, "DataModeSelector");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/controls/StepSlider.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "StepSlider": (()=>StepSlider)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-left.js [app-client] (ecmascript) <export default as ChevronLeft>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-right.js [app-client] (ecmascript) <export default as ChevronRight>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/play.js [app-client] (ecmascript) <export default as Play>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/pause.js [app-client] (ecmascript) <export default as Pause>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
;
function StepSlider({ className = '' }) {
    _s();
    const selectedYear = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useSelectedYear"])();
    const availableYearOptions = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useAvailableYearOptions"])();
    const { setSelectedYear } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    const [isAnimating, setIsAnimating] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [animationInterval, setAnimationInterval] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    // Get numeric years for animation and display
    const numericYears = availableYearOptions.filter((year)=>typeof year === 'number').sort((a, b)=>a - b);
    const hasTotal = availableYearOptions.includes('total');
    // All options in order: years + total
    const allOptions = [
        ...numericYears,
        ...hasTotal ? [
            'total'
        ] : []
    ];
    const currentIndex = allOptions.indexOf(selectedYear);
    const startAnimation = ()=>{
        if (numericYears.length <= 1) return;
        setIsAnimating(true);
        const interval = setInterval(()=>{
            const currentIndex = numericYears.indexOf(selectedYear);
            const nextIndex = currentIndex >= 0 ? (currentIndex + 1) % numericYears.length : 0;
            setSelectedYear(numericYears[nextIndex]);
        }, 1500);
        setAnimationInterval(interval);
    };
    const stopAnimation = ()=>{
        setIsAnimating(false);
        if (animationInterval) {
            clearInterval(animationInterval);
            setAnimationInterval(null);
        }
    };
    const goToNext = ()=>{
        const currentIdx = allOptions.indexOf(selectedYear);
        if (currentIdx < allOptions.length - 1) {
            setSelectedYear(allOptions[currentIdx + 1]);
        }
    };
    const goToPrevious = ()=>{
        const currentIdx = allOptions.indexOf(selectedYear);
        if (currentIdx > 0) {
            setSelectedYear(allOptions[currentIdx - 1]);
        }
    };
    const canGoNext = currentIndex < allOptions.length - 1;
    const canGoPrevious = currentIndex > 0;
    // Cleanup animation on unmount
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "StepSlider.useEffect": ()=>{
            return ({
                "StepSlider.useEffect": ()=>{
                    if (animationInterval) {
                        clearInterval(animationInterval);
                    }
                }
            })["StepSlider.useEffect"];
        }
    }["StepSlider.useEffect"], [
        animationInterval
    ]);
    if (availableYearOptions.length === 0) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `${className} flex items-center justify-center`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-gray-400 text-sm",
                children: "Loading..."
            }, void 0, false, {
                fileName: "[project]/src/components/controls/StepSlider.tsx",
                lineNumber: 78,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/controls/StepSlider.tsx",
            lineNumber: 77,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `${className} flex items-center space-x-4`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                onClick: goToPrevious,
                disabled: !canGoPrevious || isAnimating,
                className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__["ChevronLeft"], {
                    className: "w-4 h-4"
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/StepSlider.tsx",
                    lineNumber: 91,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/StepSlider.tsx",
                lineNumber: 86,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                onClick: isAnimating ? stopAnimation : startAnimation,
                disabled: numericYears.length <= 1,
                className: "p-2 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                children: isAnimating ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__["Pause"], {
                    className: "w-4 h-4"
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/StepSlider.tsx",
                    lineNumber: 100,
                    columnNumber: 24
                }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__["Play"], {
                    className: "w-4 h-4"
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/StepSlider.tsx",
                    lineNumber: 100,
                    columnNumber: 56
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/StepSlider.tsx",
                lineNumber: 95,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex items-center space-x-2",
                children: [
                    numericYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                            onClick: ()=>setSelectedYear(year),
                            disabled: isAnimating,
                            className: `px-3 py-1.5 rounded-lg text-sm font-medium transition-all min-w-[60px] ${selectedYear === year ? 'bg-blue-600 text-white shadow-lg scale-105' : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'} disabled:opacity-30 disabled:cursor-not-allowed`,
                            children: year
                        }, year, false, {
                            fileName: "[project]/src/components/controls/StepSlider.tsx",
                            lineNumber: 107,
                            columnNumber: 11
                        }, this)),
                    hasTotal && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>setSelectedYear('total'),
                        disabled: isAnimating,
                        className: `px-4 py-1.5 rounded-lg text-sm font-medium transition-all min-w-[80px] ${selectedYear === 'total' ? 'bg-gradient-to-r from-purple-600 to-blue-600 text-white shadow-lg scale-105' : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'} disabled:opacity-30 disabled:cursor-not-allowed`,
                        children: "Total"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/StepSlider.tsx",
                        lineNumber: 123,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/StepSlider.tsx",
                lineNumber: 104,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                onClick: goToNext,
                disabled: !canGoNext || isAnimating,
                className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__["ChevronRight"], {
                    className: "w-4 h-4"
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/StepSlider.tsx",
                    lineNumber: 143,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/StepSlider.tsx",
                lineNumber: 138,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/StepSlider.tsx",
        lineNumber: 84,
        columnNumber: 5
    }, this);
}
_s(StepSlider, "KoqN8grNZZizvbNbUkRAW21O+NE=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useSelectedYear"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useAvailableYearOptions"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"]
    ];
});
_c = StepSlider;
var _c;
__turbopack_context__.k.register(_c, "StepSlider");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/overlays/DataSidebar.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "DataSidebar": (()=>DataSidebar)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$x$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__X$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/x.js [app-client] (ecmascript) <export default as X>");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
function DataSidebar({ hoverInfo, onClose, isVisible = false }) {
    _s();
    // Format functions
    const formatNumber = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useCallback"])({
        "DataSidebar.useCallback[formatNumber]": (value, decimals = 2)=>{
            if (value === undefined || value === null) return '0';
            if (value === 0) return '0';
            if (value < 0.01 && value > 0) return '<0.01';
            return value.toLocaleString(undefined, {
                minimumFractionDigits: 0,
                maximumFractionDigits: decimals
            });
        }
    }["DataSidebar.useCallback[formatNumber]"], []);
    // Render sidebar content based on layer type
    const renderSidebarContent = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMemo"])({
        "DataSidebar.useMemo[renderSidebarContent]": ()=>{
            if (!hoverInfo) return null;
            switch(hoverInfo.layer){
                case 'h3':
                    const pfasGrams = hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams || 0;
                    const pesticideLoad = hoverInfo.data.pesticide_load || hoverInfo.data.total_pesticide_load || 0;
                    const diquatGrams = hoverInfo.data.diquat_grams || 0;
                    const glyphosateGrams = hoverInfo.data.glyphosate_grams || 0;
                    const area = hoverInfo.data.agricultural_area_ha || hoverInfo.data.h3_cell_area_ha || 0;
                    // Calculate intensities
                    const pfasIntensity = hoverInfo.data.pfas_intensity || (area > 0 ? pfasGrams / area : 0);
                    const pesticideIntensity = hoverInfo.data.pesticide_intensity || (area > 0 ? pesticideLoad / area : 0);
                    const diquatIntensity = hoverInfo.data.diquat_intensity || (area > 0 ? diquatGrams / area : 0);
                    const glyphosateIntensity = hoverInfo.data.glyphosate_intensity || (area > 0 ? glyphosateGrams / area : 0);
                    const applicationCount = hoverInfo.data.application_count || 0;
                    const fieldCount = hoverInfo.data.field_count || 0;
                    const coveragePercent = hoverInfo.data.coverage_percent || 0;
                    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-4",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-700 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                        className: "text-lg font-semibold mb-2 text-white",
                                        children: "Agricultural Area"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 58,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-slate-300 text-sm",
                                        children: area > 0 ? `${formatNumber(area, 1)} hectares` : 'Area data unavailable'
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 59,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 57,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border-l-4 border-orange-400",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center justify-between mb-3",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-orange-300 font-semibold",
                                            children: "Total Pesticide Load"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                            lineNumber: 67,
                                            columnNumber: 17
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 66,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "grid grid-cols-2 gap-4",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-orange-200",
                                                        children: formatNumber(pesticideLoad, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 71,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-orange-400",
                                                        children: "kg total"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 72,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 70,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-orange-200",
                                                        children: formatNumber(pesticideIntensity, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 75,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-orange-400",
                                                        children: "kg per hectare"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 76,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 74,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 69,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 65,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border-l-4 border-red-400",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center justify-between mb-3",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                className: "text-red-300 font-semibold",
                                                children: "PFAS Active Ingredients"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 84,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-xs bg-red-900/50 text-red-300 px-2 py-1 rounded-full font-medium border border-red-700",
                                                children: "Persistent"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 85,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 83,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "grid grid-cols-2 gap-4",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-red-200",
                                                        children: formatNumber(pfasGrams, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 91,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-red-400",
                                                        children: "grams total"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 92,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 90,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-red-200",
                                                        children: formatNumber(pfasIntensity, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 95,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-red-400",
                                                        children: "grams per hectare"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 96,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 94,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 89,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 82,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border-l-4 border-green-400",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center justify-between mb-3",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-green-300 font-semibold",
                                            children: "Glyphosate Active Ingredients"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                            lineNumber: 104,
                                            columnNumber: 17
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 103,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "grid grid-cols-2 gap-4",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-green-200",
                                                        children: formatNumber(glyphosateGrams, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 108,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-green-400",
                                                        children: "grams total"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 109,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 107,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-green-200",
                                                        children: formatNumber(glyphosateIntensity, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 112,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-green-400",
                                                        children: "grams per hectare"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 113,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 111,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 106,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 102,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border-l-4 border-amber-400",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center justify-between mb-3",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-amber-300 font-semibold",
                                            children: "Diquat Active Ingredients"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                            lineNumber: 121,
                                            columnNumber: 17
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 120,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "grid grid-cols-2 gap-4",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-amber-200",
                                                        children: formatNumber(diquatGrams, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 125,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-amber-400",
                                                        children: "grams total"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 126,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 124,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-center",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-2xl font-bold text-amber-200",
                                                        children: formatNumber(diquatIntensity, 2)
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 129,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-sm text-amber-400",
                                                        children: "grams per hectare"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 130,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 128,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 123,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 119,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                        className: "text-slate-200 font-semibold mb-3",
                                        children: "Activity"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 137,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "grid grid-cols-3 gap-4 text-center",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xl font-bold text-slate-100",
                                                        children: applicationCount
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 140,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xs text-slate-400",
                                                        children: "Applications"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 141,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 139,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xl font-bold text-slate-100",
                                                        children: fieldCount
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 144,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xs text-slate-400",
                                                        children: "Fields"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 145,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 143,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xl font-bold text-slate-100",
                                                        children: [
                                                            formatNumber(coveragePercent, 0),
                                                            "%"
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 148,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "text-xs text-slate-400",
                                                        children: "Coverage"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 149,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 147,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 138,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 136,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                        lineNumber: 55,
                        columnNumber: 11
                    }, this);
                case 'bnbo':
                    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-4",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-700 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                        className: "text-lg font-semibold mb-2 text-white",
                                        children: "BNBO Protected Area"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 160,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-slate-300 text-sm",
                                        children: hoverInfo.data.status || 'Status unknown'
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 161,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 159,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                        className: "text-slate-200 font-semibold mb-3",
                                        children: "Area Details"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 167,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "space-y-2 text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-slate-400",
                                                        children: "Area:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 170,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-slate-200",
                                                        children: [
                                                            formatNumber(hoverInfo.data.area_ha, 2),
                                                            " ha"
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 171,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 169,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-slate-400",
                                                        children: "Protection Level:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 174,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-slate-200",
                                                        children: hoverInfo.data.protection_level || 'Unknown'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 175,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 173,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 168,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 166,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                        lineNumber: 158,
                        columnNumber: 11
                    }, this);
                case 'bbr':
                    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-4",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-700 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                        className: "text-lg font-semibold mb-2 text-white",
                                        children: "Building"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 186,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-slate-300 text-sm",
                                        children: hoverInfo.data.building_type || 'Type unknown'
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 187,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 185,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "bg-slate-800 rounded-lg p-4 border border-slate-600",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                        className: "text-slate-200 font-semibold mb-3",
                                        children: "Building Details"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 193,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "space-y-2 text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-slate-400",
                                                        children: "Type:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 196,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-slate-200",
                                                        children: hoverInfo.data.building_type || 'Unknown'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 197,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 195,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-slate-400",
                                                        children: "Use:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 200,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-slate-200",
                                                        children: hoverInfo.data.building_use || 'Unknown'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 201,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 199,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-slate-400",
                                                        children: "Year:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 204,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-slate-200",
                                                        children: hoverInfo.data.construction_year || 'Unknown'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                        lineNumber: 205,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                                lineNumber: 203,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                        lineNumber: 194,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                                lineNumber: 192,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                        lineNumber: 184,
                        columnNumber: 11
                    }, this);
                default:
                    return null;
            }
        }
    }["DataSidebar.useMemo[renderSidebarContent]"], [
        hoverInfo,
        formatNumber
    ]);
    if (!isVisible || !hoverInfo) return null;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "fixed top-16 right-0 h-[calc(100vh-4rem)] w-96 bg-slate-900/95 backdrop-blur-sm border-l border-slate-700 shadow-2xl z-40 transform transition-transform duration-300 ease-in-out",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-slate-800 border-b border-slate-700 p-4 flex items-center justify-between",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                        className: "text-lg font-semibold text-white",
                        children: "Area Details"
                    }, void 0, false, {
                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                        lineNumber: 223,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: onClose,
                        className: "p-1 hover:bg-slate-700 rounded-full transition-colors",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$x$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__X$3e$__["X"], {
                            className: "w-5 h-5 text-slate-400 hover:text-white"
                        }, void 0, false, {
                            fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                            lineNumber: 228,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                        lineNumber: 224,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                lineNumber: 222,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "p-4 overflow-y-auto h-full pb-20",
                children: renderSidebarContent
            }, void 0, false, {
                fileName: "[project]/src/components/overlays/DataSidebar.tsx",
                lineNumber: 233,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/overlays/DataSidebar.tsx",
        lineNumber: 220,
        columnNumber: 5
    }, this);
}
_s(DataSidebar, "SGsDsI6J8pTxXmkebBVCwGn8X/8=");
_c = DataSidebar;
var _c;
__turbopack_context__.k.register(_c, "DataSidebar");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/app/page.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "default": (()=>Home)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/PMTilesMap.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/DataModeSelector.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$StepSlider$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/StepSlider.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$overlays$2f$DataSidebar$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/overlays/DataSidebar.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/settings.js [app-client] (ecmascript) <export default as Settings>");
(()=>{
    const e = new Error("Cannot find module '@/components/controls/BasemapToggle'");
    e.code = 'MODULE_NOT_FOUND';
    throw e;
})();
(()=>{
    const e = new Error("Cannot find module '@/components/controls/BNBOToggle'");
    e.code = 'MODULE_NOT_FOUND';
    throw e;
})();
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
;
;
;
;
;
;
;
;
function Home() {
    _s();
    const [isInitialized, setIsInitialized] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [isInitializing, setIsInitializing] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(true);
    const [showControls, setShowControls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false); // Start with controls hidden like London Underground
    const [showSidebar, setShowSidebar] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    // Store state
    const { selectedYear, selectedDataMode, availableYearOptions } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"])();
    const { showTooltip, tooltipData, tooltipPosition } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTooltipState"])();
    // Store actions
    const { setAvailableYearOptions, setError: mapSetError, clearError: mapClearError } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Initialize the application
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Home.useEffect": ()=>{
            const initialize = {
                "Home.useEffect.initialize": async ()=>{
                    try {
                        console.log('🚀 Starting initialization...');
                        setIsInitializing(true);
                        console.log('📡 Getting data availability...');
                        const availability = await __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getDataAvailability();
                        console.log('✅ Data availability:', availability);
                        // Create year options including 'total' option
                        const yearOptions = [
                            ...availability.years,
                            'total'
                        ];
                        setAvailableYearOptions(yearOptions);
                        setIsInitialized(true);
                        mapClearError();
                        console.log('✅ Initialization complete');
                    } catch (err) {
                        console.error('❌ Error initializing application:', err);
                        mapSetError('Failed to initialize application');
                    } finally{
                        console.log('🏁 Setting loading to false');
                        setIsInitializing(false);
                    }
                }
            }["Home.useEffect.initialize"];
            initialize();
        }
    }["Home.useEffect"], [
        setAvailableYearOptions,
        mapSetError,
        mapClearError
    ]);
    // Convert tooltip data to HoverInfo format for sidebar
    const convertToHoverInfo = (tooltipData, position)=>{
        if (!tooltipData) return null;
        // Determine layer type based on data
        let layer;
        if (tooltipData.bnbo_id || tooltipData.status) {
            layer = 'bnbo';
        } else if (tooltipData.kommune_code || tooltipData.kommune_name) {
            layer = 'h3'; // Kommune data is shown as h3 for now
        } else {
            layer = 'h3';
        }
        return {
            layer,
            data: tooltipData,
            coordinate: [
                0,
                0
            ],
            pixel: [
                position.x,
                position.y
            ]
        };
    };
    // Handle tooltip changes to show/hide sidebar
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Home.useEffect": ()=>{
            if (showTooltip && tooltipData) {
                setShowSidebar(true);
            } else {
                setShowSidebar(false);
            }
        }
    }["Home.useEffect"], [
        showTooltip,
        tooltipData
    ]);
    const handleCloseSidebar = ()=>{
        setShowSidebar(false);
    };
    // Get current hover info for sidebar
    const hoverInfo = convertToHoverInfo(tooltipData, tooltipPosition);
    // Loading state
    if (isInitializing || !isInitialized) {
        console.log('🔄 Still loading - isInitializing:', isInitializing, 'isInitialized:', isInitialized);
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-gray-900 flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 114,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-lg font-medium",
                        children: "Loading PMTiles Map..."
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 115,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-sm mt-2",
                        children: "Discovering latest data from GCS bucket"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 116,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 113,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 112,
            columnNumber: 7
        }, this);
    }
    // Error state
    if (error) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-gray-900 flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center max-w-md mx-auto p-6",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-6xl mb-4",
                        children: "⚠️"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 127,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                        className: "text-white text-xl font-semibold mb-2",
                        children: "Something went wrong"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 128,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 mb-4",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 129,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                        children: "Reload Application"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 130,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 126,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 125,
            columnNumber: 7
        }, this);
    }
    // Get year count for display
    const yearCount = availableYearOptions.filter((year)=>typeof year === 'number').length;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-gray-900 text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-3 z-50",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "flex items-center justify-between",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center space-x-4",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                        className: "text-lg font-bold text-white",
                                        children: "Danish Agricultural Pesticide Analysis"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 152,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                        className: "text-xs text-gray-400",
                                        children: [
                                            "PMTiles visualization • ",
                                            yearCount,
                                            " years of data + cumulative"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 153,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 151,
                                columnNumber: 13
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 150,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center space-x-6",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["DataModeSelector"], {
                                    variant: "topbar"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 159,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "h-6 w-px bg-gray-600"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 160,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$StepSlider$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["StepSlider"], {}, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 161,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 158,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center space-x-4",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: ()=>setShowControls(!showControls),
                                className: `p-2 rounded-lg transition-all ${showControls ? 'bg-blue-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-gray-300'}`,
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__["Settings"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 174,
                                    columnNumber: 15
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 166,
                                columnNumber: 13
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 165,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/app/page.tsx",
                    lineNumber: 148,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 147,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1 flex relative",
                children: [
                    showControls && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-80 bg-gray-800/95 backdrop-blur-sm border-r border-gray-700 overflow-y-auto",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "p-4 space-y-6",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                    className: "text-lg font-semibold text-white mb-4",
                                    children: "Advanced Controls"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 186,
                                    columnNumber: 15
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-sm font-medium text-gray-300 mb-2",
                                            children: "Data Mode"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 190,
                                            columnNumber: 17
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["DataModeSelector"], {
                                            variant: "sidebar"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 191,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 189,
                                    columnNumber: 15
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-sm font-medium text-gray-300 mb-3",
                                            children: "Layer Visibility"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 196,
                                            columnNumber: 17
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-3",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(BasemapToggle, {}, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 199,
                                                    columnNumber: 19
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(BNBOToggle, {}, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 202,
                                                    columnNumber: 19
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 197,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 195,
                                    columnNumber: 15
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 185,
                            columnNumber: 13
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 184,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1 relative",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["PMTilesMap"], {
                            className: "w-full h-full"
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 211,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 210,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$overlays$2f$DataSidebar$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["DataSidebar"], {
                        hoverInfo: hoverInfo,
                        onClose: handleCloseSidebar,
                        isVisible: showSidebar
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 215,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 181,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/app/page.tsx",
        lineNumber: 145,
        columnNumber: 5
    }, this);
}
_s(Home, "MnwBhRxv3NmksoqYDGodfT+/DbQ=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTooltipState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"]
    ];
});
_c = Home;
var _c;
__turbopack_context__.k.register(_c, "Home");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
}]);

//# sourceMappingURL=src_a86fcee9._.js.map