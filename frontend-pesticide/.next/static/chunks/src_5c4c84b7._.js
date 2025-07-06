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
// Zoom thresholds for layer switching
const KOMMUNE_MAX_ZOOM = 8;
const H3_MIN_ZOOM = 9;
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
"[project]/src/components/map/MapTooltip.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "MapTooltip": (()=>MapTooltip),
    "default": (()=>__TURBOPACK__default__export__)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$build$2f$polyfills$2f$process$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/build/polyfills/process.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
const formatNumber = (value, decimals = 2)=>{
    if (value === undefined || value === null) return 'N/A';
    if (value === 0) return '0';
    if (value < 0.01 && value > 0) {
        return value.toExponential(2);
    }
    return value.toLocaleString('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: decimals
    });
};
const formatScientific = (value, unit)=>{
    if (value === undefined || value === null) return 'N/A';
    if (value === 0) return `0 ${unit}`;
    if (value < 0.01 && value > 0) {
        return `${value.toExponential(2)} ${unit}`;
    }
    return `${value.toLocaleString('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
    })} ${unit}`;
};
const formatPercentage = (value)=>{
    if (value === undefined || value === null) return 'N/A';
    return `${formatNumber(value * 100, 1)}%`;
};
function getTooltipType(data) {
    if (data.bnbo_id || data.status) return 'bnbo';
    if (data.kommune_code || data.kommune_name) return 'kommune';
    return 'h3';
}
const H3Tooltip = ({ data })=>{
    const h3Id = data.h3_id || data.h3_cell;
    const year = data.year || 2023;
    const resolution = data.resolution || data.h3_resolution || 7;
    const pfasGrams = data.pfas_grams || data.total_pfas_containing_active_ingredient_grams || 0;
    const pesticideLoad = data.pesticide_load || data.total_pesticide_belastning || 0;
    const diquatGrams = data.diquat_grams || data.total_diquat_containing_active_ingredient_grams || 0;
    const glyphosateGrams = data.glyphosate_grams || data.total_glyphosate_containing_active_ingredient_grams || 0;
    const applications = data.applications || data.total_pesticide_applications || 0;
    const fieldCount = data.unique_field_count || data.field_count || 0;
    const area = data.h3_cell_area_ha || data.agricultural_area_ha || 0;
    const coverage = data.actual_coverage_ratio || data.avg_field_coverage || 0;
    // Calculate intensities
    const pfasIntensity = data.pfas_intensity || data.pfas_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? pfasGrams / area : 0);
    const pesticideIntensity = data.pesticide_intensity || data.pesticide_belastning_per_ha || (area > 0 ? pesticideLoad / area : 0);
    const diquatIntensity = data.diquat_intensity || data.diquat_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? diquatGrams / area : 0);
    const glyphosateIntensity = data.glyphosate_intensity || data.glyphosate_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? glyphosateGrams / area : 0);
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 150,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "relative",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center justify-between",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                            className: "font-mono text-sm font-semibold tracking-wide",
                                            children: "AGRICULTURAL ANALYSIS CELL"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 154,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                            className: "font-mono text-xs text-gray-300",
                                            children: [
                                                "H3 RES-",
                                                resolution,
                                                " • YEAR ",
                                                year,
                                                " • SECTOR ",
                                                h3Id ? h3Id.substring(0, 8).toUpperCase() : 'UNKNOWN'
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 155,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 153,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "text-right",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-xs text-gray-400",
                                            children: "AREA"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 160,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-sm font-bold text-white",
                                            children: area > 0 ? `${formatNumber(area, 1)} ha` : 'N/A'
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 161,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 159,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 152,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 151,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 149,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "border border-red-300 bg-red-50 rounded p-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center justify-between mb-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center space-x-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "w-2 h-2 bg-red-600 rounded-full"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 174,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-mono text-xs font-semibold text-red-800 tracking-wide",
                                                children: "PFAS CONTAMINATION"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 175,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 173,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-red-600 font-mono text-xs",
                                        children: "⚠ PERSISTENT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 177,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 172,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "grid grid-cols-2 gap-3 text-xs",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-900 font-bold text-lg",
                                                children: formatNumber(pfasGrams, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 181,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-600",
                                                children: "g total mass"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 182,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 180,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-900 font-bold text-lg",
                                                children: formatNumber(pfasIntensity, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 185,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-600",
                                                children: "g/ha intensity"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 186,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 184,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 179,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 171,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "border border-orange-300 bg-orange-50 rounded p-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center justify-between mb-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center space-x-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "w-2 h-2 bg-orange-600 rounded-full"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 194,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-mono text-xs font-semibold text-orange-800 tracking-wide",
                                                children: "PESTICIDE LOAD"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 195,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 193,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-orange-600 font-mono text-xs",
                                        children: "⚠ ACTIVE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 197,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 192,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "grid grid-cols-2 gap-3 text-xs",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-900 font-bold text-lg",
                                                children: formatNumber(pesticideLoad, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 201,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-600",
                                                children: "kg total load"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 202,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 200,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-900 font-bold text-lg",
                                                children: formatNumber(pesticideIntensity, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 205,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-600",
                                                children: "kg/ha intensity"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 206,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 204,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 199,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 191,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 170,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border border-gray-300 bg-gray-50 rounded p-3",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide",
                        children: "CHEMICAL COMPOSITION ANALYSIS"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 214,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-2 gap-3 text-xs",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "border-l-2 border-green-500 pl-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "GLYPHOSATE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 217,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900",
                                        children: formatScientific(glyphosateGrams, 'g')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 218,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-500",
                                        children: formatScientific(glyphosateIntensity, 'g/ha')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 219,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 216,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "border-l-2 border-yellow-500 pl-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "DIQUAT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 222,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900",
                                        children: formatScientific(diquatGrams, 'g')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 223,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-500",
                                        children: formatScientific(diquatIntensity, 'g/ha')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 224,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 221,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 215,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 213,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border border-gray-300 bg-gray-50 rounded p-3",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide",
                        children: "AGRICULTURAL ACTIVITY"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 231,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-3 gap-3 text-xs",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: applications
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 234,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "APPLICATIONS"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 235,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 233,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: fieldCount
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 238,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "FIELD COUNT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 239,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 237,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: formatPercentage(coverage)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 242,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "COVERAGE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 243,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 241,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 232,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 230,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-300 pt-2",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-mono text-xs text-gray-500",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "font-semibold",
                            children: "CELL ID:"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 251,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono",
                            children: h3Id ? h3Id.substring(0, 16) + '...' : 'UNKNOWN'
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 252,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 250,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 249,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 147,
        columnNumber: 5
    }, this);
};
_c = H3Tooltip;
const KommuneTooltip = ({ data })=>{
    const year = data.year || 2023;
    const pfasGrams = data.pfas_grams || data.total_pfas_containing_active_ingredient_grams || 0;
    const pesticideLoad = data.pesticide_load || data.total_pesticide_belastning || 0;
    const diquatGrams = data.diquat_grams || data.total_diquat_containing_active_ingredient_grams || 0;
    const glyphosateGrams = data.glyphosate_grams || data.total_glyphosate_containing_active_ingredient_grams || 0;
    const applications = data.applications || data.total_pesticide_applications || 0;
    const fieldCount = data.field_count || data.unique_field_count || 0;
    const area = data.agricultural_area_ha || data.total_agricultural_area_ha || 0;
    const coverage = data.agricultural_coverage_pct ? data.agricultural_coverage_pct / 100 : 0;
    // Calculate intensities
    const pfasIntensity = data.pfas_pesticide_belastning_per_ha || (area > 0 ? pfasGrams / area : 0);
    const pesticideIntensity = data.pesticide_belastning_per_ha || (area > 0 ? pesticideLoad / area : 0);
    const diquatIntensity = data.diquat_pesticide_belastning_per_ha || (area > 0 ? diquatGrams / area : 0);
    const glyphosateIntensity = data.glyphosate_pesticide_belastning_per_ha || (area > 0 ? glyphosateGrams / area : 0);
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 283,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "relative",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center justify-between",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                            className: "font-mono text-sm font-semibold tracking-wide",
                                            children: "MUNICIPAL ANALYSIS ZONE"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 287,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                            className: "font-mono text-xs text-gray-300",
                                            children: [
                                                data.kommune_name ? data.kommune_name.toUpperCase() : 'KOMMUNE',
                                                " • CODE ",
                                                data.kommune_code || 'N/A',
                                                " • YEAR ",
                                                year
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 288,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 286,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "text-right",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-xs text-gray-400",
                                            children: "AGRI AREA"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 293,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-sm font-bold text-white",
                                            children: area > 0 ? `${formatNumber(area, 1)} ha` : 'N/A'
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 294,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 292,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 285,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 284,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 282,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "border border-red-300 bg-red-50 rounded p-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center justify-between mb-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center space-x-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "w-2 h-2 bg-red-600 rounded-full"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 307,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-mono text-xs font-semibold text-red-800 tracking-wide",
                                                children: "PFAS CONTAMINATION"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 308,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 306,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-red-600 font-mono text-xs",
                                        children: "⚠ PERSISTENT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 310,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 305,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "grid grid-cols-2 gap-3 text-xs",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-900 font-bold text-lg",
                                                children: formatNumber(pfasGrams, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 314,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-600",
                                                children: "g total mass"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 315,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 313,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-900 font-bold text-lg",
                                                children: formatNumber(pfasIntensity, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 318,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-red-600",
                                                children: "g/ha intensity"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 319,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 317,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 312,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 304,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "border border-orange-300 bg-orange-50 rounded p-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center justify-between mb-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex items-center space-x-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "w-2 h-2 bg-orange-600 rounded-full"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 327,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-mono text-xs font-semibold text-orange-800 tracking-wide",
                                                children: "PESTICIDE LOAD"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 328,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 326,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-orange-600 font-mono text-xs",
                                        children: "⚠ ACTIVE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 330,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 325,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "grid grid-cols-2 gap-3 text-xs",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-900 font-bold text-lg",
                                                children: formatNumber(pesticideLoad, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 334,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-600",
                                                children: "kg total load"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 335,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 333,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-900 font-bold text-lg",
                                                children: formatNumber(pesticideIntensity, 2)
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 338,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-mono text-orange-600",
                                                children: "kg/ha intensity"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                                lineNumber: 339,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 337,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 332,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 324,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 303,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border border-gray-300 bg-gray-50 rounded p-3",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide",
                        children: "CHEMICAL COMPOSITION ANALYSIS"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 347,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-2 gap-3 text-xs",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "border-l-2 border-green-500 pl-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "GLYPHOSATE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 350,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900",
                                        children: formatScientific(glyphosateGrams, 'g')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 351,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-500",
                                        children: formatScientific(glyphosateIntensity, 'g/ha')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 352,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 349,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "border-l-2 border-yellow-500 pl-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "DIQUAT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 355,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900",
                                        children: formatScientific(diquatGrams, 'g')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 356,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-500",
                                        children: formatScientific(diquatIntensity, 'g/ha')
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 357,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 354,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 348,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 346,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border border-gray-300 bg-gray-50 rounded p-3",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide",
                        children: "AGRICULTURAL ACTIVITY"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 364,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-3 gap-3 text-xs",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: applications
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 367,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "APPLICATIONS"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 368,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 366,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: fieldCount
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 371,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "FIELD COUNT"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 372,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 370,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-center",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono font-bold text-gray-900 text-lg",
                                        children: formatPercentage(coverage)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 375,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-gray-600",
                                        children: "COVERAGE"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 376,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 374,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 365,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 363,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-300 pt-2",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-mono text-xs text-gray-500",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "font-semibold",
                            children: "KOMMUNE CODE:"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 384,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono",
                            children: data.kommune_code || 'UNKNOWN'
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 385,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 383,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 382,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 280,
        columnNumber: 5
    }, this);
};
_c1 = KommuneTooltip;
const BNBOTooltip = ({ data })=>{
    const statusConfig = data.status ? __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["BNBO_STATUS_CONFIG"][data.status] : null;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 401,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "relative",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center justify-between",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                            className: "font-mono text-sm font-semibold tracking-wide",
                                            children: "ENVIRONMENTAL PROTECTION ZONE"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 405,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                            className: "font-mono text-xs text-gray-300",
                                            children: [
                                                "BNBO SECTOR ",
                                                data.bnbo_id ? data.bnbo_id.substring(0, 8).toUpperCase() : 'UNKNOWN'
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 406,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 404,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "text-right",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-xs text-gray-400",
                                            children: "AREA"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 411,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "font-mono text-sm font-bold text-white",
                                            children: data.area_ha ? `${formatNumber(data.area_ha, 1)} ha` : 'N/A'
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                                            lineNumber: 412,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                                    lineNumber: 410,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 403,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 402,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 400,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border border-gray-300 bg-gray-50 rounded p-3",
                children: [
                    statusConfig && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center space-x-3 mb-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "w-4 h-4 rounded border border-gray-400",
                                style: {
                                    backgroundColor: statusConfig.color
                                }
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 424,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-sm font-semibold text-gray-900",
                                        children: statusConfig.label
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 429,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-mono text-xs text-gray-600",
                                        children: statusConfig.description
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 430,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 428,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 423,
                        columnNumber: 11
                    }, this),
                    data.description && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-mono text-xs font-semibold text-gray-900 mb-1",
                                children: "DESCRIPTION:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 437,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                className: "font-mono text-xs text-gray-600 break-words",
                                children: data.description
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 438,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 436,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 421,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-300 pt-2",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-mono text-xs text-gray-500",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "font-semibold",
                            children: "BNBO ID:"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 446,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono",
                            children: data.bnbo_id || 'UNKNOWN'
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 447,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 445,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 444,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 398,
        columnNumber: 5
    }, this);
};
_c2 = BNBOTooltip;
const MapTooltip = ()=>{
    _s();
    const { showTooltip, tooltipData, tooltipPosition } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTooltipState"])();
    if (!showTooltip || !tooltipData) {
        return null;
    }
    const tooltipType = getTooltipType(tooltipData);
    // Position tooltip to avoid going off screen
    const adjustedPosition = {
        left: Math.min(tooltipPosition.x + 10, window.innerWidth - 320),
        top: Math.min(tooltipPosition.y + 10, window.innerHeight - 400)
    };
    // If tooltip would go off the right edge, position it to the left of cursor
    if (tooltipPosition.x + 320 > window.innerWidth) {
        adjustedPosition.left = tooltipPosition.x - 320 - 10;
    }
    // If tooltip would go off the bottom edge, position it above cursor
    if (tooltipPosition.y + 400 > window.innerHeight) {
        adjustedPosition.top = tooltipPosition.y - 400 - 10;
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "fixed z-50 pointer-events-none",
        style: {
            left: adjustedPosition.left,
            top: adjustedPosition.top
        },
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "p-4",
                children: [
                    tooltipType === 'h3' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(H3Tooltip, {
                        data: tooltipData
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 491,
                        columnNumber: 36
                    }, this),
                    tooltipType === 'kommune' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(KommuneTooltip, {
                        data: tooltipData
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 492,
                        columnNumber: 41
                    }, this),
                    tooltipType === 'bnbo' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(BNBOTooltip, {
                        data: tooltipData
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 493,
                        columnNumber: 38
                    }, this),
                    ("TURBOPACK compile-time value", "development") === 'development' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("details", {
                        className: "mt-3",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("summary", {
                                className: "font-mono text-xs text-gray-500 cursor-pointer hover:text-gray-700",
                                children: "RAW DATA DEBUG"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 498,
                                columnNumber: 15
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("pre", {
                                className: "font-mono text-xs mt-2 p-2 bg-gray-100 rounded overflow-auto max-h-32 text-gray-700",
                                children: JSON.stringify(tooltipData, null, 2)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 501,
                                columnNumber: 15
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 497,
                        columnNumber: 13
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 490,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/MapTooltip.tsx",
            lineNumber: 489,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 482,
        columnNumber: 5
    }, this);
};
_s(MapTooltip, "Ntkkd3J+9yOYr3ukqYmlafyp0e0=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTooltipState"]
    ];
});
_c3 = MapTooltip;
const __TURBOPACK__default__export__ = MapTooltip;
var _c, _c1, _c2, _c3;
__turbopack_context__.k.register(_c, "H3Tooltip");
__turbopack_context__.k.register(_c1, "KommuneTooltip");
__turbopack_context__.k.register(_c2, "BNBOTooltip");
__turbopack_context__.k.register(_c3, "MapTooltip");
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$MapTooltip$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/MapTooltip.tsx [app-client] (ecmascript)");
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
    // Compute layer visibility based on zoom (stable)
    const shouldShowKommune = zoom <= 8;
    const shouldShowH3 = zoom >= 9;
    const currentH3Resolution = zoom >= 14 ? 10 : zoom >= 12 ? 9 : zoom >= 10 ? 8 : 7;
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
                    console.log('✅ Added bnbo source');
                }
                console.log('🗺️ Final sources configuration:', sources);
                // Create layers array with only layers for available sources
                const layers = [];
                // Always add basemap layer if available
                if (sources.basemap) {
                    layers.push({
                        id: 'basemap-fill',
                        type: 'fill',
                        source: 'basemap',
                        'source-layer': 'earth',
                        paint: {
                            'fill-color': '#000000',
                            'fill-opacity': 1
                        }
                    });
                    console.log('✅ Added basemap layer');
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
                // Add BNBO layers if available
                if (sources.bnbo) {
                    layers.push({
                        id: 'bnbo-fill',
                        type: 'fill',
                        source: 'bnbo',
                        'source-layer': 'bnbo_areas',
                        layout: {
                            visibility: showBNBOLayer ? 'visible' : 'none'
                        },
                        paint: {
                            'fill-color': [
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
                            'fill-opacity': 0.6
                        }
                    }, {
                        id: 'bnbo-stroke',
                        type: 'line',
                        source: 'bnbo',
                        'source-layer': 'bnbo_areas',
                        layout: {
                            visibility: showBNBOLayer ? 'visible' : 'none'
                        },
                        paint: {
                            'line-color': '#ffffff',
                            'line-width': 1,
                            'line-opacity': 0.8
                        }
                    });
                    console.log('✅ Added BNBO layers');
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
                        // Log the full error object to see what properties are available
                        console.error('❌ Full error object:', JSON.stringify(e, null, 2));
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
                        }
                    }
                }["PMTilesMapInner.useEffect.updateLayerVisibility"];
                // Update Kommune layer visibility
                updateLayerVisibility('kommune-fill', layerVisibility.shouldShowKommune);
                updateLayerVisibility('kommune-stroke', layerVisibility.shouldShowKommune);
                // Update H3 layer visibility
                updateLayerVisibility('h3-fill', layerVisibility.shouldShowH3);
                updateLayerVisibility('h3-stroke', layerVisibility.shouldShowH3);
                // Update BNBO layer visibility
                updateLayerVisibility('bnbo-fill', showBNBOLayer);
                updateLayerVisibility('bnbo-stroke', showBNBOLayer);
            } catch (error) {
                console.warn('Error updating layer visibility:', error);
            }
        }
    }["PMTilesMapInner.useEffect"], [
        zoom,
        showBNBOLayer,
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
                        }
                    }
                }["PMTilesMapInner.useEffect"]);
                // Re-add Kommune layers with correct source-layer name
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
                });
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
                });
                // Re-add H3 layers with correct source-layer name
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
                });
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
                });
                console.log('✅ Updated source-layer names:', {
                    kommune: kommuneSourceLayer,
                    h3: h3SourceLayer
                });
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
                        lineNumber: 894,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm",
                        children: "Loading map..."
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 895,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 893,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 892,
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
                        lineNumber: 906,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm mb-2",
                        children: "Map Error"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 907,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-xs",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 908,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: clearError,
                        className: "mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 909,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 905,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 904,
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
                lineNumber: 922,
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
                            lineNumber: 928,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "text-white text-sm",
                            children: "Initializing map..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 929,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 927,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 926,
                columnNumber: 9
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute top-4 left-4 bg-black/80 text-white px-3 py-2 rounded text-sm max-w-xs",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Year: ",
                            selectedYear
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 936,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Mode: ",
                            selectedDataMode
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 937,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Zoom: ",
                            Math.round(zoom * 10) / 10
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 938,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Layers: ",
                            shouldShowKommune ? 'Kommune' : shouldShowH3 ? `H3 (${currentH3Resolution})` : 'None'
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 939,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-2 text-xs opacity-75",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: "Sources:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 941,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• Basemap: ",
                                    pmtilesUrls.basemap ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 942,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• Kommune: ",
                                    pmtilesUrls.kommune ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 943,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• H3: ",
                                    pmtilesUrls.h3 ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 944,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• BNBO: ",
                                    pmtilesUrls.bnbo ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 945,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 940,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 935,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$MapTooltip$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["MapTooltip"], {}, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 950,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 921,
        columnNumber: 5
    }, this);
};
_s(PMTilesMapInner, "wUiLJuyEOM4pIWukC/If6+9UIp0=", false, function() {
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
                    lineNumber: 962,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                    className: "text-white text-xl font-semibold mb-2",
                    children: "Map Error"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 963,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                    className: "text-gray-400 mb-4",
                    children: error.message
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 964,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: resetErrorBoundary,
                    className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                    children: "Reload Map"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 965,
                    columnNumber: 7
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 961,
            columnNumber: 5
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 960,
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
            lineNumber: 979,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 978,
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
                lineNumber: 29,
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
                    lineNumber: 37,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 34,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex justify-between text-xs text-gray-600",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "0"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 42,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Low"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 43,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Medium"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 44,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "High"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 45,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: [
                            "1000+ ",
                            config.unit
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 46,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 41,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "mt-2 text-xs text-gray-500",
                children: config.description
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 49,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
        lineNumber: 28,
        columnNumber: 5
    }, this);
};
_c = ColorScaleLegend;
const DataModeSelector = ({ className = '' })=>{
    _s();
    const { selectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { setSelectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    const modes = [
        {
            key: 'pesticide_total',
            label: 'Total Pesticide',
            description: 'All pesticide applications combined',
            color: 'text-gray-700'
        },
        {
            key: 'pfas',
            label: 'PFAS',
            description: 'PFAS-containing pesticides only',
            color: 'text-red-600'
        },
        {
            key: 'diquat',
            label: 'Diquat',
            description: 'Diquat-containing pesticides only',
            color: 'text-blue-600'
        },
        {
            key: 'glyphosate',
            label: 'Glyphosate',
            description: 'Glyphosate-containing pesticides only',
            color: 'text-green-600'
        }
    ];
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `bg-white rounded-lg shadow-lg border border-gray-200 p-4 ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                className: "text-lg font-semibold text-gray-900 mb-4",
                children: "Data Mode"
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 89,
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
                                            lineNumber: 104,
                                            columnNumber: 17
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "text-sm text-gray-600 mt-1",
                                            children: mode.description
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                            lineNumber: 109,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                    lineNumber: 103,
                                    columnNumber: 15
                                }, this),
                                selectedDataMode === mode.key && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "ml-3",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "w-2 h-2 bg-blue-500 rounded-full"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                        lineNumber: 116,
                                        columnNumber: 19
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                    lineNumber: 115,
                                    columnNumber: 17
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 102,
                            columnNumber: 13
                        }, this)
                    }, mode.key, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 93,
                        columnNumber: 11
                    }, this))
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 91,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(ColorScaleLegend, {
                mode: selectedDataMode
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 125,
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
                            lineNumber: 130,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: "Data visualization switches automatically between Kommune (low zoom) and H3 cell (high zoom) layers. Zoom in for detailed field-level analysis."
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 131,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                    lineNumber: 129,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 128,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
        lineNumber: 88,
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/settings.js [app-client] (ecmascript) <export default as Settings>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Eye$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/eye.js [app-client] (ecmascript) <export default as Eye>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2d$off$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__EyeOff$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/eye-off.js [app-client] (ecmascript) <export default as EyeOff>");
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
function Home() {
    _s();
    const [isInitialized, setIsInitialized] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [isInitializing, setIsInitializing] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(true);
    const [showControls, setShowControls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(true);
    // Store state
    const { selectedYear, selectedDataMode, availableYearOptions } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { showBNBOLayer } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLayerVisibility"])();
    const { error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"])();
    // Store actions
    const { setAvailableYearOptions, toggleBNBOLayer, setError: mapSetError, clearError: mapClearError } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
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
                        lineNumber: 65,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-lg font-medium",
                        children: "Loading PMTiles Map..."
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 66,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-sm mt-2",
                        children: "Discovering latest data from GCS bucket"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 67,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 64,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 63,
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
                        lineNumber: 78,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                        className: "text-white text-xl font-semibold mb-2",
                        children: "Something went wrong"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 79,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 mb-4",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 80,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                        children: "Reload Application"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 81,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 77,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 76,
            columnNumber: 7
        }, this);
    }
    // Get year count for display
    const yearCount = availableYearOptions.filter((year)=>typeof year === 'number').length;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-gray-900 text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-4 z-50",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "flex items-center justify-between",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                    className: "text-xl font-bold text-white",
                                    children: "Danish Agricultural Pesticide Analysis"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 102,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                    className: "text-sm text-gray-400",
                                    children: [
                                        "PMTiles visualization • ",
                                        yearCount,
                                        " years of data + cumulative"
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 103,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 101,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$StepSlider$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["StepSlider"], {}, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 108,
                                columnNumber: 13
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 107,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center space-x-4",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                    onClick: toggleBNBOLayer,
                                    className: `flex items-center space-x-2 px-3 py-2 rounded-lg transition-all ${showBNBOLayer ? 'bg-green-600 hover:bg-green-700' : 'bg-gray-700 hover:bg-gray-600'}`,
                                    children: [
                                        showBNBOLayer ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Eye$3e$__["Eye"], {
                                            className: "w-4 h-4"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 121,
                                            columnNumber: 32
                                        }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2d$off$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__EyeOff$3e$__["EyeOff"], {
                                            className: "w-4 h-4"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 121,
                                            columnNumber: 62
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            className: "text-sm",
                                            children: "BNBO"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 122,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 113,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                    onClick: ()=>setShowControls(!showControls),
                                    className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-all",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__["Settings"], {
                                        className: "w-5 h-5"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 129,
                                        columnNumber: 15
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 125,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 112,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/app/page.tsx",
                    lineNumber: 99,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 98,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1 flex relative",
                children: [
                    showControls && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-80 bg-gray-800/95 backdrop-blur-sm border-r border-gray-700 overflow-y-auto",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["DataModeSelector"], {
                                className: "m-4"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 140,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "m-4 p-4 bg-gray-700/50 rounded-lg",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                        className: "text-sm font-semibold text-white mb-3",
                                        children: "Current View"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 144,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "space-y-2 text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "Year:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 147,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-white",
                                                        children: selectedYear === 'total' ? 'Cumulative (All Years)' : selectedYear
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 148,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 146,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "Data Mode:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 153,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-white capitalize",
                                                        children: selectedDataMode.replace('_', ' ')
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 154,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 152,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "BNBO Layer:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 157,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: `font-medium ${showBNBOLayer ? 'text-green-400' : 'text-gray-400'}`,
                                                        children: showBNBOLayer ? 'Visible' : 'Hidden'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 158,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 156,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 145,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "mt-4 pt-3 border-t border-gray-600",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                className: "text-xs font-semibold text-gray-300 mb-2",
                                                children: "Usage Tips"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 165,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("ul", {
                                                className: "text-xs text-gray-400 space-y-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom out: Municipality-level data"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 167,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom in: H3 cell-level detail"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 168,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Hover for detailed information"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 169,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Click for expanded data view"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 170,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: '• Use "Total" for cumulative analysis'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 171,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 166,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 164,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 143,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 139,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1 relative",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["PMTilesMap"], {
                                className: "w-full h-full"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 180,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "absolute top-4 right-4 z-40",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-gray-600 text-center",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-medium",
                                                children: "Zoom Level"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 186,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-gray-500",
                                                children: "Auto-switching layers"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 187,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 185,
                                        columnNumber: 15
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 184,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 183,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "absolute bottom-4 left-4 z-40",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg max-w-xs",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-gray-800",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-medium mb-1",
                                                children: "Layer Switching"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 196,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "space-y-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-blue-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 199,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "Kommune (zoom 4-8)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 200,
                                                                columnNumber: 21
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 198,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-red-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 203,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "H3 Cells (zoom 9+)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 204,
                                                                columnNumber: 21
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 202,
                                                        columnNumber: 19
                                                    }, this),
                                                    showBNBOLayer && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-green-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 208,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "BNBO Protected Areas"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 209,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 207,
                                                        columnNumber: 21
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
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 194,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 193,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 179,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 136,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/app/page.tsx",
        lineNumber: 96,
        columnNumber: 5
    }, this);
}
_s(Home, "+Mtj+4cZqe2D2g92LaQ05Xg0yiU=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLayerVisibility"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"],
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

//# sourceMappingURL=src_5c4c84b7._.js.map