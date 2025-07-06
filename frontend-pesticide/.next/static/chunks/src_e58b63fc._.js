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
var _s = __turbopack_context__.k.signature(), _s1 = __turbopack_context__.k.signature(), _s2 = __turbopack_context__.k.signature(), _s3 = __turbopack_context__.k.signature(), _s4 = __turbopack_context__.k.signature(), _s5 = __turbopack_context__.k.signature(), _s6 = __turbopack_context__.k.signature(), _s7 = __turbopack_context__.k.signature(), _s8 = __turbopack_context__.k.signature(), _s9 = __turbopack_context__.k.signature(), _s10 = __turbopack_context__.k.signature(), _s11 = __turbopack_context__.k.signature(), _s12 = __turbopack_context__.k.signature(), _s13 = __turbopack_context__.k.signature(), _s14 = __turbopack_context__.k.signature(), _s15 = __turbopack_context__.k.signature(), _s16 = __turbopack_context__.k.signature(), _s17 = __turbopack_context__.k.signature(), _s18 = __turbopack_context__.k.signature(), _s19 = __turbopack_context__.k.signature(), _s20 = __turbopack_context__.k.signature(), _s21 = __turbopack_context__.k.signature(), _s22 = __turbopack_context__.k.signature(), _s23 = __turbopack_context__.k.signature();
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
        showBNBOLayer: false,
        showBasemap: true,
        isLoading: false,
        isLoadingYear: false,
        isLoadingTiles: false,
        loadingMessage: '',
        error: null,
        availableYears: [],
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
        setAvailableYears: (availableYears)=>set({
                availableYears
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
                showBNBOLayer: false,
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
const useAvailableResolutions = ()=>{
    _s7();
    return useMapStore({
        "useAvailableResolutions.useMapStore": (state)=>state.availableResolutions
    }["useAvailableResolutions.useMapStore"]);
};
_s7(useAvailableResolutions, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowBNBOLayer = ()=>{
    _s8();
    return useMapStore({
        "useShowBNBOLayer.useMapStore": (state)=>state.showBNBOLayer
    }["useShowBNBOLayer.useMapStore"]);
};
_s8(useShowBNBOLayer, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowBasemap = ()=>{
    _s9();
    return useMapStore({
        "useShowBasemap.useMapStore": (state)=>state.showBasemap
    }["useShowBasemap.useMapStore"]);
};
_s9(useShowBasemap, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoading = ()=>{
    _s10();
    return useMapStore({
        "useIsLoading.useMapStore": (state)=>state.isLoading
    }["useIsLoading.useMapStore"]);
};
_s10(useIsLoading, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoadingYear = ()=>{
    _s11();
    return useMapStore({
        "useIsLoadingYear.useMapStore": (state)=>state.isLoadingYear
    }["useIsLoadingYear.useMapStore"]);
};
_s11(useIsLoadingYear, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useIsLoadingTiles = ()=>{
    _s12();
    return useMapStore({
        "useIsLoadingTiles.useMapStore": (state)=>state.isLoadingTiles
    }["useIsLoadingTiles.useMapStore"]);
};
_s12(useIsLoadingTiles, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useLoadingMessage = ()=>{
    _s13();
    return useMapStore({
        "useLoadingMessage.useMapStore": (state)=>state.loadingMessage
    }["useLoadingMessage.useMapStore"]);
};
_s13(useLoadingMessage, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useError = ()=>{
    _s14();
    return useMapStore({
        "useError.useMapStore": (state)=>state.error
    }["useError.useMapStore"]);
};
_s14(useError, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowControls = ()=>{
    _s15();
    return useMapStore({
        "useShowControls.useMapStore": (state)=>state.showControls
    }["useShowControls.useMapStore"]);
};
_s15(useShowControls, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useShowTooltip = ()=>{
    _s16();
    return useMapStore({
        "useShowTooltip.useMapStore": (state)=>state.showTooltip
    }["useShowTooltip.useMapStore"]);
};
_s16(useShowTooltip, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useTooltipData = ()=>{
    _s17();
    return useMapStore({
        "useTooltipData.useMapStore": (state)=>state.tooltipData
    }["useTooltipData.useMapStore"]);
};
_s17(useTooltipData, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useTooltipPosition = ()=>{
    _s18();
    return useMapStore({
        "useTooltipPosition.useMapStore": (state)=>state.tooltipPosition
    }["useTooltipPosition.useMapStore"]);
};
_s18(useTooltipPosition, "nurWsAZ3N93L89lPaNY0xu41ksw=", false, function() {
    return [
        useMapStore
    ];
});
const useMapViewState = ()=>{
    _s19();
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
_s19(useMapViewState, "UgxXP0nHga0ZRG28hQjNYKUeIaY=", false, function() {
    return [
        useZoom,
        useCenter,
        useBearing,
        usePitch
    ];
});
const useDataState = ()=>{
    _s20();
    const selectedYear = useSelectedYear();
    const selectedDataMode = useSelectedDataMode();
    const availableYears = useAvailableYears();
    const availableResolutions = useAvailableResolutions();
    return {
        selectedYear,
        selectedDataMode,
        availableYears,
        availableResolutions
    };
};
_s20(useDataState, "7EA3m6ko28uxmVKscXngcCbcbQc=", false, function() {
    return [
        useSelectedYear,
        useSelectedDataMode,
        useAvailableYears,
        useAvailableResolutions
    ];
});
const useLayerVisibility = ()=>{
    _s21();
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
_s21(useLayerVisibility, "XUgWOAQAPrCwTQKLmeWmXLKgFHc=", false, function() {
    return [
        useZoom,
        useShowBNBOLayer,
        useShowBasemap
    ];
});
const useLoadingState = ()=>{
    _s22();
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
_s22(useLoadingState, "ik6VbIBVYUirotiPuBBfnGe59cA=", false, function() {
    return [
        useIsLoading,
        useIsLoadingYear,
        useIsLoadingTiles,
        useLoadingMessage,
        useError
    ];
});
const useTooltipState = ()=>{
    _s23();
    const showTooltip = useShowTooltip();
    const tooltipData = useTooltipData();
    const tooltipPosition = useTooltipPosition();
    return {
        showTooltip,
        tooltipData,
        tooltipPosition
    };
};
_s23(useTooltipState, "STrPZnizyzIARKPCu+euDHEFu8c=", false, function() {
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
const formatPercentage = (value)=>{
    if (value === undefined || value === null) return 'N/A';
    return `${formatNumber(value * 100, 1)}%`;
};
const getTooltipType = (data)=>{
    if (data.h3_cell) return 'h3';
    if (data.kommune_code || data.kommune_name) return 'kommune';
    if (data.bnbo_id || data.status) return 'bnbo';
    return 'unknown';
};
const H3Tooltip = ({ data })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: "H3 Cell"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 94,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-sm text-gray-600 font-mono",
                        children: data.h3_cell
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 95,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 93,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "grid grid-cols-2 gap-3 text-sm",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 100,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: [
                                    formatNumber(data.h3_cell_area_ha),
                                    " ha"
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 101,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 99,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Fields:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 104,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: formatNumber(data.unique_field_count, 0)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 105,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 103,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Coverage:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 108,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: formatPercentage(data.actual_coverage_ratio)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 109,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 107,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Crop Types:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 112,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: formatNumber(data.crop_diversity, 0)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 113,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 111,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 98,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Pesticide Intensity (per ha)"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 118,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 121,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.pesticide_belastning_per_ha),
                                            " kg/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 122,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 120,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 125,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.pfas_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 126,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 124,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 129,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.diquat_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 130,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 128,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 133,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.glyphosate_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 134,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 132,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 119,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 117,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Total Applications"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 140,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "All:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 143,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.total_pesticide_applications, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 144,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 142,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 147,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.pfas_containing_applications, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 148,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 146,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 151,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.diquat_containing_applications, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 152,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 150,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 155,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.glyphosate_containing_applications, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 156,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 154,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 141,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 139,
                columnNumber: 5
            }, this),
            data.crop_types && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-1",
                        children: "Crop Types"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 163,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-xs text-gray-600 max-w-xs break-words",
                        children: data.crop_types
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 164,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 162,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 92,
        columnNumber: 3
    }, this);
_c = H3Tooltip;
const KommuneTooltip = ({ data })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: data.kommune_name
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 173,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-sm text-gray-600",
                        children: [
                            "Kommune Code: ",
                            data.kommune_code
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 174,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 172,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "grid grid-cols-2 gap-3 text-sm",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Total Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 179,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: [
                                    formatNumber(data.kommune_area_ha),
                                    " ha"
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 180,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 178,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Agricultural:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 183,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: [
                                    formatNumber(data.total_agricultural_area_ha),
                                    " ha"
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 184,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 182,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Fields:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 187,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: formatNumber(data.unique_field_count, 0)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 188,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 186,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Companies:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 191,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "font-medium",
                                children: formatNumber(data.unique_company_count, 0)
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 192,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 190,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 177,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Coverage Statistics"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 197,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Agricultural %:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 200,
                                        columnNumber: 12
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatPercentage((data.agricultural_coverage_pct || 0) / 100)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 201,
                                        columnNumber: 12
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 199,
                                columnNumber: 18
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Avg Coverage:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 204,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatPercentage(data.avg_field_coverage_ratio)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 205,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 203,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Max Coverage:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 208,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatPercentage(data.max_field_coverage_ratio)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 209,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 207,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 198,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 196,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Pesticide Intensity (per ha)"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 215,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 218,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.pesticide_belastning_per_ha),
                                            " kg/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 219,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 217,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 222,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.pfas_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 223,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 221,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 226,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.diquat_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 227,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 225,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 230,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: [
                                            formatNumber(data.glyphosate_containing_active_ingredient_intensity_grams_per_ha),
                                            " g/ha"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 231,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 229,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 216,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 214,
                columnNumber: 5
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Product Diversity"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 237,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-2 gap-2 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 240,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.unique_pesticide_products, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 241,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 239,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 244,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.unique_pfas_products, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 245,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 243,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 248,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.unique_diquat_products, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 249,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 247,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 252,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "font-medium",
                                        children: formatNumber(data.unique_glyphosate_products, 0)
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 253,
                                        columnNumber: 11
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 251,
                                columnNumber: 9
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 238,
                        columnNumber: 7
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 236,
                columnNumber: 5
            }, this),
            data.crop_types && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-1",
                        children: "Crop Types"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 260,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-xs text-gray-600 max-w-xs break-words",
                        children: data.crop_types
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 261,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 259,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 171,
        columnNumber: 3
    }, this);
_c1 = KommuneTooltip;
const BNBOTooltip = ({ data })=>{
    const statusConfig = data.status ? __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["BNBO_STATUS_CONFIG"][data.status] : null;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: "BNBO Area"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 273,
                        columnNumber: 9
                    }, this),
                    data.bnbo_id && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-sm text-gray-600 font-mono",
                        children: data.bnbo_id
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 275,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 272,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: [
                    statusConfig && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center space-x-2",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "w-3 h-3 rounded-full",
                                style: {
                                    backgroundColor: statusConfig.color
                                }
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 282,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-medium text-sm",
                                        children: statusConfig.label
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 287,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-gray-600",
                                        children: statusConfig.description
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 288,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 286,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 281,
                        columnNumber: 11
                    }, this),
                    data.area_ha && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 295,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "font-medium ml-2",
                                children: [
                                    formatNumber(data.area_ha),
                                    " ha"
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 296,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 294,
                        columnNumber: 11
                    }, this),
                    data.description && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Description:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 302,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                className: "text-xs text-gray-600 mt-1 max-w-xs break-words",
                                children: data.description
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 303,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 301,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/MapTooltip.tsx",
                lineNumber: 279,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 271,
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
        className: "absolute z-50 pointer-events-none",
        style: {
            left: adjustedPosition.left,
            top: adjustedPosition.top
        },
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-white rounded-lg shadow-lg border border-gray-200 p-4 max-w-sm",
            children: [
                tooltipType === 'h3' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(H3Tooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 345,
                    columnNumber: 34
                }, this),
                tooltipType === 'kommune' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(KommuneTooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 346,
                    columnNumber: 39
                }, this),
                tooltipType === 'bnbo' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(BNBOTooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 347,
                    columnNumber: 36
                }, this),
                tooltipType === 'unknown' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-sm text-gray-600",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            children: "Unknown data type"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 350,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("pre", {
                            className: "text-xs mt-2 overflow-hidden",
                            children: [
                                JSON.stringify(tooltipData, null, 2).slice(0, 200),
                                "..."
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 351,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 349,
                    columnNumber: 11
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/MapTooltip.tsx",
            lineNumber: 344,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/MapTooltip.tsx",
        lineNumber: 337,
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
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
;
;
// Dynamic imports for browser-only modules
const loadMapLibreAndPMTiles = async ()=>{
    if ("TURBOPACK compile-time falsy", 0) {
        "TURBOPACK unreachable";
    }
    const [maplibregl, { Protocol }] = await Promise.all([
        __turbopack_context__.r("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-client] (ecmascript, async loader)")(__turbopack_context__.i),
        __turbopack_context__.r("[project]/node_modules/pmtiles/dist/index.js [app-client] (ecmascript, async loader)")(__turbopack_context__.i)
    ]);
    // Register PMTiles protocol
    let protocolRegistered = false;
    if (!protocolRegistered) {
        const protocol = new Protocol();
        maplibregl.default.addProtocol('pmtiles', protocol.tile);
        protocolRegistered = true;
    }
    return maplibregl.default;
};
const PMTilesMapInner = ({ className = 'w-full h-full' })=>{
    _s();
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    const map = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    const [mapLibre, setMapLibre] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    const [mapLoaded, setMapLoaded] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    // Store state
    const { zoom, center, bearing, pitch } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapViewState"])();
    const { selectedYear, selectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { showBNBOLayer } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLayerVisibility"])();
    const { isLoading, error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"])();
    // Store actions
    const { setViewState, setIsLoading, setError, clearError, showTooltipWithData, hideTooltip } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Load MapLibre and PMTiles
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            loadMapLibreAndPMTiles().then(setMapLibre);
        }
    }["PMTilesMapInner.useEffect"], []);
    // Initialize map
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "PMTilesMapInner.useEffect": ()=>{
            if (!mapContainer.current || map.current || !mapLibre) return;
            try {
                map.current = new mapLibre.Map({
                    container: mapContainer.current,
                    style: {
                        version: 8,
                        sources: {
                            'basemap': {
                                type: 'raster',
                                tiles: [
                                    'https://tile.openstreetmap.org/{z}/{x}/{y}.png'
                                ],
                                tileSize: 256,
                                attribution: '© OpenStreetMap contributors'
                            }
                        },
                        layers: [
                            {
                                id: 'basemap',
                                type: 'raster',
                                source: 'basemap',
                                paint: {
                                    'raster-opacity': 0.8
                                }
                            }
                        ]
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
                        const features = map.current?.queryRenderedFeatures(e.point);
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
                        const features = map.current?.queryRenderedFeatures(e.point);
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
                        console.error('Map error:', e);
                        setError('Map loading error');
                    }
                }["PMTilesMapInner.useEffect"]);
            } catch (err) {
                console.error('Error initializing map:', err);
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
        center,
        zoom,
        bearing,
        pitch
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
                        lineNumber: 182,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm",
                        children: "Loading map..."
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 183,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 181,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 180,
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
                        lineNumber: 194,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm mb-2",
                        children: "Map Error"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 195,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-xs",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 196,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: clearError,
                        className: "mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 197,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 193,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 192,
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
                lineNumber: 210,
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
                            lineNumber: 216,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "text-white text-sm",
                            children: "Initializing map..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 217,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 215,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 214,
                columnNumber: 9
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute top-4 left-4 bg-black/80 text-white px-3 py-2 rounded text-sm",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Year: ",
                            selectedYear
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 224,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Mode: ",
                            selectedDataMode
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 225,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Zoom: ",
                            Math.round(zoom * 10) / 10
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 226,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 223,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$MapTooltip$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["MapTooltip"], {}, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 230,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 209,
        columnNumber: 5
    }, this);
};
_s(PMTilesMapInner, "QJFicv9MubD5lU3zGfZqpLAUZd4=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapViewState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLayerVisibility"],
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
                    lineNumber: 242,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                    className: "text-white text-xl font-semibold mb-2",
                    children: "Map Error"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 243,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                    className: "text-gray-400 mb-4",
                    children: error.message
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 244,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: resetErrorBoundary,
                    className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                    children: "Reload Map"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 245,
                    columnNumber: 7
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 241,
            columnNumber: 5
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 240,
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
            lineNumber: 259,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 258,
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
    // Mock data availability - in a real implementation, this would query the GCS API
    async getDataAvailability() {
        const cacheKey = 'data_availability';
        if (this.cache.has(cacheKey)) {
            return this.cache.get(cacheKey);
        }
        // For now, return static data based on known structure
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
    }
    async discoverBasemapTiles() {
        return `${this.baseUrl}/pmtiles/protomaps_denmark.pmtiles`;
    }
    async discoverLatestBNBOTiles() {
        return `${this.baseUrl}/pmtiles/bnbo_areas.pmtiles`;
    }
    async discoverLatestH3Tiles(year, resolution) {
        // In a real implementation, this would discover the latest timestamp
        // For now, we'll construct the URL based on known patterns
        const pattern = `gold/pmtiles/h3_pfas_${year}_res${resolution}`;
        // Mock latest timestamp - in practice, this would be discovered
        const mockTimestamp = '20241201_120000';
        return `${this.baseUrl}/${pattern}/${mockTimestamp}/h3_pfas_${year}_res${resolution}.pmtiles`;
    }
    async discoverLatestKommuneTiles(year) {
        // In a real implementation, this would discover the latest timestamp
        const pattern = `gold/pmtiles/kommune_pfas_${year}`;
        // Mock latest timestamp
        const mockTimestamp = '20241201_120000';
        return `${this.baseUrl}/${pattern}/${mockTimestamp}/kommune_pfas_${year}.pmtiles`;
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
            h3Urls[key] = await this.discoverLatestH3Tiles(year, resolution);
        }
        // Get kommune URL
        const kommuneUrls = {};
        kommuneUrls[year.toString()] = await this.discoverLatestKommuneTiles(year);
        return {
            basemap,
            h3: h3Urls,
            kommune: kommuneUrls,
            bnbo
        };
    }
    // Clear cache
    clearCache() {
        this.cache.clear();
    }
}
const pmtilesDiscovery = new PMTilesDiscoveryService();
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/play.js [app-client] (ecmascript) <export default as Play>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/pause.js [app-client] (ecmascript) <export default as Pause>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-left.js [app-client] (ecmascript) <export default as ChevronLeft>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-right.js [app-client] (ecmascript) <export default as ChevronRight>");
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
function Home() {
    _s();
    const [isInitialized, setIsInitialized] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [showControls, setShowControls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(true);
    const [isAnimating, setIsAnimating] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [animationInterval, setAnimationInterval] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    // Store state
    const { selectedYear, selectedDataMode, availableYears } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useDataState"])();
    const { showBNBOLayer } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLayerVisibility"])();
    const { isLoading, error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useLoadingState"])();
    // Store actions
    const { setSelectedYear, setAvailableYears, toggleBNBOLayer, setError: mapSetError, clearError: mapClearError, setIsLoading } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Initialize the application
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Home.useEffect": ()=>{
            const initialize = {
                "Home.useEffect.initialize": async ()=>{
                    try {
                        setIsLoading(true);
                        const availability = await __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getDataAvailability();
                        setAvailableYears(availability.years);
                        // Set initial year to most recent
                        if (availability.years.length > 0) {
                            setSelectedYear(availability.latestYear);
                        }
                        setIsInitialized(true);
                        mapClearError();
                    } catch (err) {
                        console.error('Error initializing application:', err);
                        mapSetError('Failed to initialize application');
                    } finally{
                        setIsLoading(false);
                    }
                }
            }["Home.useEffect.initialize"];
            initialize();
        }
    }["Home.useEffect"], [
        setIsLoading,
        setAvailableYears,
        setSelectedYear,
        mapSetError,
        mapClearError
    ]);
    // Animation controls
    const startAnimation = ()=>{
        if (availableYears.length <= 1) return;
        setIsAnimating(true);
        const interval = setInterval(()=>{
            const currentIndex = availableYears.indexOf(selectedYear);
            const nextIndex = (currentIndex + 1) % availableYears.length;
            setSelectedYear(availableYears[nextIndex]);
        }, 1500); // Change year every 1.5 seconds
        setAnimationInterval(interval);
    };
    const stopAnimation = ()=>{
        setIsAnimating(false);
        if (animationInterval) {
            clearInterval(animationInterval);
            setAnimationInterval(null);
        }
    };
    const goToNextYear = ()=>{
        const currentIndex = availableYears.indexOf(selectedYear);
        if (currentIndex < availableYears.length - 1) {
            setSelectedYear(availableYears[currentIndex + 1]);
        }
    };
    const goToPreviousYear = ()=>{
        const currentIndex = availableYears.indexOf(selectedYear);
        if (currentIndex > 0) {
            setSelectedYear(availableYears[currentIndex - 1]);
        }
    };
    const canGoNext = ()=>{
        const currentIndex = availableYears.indexOf(selectedYear);
        return currentIndex < availableYears.length - 1;
    };
    const canGoPrevious = ()=>{
        const currentIndex = availableYears.indexOf(selectedYear);
        return currentIndex > 0;
    };
    // Cleanup animation on unmount
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Home.useEffect": ()=>{
            return ({
                "Home.useEffect": ()=>{
                    if (animationInterval) {
                        clearInterval(animationInterval);
                    }
                }
            })["Home.useEffect"];
        }
    }["Home.useEffect"], [
        animationInterval
    ]);
    // Loading state
    if (isLoading || !isInitialized) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-gray-900 flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 118,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-lg font-medium",
                        children: "Loading PMTiles Map..."
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 119,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-sm mt-2",
                        children: "Discovering latest data from GCS bucket"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 120,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 117,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 116,
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
                        lineNumber: 131,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                        className: "text-white text-xl font-semibold mb-2",
                        children: "Something went wrong"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 132,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 mb-4",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 133,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                        children: "Reload Application"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 134,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 130,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 129,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-gray-900 text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-4 z-50",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center justify-between",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                        className: "text-xl font-bold text-white",
                                        children: "Danish Agricultural Pesticide Analysis"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 152,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                        className: "text-sm text-gray-400",
                                        children: [
                                            "PMTiles visualization • ",
                                            availableYears.length,
                                            " years of data"
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 153,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 151,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center space-x-4",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: goToPreviousYear,
                                        disabled: !canGoPrevious() || isAnimating,
                                        className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__["ChevronLeft"], {
                                            className: "w-5 h-5"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 163,
                                            columnNumber: 15
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 158,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: isAnimating ? stopAnimation : startAnimation,
                                        disabled: availableYears.length <= 1,
                                        className: "p-2 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                        children: isAnimating ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__["Pause"], {
                                            className: "w-5 h-5"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 171,
                                            columnNumber: 30
                                        }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__["Play"], {
                                            className: "w-5 h-5"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 171,
                                            columnNumber: 62
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 166,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: goToNextYear,
                                        disabled: !canGoNext() || isAnimating,
                                        className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__["ChevronRight"], {
                                            className: "w-5 h-5"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 179,
                                            columnNumber: 15
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 174,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "px-4 py-2 bg-gray-700 rounded-lg",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            className: "text-lg font-bold",
                                            children: selectedYear
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 184,
                                            columnNumber: 15
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 183,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 157,
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
                                                lineNumber: 198,
                                                columnNumber: 32
                                            }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2d$off$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__EyeOff$3e$__["EyeOff"], {
                                                className: "w-4 h-4"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 198,
                                                columnNumber: 62
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-sm",
                                                children: "BNBO"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 199,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 190,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: ()=>setShowControls(!showControls),
                                        className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-all",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__["Settings"], {
                                            className: "w-5 h-5"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 206,
                                            columnNumber: 15
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 202,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 189,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 149,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-4 flex items-center justify-center",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "w-full max-w-2xl",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "flex justify-between text-xs text-gray-400 mb-2",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            children: Math.min(...availableYears)
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 215,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            className: "font-bold text-white",
                                            children: selectedYear
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 216,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            children: Math.max(...availableYears)
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 217,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 214,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "relative",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                            type: "range",
                                            min: Math.min(...availableYears),
                                            max: Math.max(...availableYears),
                                            step: "1",
                                            value: selectedYear,
                                            onChange: (e)=>setSelectedYear(parseInt(e.target.value)),
                                            className: "w-full h-2 bg-gray-600 rounded-lg appearance-none cursor-pointer slider",
                                            disabled: isAnimating
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 220,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "flex justify-between mt-2",
                                            children: availableYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: `w-1 h-3 rounded-full transition-all ${year === selectedYear ? 'bg-blue-500' : 'bg-gray-500'}`
                                                }, year, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 232,
                                                    columnNumber: 19
                                                }, this))
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 230,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 219,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 213,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 212,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 148,
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
                                lineNumber: 250,
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
                                        lineNumber: 254,
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
                                                        lineNumber: 257,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-white",
                                                        children: selectedYear
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 258,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 256,
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
                                                        lineNumber: 261,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium text-white capitalize",
                                                        children: selectedDataMode.replace('_', ' ')
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 262,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 260,
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
                                                        lineNumber: 265,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: `font-medium ${showBNBOLayer ? 'text-green-400' : 'text-gray-400'}`,
                                                        children: showBNBOLayer ? 'Visible' : 'Hidden'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 266,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 264,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 255,
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
                                                lineNumber: 273,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("ul", {
                                                className: "text-xs text-gray-400 space-y-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom out: Municipality-level data"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 275,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom in: H3 cell-level detail"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 276,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Hover for detailed information"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 277,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Click for expanded data view"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 278,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 274,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 272,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 253,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 249,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1 relative",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["PMTilesMap"], {
                                className: "w-full h-full"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 287,
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
                                                lineNumber: 293,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-gray-500",
                                                children: "Auto-switching layers"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 294,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 292,
                                        columnNumber: 15
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 291,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 290,
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
                                                lineNumber: 303,
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
                                                                lineNumber: 306,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "Kommune (zoom 4-8)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 307,
                                                                columnNumber: 21
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 305,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-red-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 310,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "H3 Cells (zoom 9+)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 311,
                                                                columnNumber: 21
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 309,
                                                        columnNumber: 19
                                                    }, this),
                                                    showBNBOLayer && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-green-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 315,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                children: "BNBO Protected Areas"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 316,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 314,
                                                        columnNumber: 21
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 304,
                                                columnNumber: 17
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 302,
                                        columnNumber: 15
                                    }, this)
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 301,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 300,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 286,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 246,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/app/page.tsx",
        lineNumber: 146,
        columnNumber: 5
    }, this);
}
_s(Home, "q8rkvFqOHrAZhEiiZkn7KF/0p8o=", false, function() {
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

//# sourceMappingURL=src_e58b63fc._.js.map