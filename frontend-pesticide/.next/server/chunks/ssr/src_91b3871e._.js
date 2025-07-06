module.exports = {

"[project]/src/stores/map-store.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
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
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-ssr] (ecmascript)");
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
const useMapStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["devtools"])((set, get)=>({
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
const useZoom = ()=>useMapStore((state)=>state.zoom);
const useCenter = ()=>useMapStore((state)=>state.center);
const useBearing = ()=>useMapStore((state)=>state.bearing);
const usePitch = ()=>useMapStore((state)=>state.pitch);
const useSelectedYear = ()=>useMapStore((state)=>state.selectedYear);
const useSelectedDataMode = ()=>useMapStore((state)=>state.selectedDataMode);
const useAvailableYears = ()=>useMapStore((state)=>state.availableYears);
const useAvailableYearOptions = ()=>useMapStore((state)=>state.availableYearOptions);
const useAvailableResolutions = ()=>useMapStore((state)=>state.availableResolutions);
const useShowBNBOLayer = ()=>useMapStore((state)=>state.showBNBOLayer);
const useShowBasemap = ()=>useMapStore((state)=>state.showBasemap);
const useIsLoading = ()=>useMapStore((state)=>state.isLoading);
const useIsLoadingYear = ()=>useMapStore((state)=>state.isLoadingYear);
const useIsLoadingTiles = ()=>useMapStore((state)=>state.isLoadingTiles);
const useLoadingMessage = ()=>useMapStore((state)=>state.loadingMessage);
const useError = ()=>useMapStore((state)=>state.error);
const useShowControls = ()=>useMapStore((state)=>state.showControls);
const useShowTooltip = ()=>useMapStore((state)=>state.showTooltip);
const useTooltipData = ()=>useMapStore((state)=>state.tooltipData);
const useTooltipPosition = ()=>useMapStore((state)=>state.tooltipPosition);
const useMapViewState = ()=>{
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
const useDataState = ()=>{
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
const useLayerVisibility = ()=>{
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
const useLoadingState = ()=>{
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
const useTooltipState = ()=>{
    const showTooltip = useShowTooltip();
    const tooltipData = useTooltipData();
    const tooltipPosition = useTooltipPosition();
    return {
        showTooltip,
        tooltipData,
        tooltipPosition
    };
};
}}),
"[project]/src/components/map/MapTooltip.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "MapTooltip": (()=>MapTooltip),
    "default": (()=>__TURBOPACK__default__export__)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-ssr] (ecmascript)");
'use client';
;
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
const H3Tooltip = ({ data })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: "H3 Cell"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 94,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "grid grid-cols-2 gap-3 text-sm",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 100,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Fields:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 104,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Coverage:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 108,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Crop Types:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 112,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Pesticide Intensity (per ha)"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 118,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 121,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 125,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 129,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 133,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Total Applications"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 140,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "All:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 143,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 147,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 151,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 155,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            data.crop_types && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-1",
                        children: "Crop Types"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 163,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
const KommuneTooltip = ({ data })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: data.kommune_name
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 173,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "grid grid-cols-2 gap-3 text-sm",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Total Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 179,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Agricultural:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 183,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Fields:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 187,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Companies:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 191,
                                columnNumber: 9
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Coverage Statistics"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 197,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Agricultural %:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 200,
                                        columnNumber: 12
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Avg Coverage:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 204,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Max Coverage:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 208,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Pesticide Intensity (per ha)"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 215,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "space-y-1 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 218,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 222,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 226,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 230,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-2",
                        children: "Product Diversity"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 237,
                        columnNumber: 7
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "grid grid-cols-2 gap-2 text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-gray-600",
                                        children: "Total:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 240,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-red-600",
                                        children: "PFAS:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 244,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-blue-600",
                                        children: "Diquat:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 248,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex justify-between",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-green-600",
                                        children: "Glyphosate:"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 252,
                                        columnNumber: 11
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            data.crop_types && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-t border-gray-200 pt-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                        className: "font-medium text-gray-900 mb-1",
                        children: "Crop Types"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 260,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
const BNBOTooltip = ({ data })=>{
    const statusConfig = data.status ? __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["BNBO_STATUS_CONFIG"][data.status] : null;
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "space-y-3",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "border-b border-gray-200 pb-2",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                        className: "font-semibold text-gray-900",
                        children: "BNBO Area"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                        lineNumber: 273,
                        columnNumber: 9
                    }, this),
                    data.bnbo_id && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: [
                    statusConfig && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center space-x-2",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "w-3 h-3 rounded-full",
                                style: {
                                    backgroundColor: statusConfig.color
                                }
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 282,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "font-medium text-sm",
                                        children: statusConfig.label
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/MapTooltip.tsx",
                                        lineNumber: 287,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    data.area_ha && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Area:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 295,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                    data.description && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-sm",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-gray-600",
                                children: "Description:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/MapTooltip.tsx",
                                lineNumber: 302,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
const MapTooltip = ()=>{
    const { showTooltip, tooltipData, tooltipPosition } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useTooltipState"])();
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
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "absolute z-50 pointer-events-none",
        style: {
            left: adjustedPosition.left,
            top: adjustedPosition.top
        },
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-white rounded-lg shadow-lg border border-gray-200 p-4 max-w-sm",
            children: [
                tooltipType === 'h3' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(H3Tooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 345,
                    columnNumber: 34
                }, this),
                tooltipType === 'kommune' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(KommuneTooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 346,
                    columnNumber: 39
                }, this),
                tooltipType === 'bnbo' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(BNBOTooltip, {
                    data: tooltipData
                }, void 0, false, {
                    fileName: "[project]/src/components/map/MapTooltip.tsx",
                    lineNumber: 347,
                    columnNumber: 36
                }, this),
                tooltipType === 'unknown' && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-sm text-gray-600",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            children: "Unknown data type"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/MapTooltip.tsx",
                            lineNumber: 350,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("pre", {
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
const __TURBOPACK__default__export__ = MapTooltip;
}}),
"[project]/src/services/pmtiles-discovery.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
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
}}),
"[project]/src/components/map/PMTilesMap.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "PMTilesMap": (()=>PMTilesMap)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$react$2d$error$2d$boundary$2f$dist$2f$react$2d$error$2d$boundary$2e$development$2e$esm$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/react-error-boundary/dist/react-error-boundary.development.esm.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$MapTooltip$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/MapTooltip.tsx [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-ssr] (ecmascript)");
'use client';
;
;
;
;
;
;
;
// Dynamic imports for browser-only modules
const loadMapLibreAndPMTiles = async ()=>{
    if ("TURBOPACK compile-time truthy", 1) return null;
    "TURBOPACK unreachable";
    const maplibregl = undefined, Protocol = undefined;
    // Register PMTiles protocol
    let protocolRegistered;
};
const PMTilesMapInner = ({ className = 'w-full h-full' })=>{
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const map = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const [mapLibre, setMapLibre] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    const [mapLoaded, setMapLoaded] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [pmtilesUrls, setPmtilesUrls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])({});
    // Store state
    const { zoom, center, bearing, pitch } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapViewState"])();
    const { selectedYear, selectedDataMode, availableYears } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useDataState"])();
    const showBNBOLayer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])((state)=>state.showBNBOLayer);
    const showBasemap = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])((state)=>state.showBasemap);
    const { isLoading, error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useLoadingState"])();
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
    const { setViewState, setIsLoading, setError, clearError, showTooltipWithData, hideTooltip, setAvailableYears } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Load MapLibre and PMTiles
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        let mounted = true;
        const initMapLibre = async ()=>{
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
        };
        initMapLibre();
        return ()=>{
            mounted = false;
        };
    }, []);
    // Load PMTiles URLs
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        console.log('🔄 PMTiles useEffect triggered - year:', selectedYear, 'resolution:', currentH3Resolution, 'zoom:', zoom);
        let mounted = true;
        const loadPMTilesUrls = async ()=>{
            try {
                setIsLoading(true);
                // Discover and validate URLs
                const [validatedUrls, years] = await Promise.all([
                    __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].discoverAndValidateUrls(selectedYear, currentH3Resolution),
                    __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getAvailableYears()
                ]);
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
                    if (!validatedUrls.basemap) {
                        setError('Basemap not available');
                    } else {
                        clearError();
                    }
                }
            } catch (error) {
                console.error('Error loading PMTiles URLs:', error);
                if (mounted) {
                    setError('Failed to load data sources');
                }
            } finally{
                if (mounted) {
                    setIsLoading(false);
                }
            }
        };
        loadPMTilesUrls();
        return ()=>{
            mounted = false;
        };
    }, [
        selectedYear,
        currentH3Resolution
    ]);
    // Initialize map
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        console.log('🔍 Map initialization check:', {
            mapLibre: !!mapLibre,
            mapContainer: !!mapContainer.current,
            basemapUrl: pmtilesUrls.basemap,
            pmtilesUrls: pmtilesUrls
        });
        if (!mapLibre || !mapContainer.current || !pmtilesUrls.basemap) return;
        try {
            console.log('🚀 Creating map with URLs:', pmtilesUrls);
            map.current = new mapLibre.Map({
                container: mapContainer.current,
                style: {
                    version: 8,
                    sources: {
                        'basemap': {
                            type: 'vector',
                            url: `pmtiles://${pmtilesUrls.basemap}`
                        },
                        'kommune': {
                            type: 'vector',
                            url: `pmtiles://${pmtilesUrls.kommune}`
                        },
                        'h3': {
                            type: 'vector',
                            url: `pmtiles://${pmtilesUrls.h3}`
                        },
                        'bnbo': {
                            type: 'vector',
                            url: `pmtiles://${pmtilesUrls.bnbo}`
                        }
                    },
                    layers: [
                        // Black basemap - use landuse layer for Denmark land areas
                        {
                            id: 'basemap-fill',
                            type: 'fill',
                            source: 'basemap',
                            'source-layer': 'landuse',
                            paint: {
                                'fill-color': '#000000',
                                'fill-opacity': 1
                            }
                        },
                        // Kommune layer (visible at low zoom) - use actual source-layer name
                        {
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
                        },
                        // Kommune borders
                        {
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
                        },
                        // H3 layer (visible at high zoom)
                        {
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
                        },
                        // H3 borders
                        {
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
                        },
                        // BNBO layer (overlay)
                        {
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
                                    '#cccccc' // fallback
                                ],
                                'fill-opacity': 0.6
                            }
                        },
                        // BNBO borders
                        {
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
            map.current.on('load', ()=>{
                console.log('🎉 Map loaded successfully!');
                // Debug: Check available sources and layers
                const style = map.current.getStyle();
                console.log('📊 Map style sources:', Object.keys(style.sources));
                console.log('📊 Map style layers:', style.layers.map((l)=>({
                        id: l.id,
                        type: l.type,
                        source: l.source,
                        'source-layer': l['source-layer']
                    })));
                // Debug: Try to get source data and inspect tiles
                setTimeout(()=>{
                    if (!map.current) return;
                    console.log(`📊 Current zoom: ${map.current.getZoom()}, center:`, map.current.getCenter());
                    // Try to inspect each source more thoroughly
                    const sources = [
                        'basemap',
                        'kommune',
                        'h3',
                        'bnbo'
                    ];
                    sources.forEach((sourceId)=>{
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
                    });
                    // Also try to get all rendered features at current view
                    try {
                        if (!map.current) return;
                        const allRenderedFeatures = map.current.queryRenderedFeatures();
                        console.log(`📊 Total rendered features in view: ${allRenderedFeatures.length}`);
                        if (allRenderedFeatures.length > 0) {
                            const sourceLayerCounts = allRenderedFeatures.reduce((acc, f)=>{
                                const key = `${f.source}:${f.sourceLayer}`;
                                acc[key] = (acc[key] || 0) + 1;
                                return acc;
                            }, {});
                            console.log(`📊 Rendered features by source:layer:`, sourceLayerCounts);
                        }
                    } catch (e) {
                        console.log(`📊 Could not query rendered features:`, e);
                    }
                }, 3000);
                setMapLoaded(true);
                clearError();
            });
            map.current.on('move', ()=>{
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
            });
            map.current.on('click', (e)=>{
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
            });
            map.current.on('mousemove', (e)=>{
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
            });
            map.current.on('mouseleave', ()=>{
                hideTooltip();
                if (map.current) {
                    map.current.getCanvas().style.cursor = '';
                }
            });
            map.current.on('error', (e)=>{
                console.error('❌ Map error:', e);
                console.error('❌ Map error details:', {
                    type: e.type,
                    error: e.error,
                    sourceId: e.sourceId,
                    tile: e.tile
                });
                setError(`Map loading error: ${e.error?.message || 'Unknown error'}`);
            });
            map.current.on('sourcedata', (e)=>{
                if (e.isSourceLoaded) {
                    console.log(`📊 Source loaded: ${e.sourceId}`, e);
                }
            });
            map.current.on('sourcedataloading', (e)=>{
                console.log(`📊 Source loading: ${e.sourceId}`, e);
            });
        } catch (err) {
            console.error('❌ Error initializing map:', err);
            setError('Failed to initialize map');
        }
        return ()=>{
            if (map.current) {
                map.current.remove();
                map.current = null;
            }
        };
    }, [
        mapLibre,
        pmtilesUrls
    ]);
    // Update layer visibility based on zoom
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!map.current || !mapLoaded) return;
        try {
            const layerVisibility = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["getComputedLayerVisibility"])(zoom);
            // Helper function to safely update layer visibility
            const updateLayerVisibility = (layerId, visible)=>{
                if (map.current && map.current.getLayer(layerId)) {
                    map.current.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
                }
            };
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
    }, [
        zoom,
        showBNBOLayer,
        mapLoaded
    ]);
    // Update paint expressions when data mode changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
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
            setTimeout(()=>{
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
            }, 1000);
        } catch (error) {
            console.warn('Error updating paint expressions:', error);
        }
    }, [
        selectedDataMode,
        currentPropertyName,
        mapLoaded
    ]);
    // Update source-layer names when year or resolution changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
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
            layersToUpdate.forEach((layerId)=>{
                if (map.current && map.current.getLayer(layerId)) {
                    map.current.removeLayer(layerId);
                }
            });
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
    }, [
        selectedYear,
        currentH3Resolution,
        mapLoaded,
        shouldShowKommune,
        shouldShowH3,
        currentPropertyName
    ]);
    // Show loading state
    if (!mapLibre || isLoading) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative flex items-center justify-center bg-gray-900 ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 735,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm",
                        children: "Loading map..."
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 736,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 734,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 733,
            columnNumber: 7
        }, this);
    }
    // Show error state
    if (error) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative flex items-center justify-center bg-gray-900 ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-4xl mb-4",
                        children: "⚠️"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 747,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm mb-2",
                        children: "Map Error"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 748,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 text-xs",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 749,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: clearError,
                        className: "mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 750,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 746,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 745,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `relative ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                ref: mapContainer,
                className: "w-full h-full"
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 763,
                columnNumber: 7
            }, this),
            !mapLoaded && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute inset-0 flex items-center justify-center bg-gray-900/80",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-center",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-2"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 769,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "text-white text-sm",
                            children: "Initializing map..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 770,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 768,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 767,
                columnNumber: 9
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute top-4 left-4 bg-black/80 text-white px-3 py-2 rounded text-sm max-w-xs",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Year: ",
                            selectedYear
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 777,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Mode: ",
                            selectedDataMode
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 778,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Zoom: ",
                            Math.round(zoom * 10) / 10
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 779,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            "Layers: ",
                            shouldShowKommune ? 'Kommune' : shouldShowH3 ? `H3 (${currentH3Resolution})` : 'None'
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 780,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-2 text-xs opacity-75",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: "Sources:"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 782,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• Basemap: ",
                                    pmtilesUrls.basemap ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 783,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• Kommune: ",
                                    pmtilesUrls.kommune ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 784,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• H3: ",
                                    pmtilesUrls.h3 ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 785,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    "• BNBO: ",
                                    pmtilesUrls.bnbo ? '✅' : '❌'
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 786,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                        lineNumber: 781,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 776,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$MapTooltip$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["MapTooltip"], {}, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 791,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 762,
        columnNumber: 5
    }, this);
};
// Error boundary component
const MapErrorFallback = ({ error, resetErrorBoundary })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "flex items-center justify-center h-full bg-gray-900",
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "text-center",
            children: [
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-red-400 text-6xl mb-4",
                    children: "⚠️"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 803,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                    className: "text-white text-xl font-semibold mb-2",
                    children: "Map Error"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 804,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                    className: "text-gray-400 mb-4",
                    children: error.message
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 805,
                    columnNumber: 7
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: resetErrorBoundary,
                    className: "px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors",
                    children: "Reload Map"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 806,
                    columnNumber: 7
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 802,
            columnNumber: 5
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 801,
        columnNumber: 3
    }, this);
const PMTilesMap = (props)=>{
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$react$2d$error$2d$boundary$2f$dist$2f$react$2d$error$2d$boundary$2e$development$2e$esm$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["ErrorBoundary"], {
        FallbackComponent: MapErrorFallback,
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(PMTilesMapInner, {
            ...props
        }, void 0, false, {
            fileName: "[project]/src/components/map/PMTilesMap.tsx",
            lineNumber: 820,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 819,
        columnNumber: 5
    }, this);
};
}}),
"[project]/src/components/controls/DataModeSelector.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "DataModeSelector": (()=>DataModeSelector),
    "default": (()=>__TURBOPACK__default__export__)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-ssr] (ecmascript)");
'use client';
;
;
const ColorScaleLegend = ({ mode })=>{
    const config = __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["DATA_MODE_CONFIG"][mode];
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
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "mt-3 p-3 bg-gray-50 rounded-lg",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "relative h-4 rounded mb-2",
                style: {
                    background: `linear-gradient(to right, ${colorStops.map((stop)=>stop.color).join(', ')})`
                },
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex justify-between text-xs text-gray-600",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "0"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 42,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Low"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 43,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "Medium"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 44,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        children: "High"
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                        lineNumber: 45,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
const DataModeSelector = ({ className = '' })=>{
    const { selectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useDataState"])();
    const { setSelectedDataMode } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])();
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
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `bg-white rounded-lg shadow-lg border border-gray-200 p-4 ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                className: "text-lg font-semibold text-gray-900 mb-4",
                children: "Data Mode"
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 89,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-2",
                children: modes.map((mode)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>setSelectedDataMode(mode.key),
                        className: `w-full text-left p-3 rounded-lg border transition-all duration-200 ${selectedDataMode === mode.key ? 'border-blue-500 bg-blue-50 shadow-sm' : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'}`,
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center justify-between",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "flex-1",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: `font-medium ${mode.color} ${selectedDataMode === mode.key ? 'text-blue-700' : ''}`,
                                            children: mode.label
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                                            lineNumber: 104,
                                            columnNumber: 17
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                                selectedDataMode === mode.key && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "ml-3",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(ColorScaleLegend, {
                mode: selectedDataMode
            }, void 0, false, {
                fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                lineNumber: 125,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-sm text-yellow-800",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "font-medium mb-1",
                            children: "Note:"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/DataModeSelector.tsx",
                            lineNumber: 130,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
const __TURBOPACK__default__export__ = DataModeSelector;
}}),
"[project]/src/components/controls/YearSelector.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "YearSelector": (()=>YearSelector)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-left.js [app-ssr] (ecmascript) <export default as ChevronLeft>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-right.js [app-ssr] (ecmascript) <export default as ChevronRight>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/play.js [app-ssr] (ecmascript) <export default as Play>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/pause.js [app-ssr] (ecmascript) <export default as Pause>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react.js [app-ssr] (ecmascript)");
'use client';
;
;
;
;
function YearSelector({ className = '' }) {
    const selectedYear = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useSelectedYear"])();
    const availableYearOptions = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useAvailableYearOptions"])();
    const { setSelectedYear } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])();
    const [isAnimating, setIsAnimating] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [animationInterval, setAnimationInterval] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    // Filter out 'total' for animation (only animate through numeric years)
    const numericYears = availableYearOptions.filter((year)=>typeof year === 'number');
    const startAnimation = ()=>{
        if (numericYears.length <= 1) return;
        setIsAnimating(true);
        const interval = setInterval(()=>{
            const currentIndex = numericYears.indexOf(selectedYear);
            const nextIndex = currentIndex >= 0 ? (currentIndex + 1) % numericYears.length : 0;
            setSelectedYear(numericYears[nextIndex]);
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
        if (selectedYear === 'total') {
            // If currently on total, go to first numeric year
            if (numericYears.length > 0) {
                setSelectedYear(numericYears[0]);
            }
        } else {
            const currentIndex = numericYears.indexOf(selectedYear);
            if (currentIndex < numericYears.length - 1) {
                setSelectedYear(numericYears[currentIndex + 1]);
            } else {
                // Go to total after last year
                setSelectedYear('total');
            }
        }
    };
    const goToPreviousYear = ()=>{
        if (selectedYear === 'total') {
            // If currently on total, go to last numeric year
            if (numericYears.length > 0) {
                setSelectedYear(numericYears[numericYears.length - 1]);
            }
        } else {
            const currentIndex = numericYears.indexOf(selectedYear);
            if (currentIndex > 0) {
                setSelectedYear(numericYears[currentIndex - 1]);
            }
        }
    };
    const canGoNext = ()=>{
        return availableYearOptions.length > 1;
    };
    const canGoPrevious = ()=>{
        return availableYearOptions.length > 1;
    };
    // Cleanup animation on unmount
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        return ()=>{
            if (animationInterval) {
                clearInterval(animationInterval);
            }
        };
    }, [
        animationInterval
    ]);
    if (availableYearOptions.length === 0) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `${className} flex items-center justify-center`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-gray-400 text-sm",
                children: "Loading years..."
            }, void 0, false, {
                fileName: "[project]/src/components/controls/YearSelector.tsx",
                lineNumber: 94,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/controls/YearSelector.tsx",
            lineNumber: 93,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `${className} space-y-4`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex items-center justify-center space-x-4",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: goToPreviousYear,
                        disabled: !canGoPrevious() || isAnimating,
                        className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__["ChevronLeft"], {
                            className: "w-5 h-5"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                            lineNumber: 108,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 103,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: isAnimating ? stopAnimation : startAnimation,
                        disabled: numericYears.length <= 1,
                        className: "p-2 rounded-lg bg-blue-600 hover:bg-blue-700 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                        children: isAnimating ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__["Pause"], {
                            className: "w-5 h-5"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                            lineNumber: 116,
                            columnNumber: 26
                        }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__["Play"], {
                            className: "w-5 h-5"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                            lineNumber: 116,
                            columnNumber: 58
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 111,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: goToNextYear,
                        disabled: !canGoNext() || isAnimating,
                        className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__["ChevronRight"], {
                            className: "w-5 h-5"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                            lineNumber: 124,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 119,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/YearSelector.tsx",
                lineNumber: 102,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "px-4 py-2 bg-gray-700 rounded-lg inline-block",
                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                        className: "text-lg font-bold",
                        children: selectedYear === 'total' ? 'Total (All Years)' : selectedYear
                    }, void 0, false, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 131,
                        columnNumber: 11
                    }, this)
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/YearSelector.tsx",
                    lineNumber: 130,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/YearSelector.tsx",
                lineNumber: 129,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "space-y-3",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-sm text-gray-400 mb-2",
                                children: "Individual Years"
                            }, void 0, false, {
                                fileName: "[project]/src/components/controls/YearSelector.tsx",
                                lineNumber: 141,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "grid grid-cols-3 gap-2",
                                children: numericYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: ()=>setSelectedYear(year),
                                        disabled: isAnimating,
                                        className: `px-3 py-2 rounded-lg text-sm font-medium transition-all ${selectedYear === year ? 'bg-blue-600 text-white' : 'bg-gray-600 text-gray-300 hover:bg-gray-500'} disabled:opacity-30 disabled:cursor-not-allowed`,
                                        children: year
                                    }, year, false, {
                                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                                        lineNumber: 144,
                                        columnNumber: 15
                                    }, this))
                            }, void 0, false, {
                                fileName: "[project]/src/components/controls/YearSelector.tsx",
                                lineNumber: 142,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 140,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-sm text-gray-400 mb-2",
                                children: "Cumulative Data"
                            }, void 0, false, {
                                fileName: "[project]/src/components/controls/YearSelector.tsx",
                                lineNumber: 162,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: ()=>setSelectedYear('total'),
                                disabled: isAnimating,
                                className: `w-full px-4 py-3 rounded-lg text-sm font-medium transition-all ${selectedYear === 'total' ? 'bg-gradient-to-r from-purple-600 to-blue-600 text-white shadow-lg' : 'bg-gray-600 text-gray-300 hover:bg-gray-500'} disabled:opacity-30 disabled:cursor-not-allowed`,
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "flex items-center justify-center space-x-2",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            children: "Total"
                                        }, void 0, false, {
                                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                                            lineNumber: 173,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                            className: "text-xs opacity-75",
                                            children: [
                                                "(",
                                                numericYears.length > 0 ? `${Math.min(...numericYears)}-${Math.max(...numericYears)}` : 'All Years',
                                                ")"
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/components/controls/YearSelector.tsx",
                                            lineNumber: 174,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/controls/YearSelector.tsx",
                                    lineNumber: 172,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/components/controls/YearSelector.tsx",
                                lineNumber: 163,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/controls/YearSelector.tsx",
                        lineNumber: 161,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/controls/YearSelector.tsx",
                lineNumber: 138,
                columnNumber: 7
            }, this),
            isAnimating && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-xs text-blue-400 animate-pulse",
                    children: "Animating through years..."
                }, void 0, false, {
                    fileName: "[project]/src/components/controls/YearSelector.tsx",
                    lineNumber: 185,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/controls/YearSelector.tsx",
                lineNumber: 184,
                columnNumber: 9
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/controls/YearSelector.tsx",
        lineNumber: 100,
        columnNumber: 5
    }, this);
}
}}),
"[project]/src/app/page.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "default": (()=>Home)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/PMTilesMap.tsx [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/DataModeSelector.tsx [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$YearSelector$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/YearSelector.tsx [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/map-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/settings.js [app-ssr] (ecmascript) <export default as Settings>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Eye$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/eye.js [app-ssr] (ecmascript) <export default as Eye>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2d$off$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__EyeOff$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/eye-off.js [app-ssr] (ecmascript) <export default as EyeOff>");
'use client';
;
;
;
;
;
;
;
;
function Home() {
    const [isInitialized, setIsInitialized] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [isInitializing, setIsInitializing] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(true);
    const [showControls, setShowControls] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(true);
    // Store state
    const { selectedYear, selectedDataMode, availableYearOptions } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useDataState"])();
    const { showBNBOLayer } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useLayerVisibility"])();
    const { error } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useLoadingState"])();
    // Store actions
    const { setAvailableYearOptions, toggleBNBOLayer, setError: mapSetError, clearError: mapClearError } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$map$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useMapStore"])();
    // Initialize the application
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        const initialize = async ()=>{
            try {
                console.log('🚀 Starting initialization...');
                setIsInitializing(true);
                console.log('📡 Getting data availability...');
                const availability = await __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getDataAvailability();
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
        };
        initialize();
    }, [
        setAvailableYearOptions,
        mapSetError,
        mapClearError
    ]);
    // Loading state
    if (isInitializing || !isInitialized) {
        console.log('🔄 Still loading - isInitializing:', isInitializing, 'isInitialized:', isInitialized);
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-gray-900 flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 65,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-lg font-medium",
                        children: "Loading PMTiles Map..."
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 66,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-gray-900 flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center max-w-md mx-auto p-6",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-6xl mb-4",
                        children: "⚠️"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 78,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h2", {
                        className: "text-white text-xl font-semibold mb-2",
                        children: "Something went wrong"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 79,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-gray-400 mb-4",
                        children: error
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 80,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
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
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-gray-900 text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-4 z-50",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "flex items-center justify-between",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                    className: "text-xl font-bold text-white",
                                    children: "Danish Agricultural Pesticide Analysis"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 102,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$YearSelector$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["YearSelector"], {}, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 108,
                                columnNumber: 13
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 107,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "flex items-center space-x-4",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                    onClick: toggleBNBOLayer,
                                    className: `flex items-center space-x-2 px-3 py-2 rounded-lg transition-all ${showBNBOLayer ? 'bg-green-600 hover:bg-green-700' : 'bg-gray-700 hover:bg-gray-600'}`,
                                    children: [
                                        showBNBOLayer ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Eye$3e$__["Eye"], {
                                            className: "w-4 h-4"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 121,
                                            columnNumber: 32
                                        }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$eye$2d$off$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__EyeOff$3e$__["EyeOff"], {
                                            className: "w-4 h-4"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 121,
                                            columnNumber: 62
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                    onClick: ()=>setShowControls(!showControls),
                                    className: "p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-all",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$settings$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Settings$3e$__["Settings"], {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1 flex relative",
                children: [
                    showControls && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-80 bg-gray-800/95 backdrop-blur-sm border-r border-gray-700 overflow-y-auto",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$DataModeSelector$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["DataModeSelector"], {
                                className: "m-4"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 140,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "m-4 p-4 bg-gray-700/50 rounded-lg",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                        className: "text-sm font-semibold text-white mb-3",
                                        children: "Current View"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 144,
                                        columnNumber: 15
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "space-y-2 text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "Year:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 147,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "Data Mode:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 153,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-gray-400",
                                                        children: "BNBO Layer:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 157,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "mt-4 pt-3 border-t border-gray-600",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                className: "text-xs font-semibold text-gray-300 mb-2",
                                                children: "Usage Tips"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 165,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("ul", {
                                                className: "text-xs text-gray-400 space-y-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom out: Municipality-level data"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 167,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Zoom in: H3 cell-level detail"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 168,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Hover for detailed information"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 169,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
                                                        children: "• Click for expanded data view"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 170,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("li", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1 relative",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["PMTilesMap"], {
                                className: "w-full h-full"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 180,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "absolute top-4 right-4 z-40",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-gray-600 text-center",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-medium",
                                                children: "Zoom Level"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 186,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "absolute bottom-4 left-4 z-40",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg max-w-xs",
                                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-gray-800",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "font-medium mb-1",
                                                children: "Layer Switching"
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 196,
                                                columnNumber: 17
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "space-y-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-blue-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 199,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-red-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 203,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                    showBNBOLayer && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center space-x-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "w-3 h-3 bg-green-500 rounded"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 208,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
}}),

};

//# sourceMappingURL=src_91b3871e._.js.map