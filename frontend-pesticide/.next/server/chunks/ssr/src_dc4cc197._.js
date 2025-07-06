module.exports = {

"[project]/src/stores/pmtiles-store.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "usePMTilesStore": (()=>usePMTilesStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-ssr] (ecmascript)");
;
;
const usePMTilesStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
        // Initial state
        metadata: null,
        metadataLoading: false,
        metadataError: null,
        loadedTiles: {},
        // Metadata actions
        setMetadata: (metadata)=>set({
                metadata,
                metadataError: null
            }),
        setMetadataLoading: (loading)=>set({
                metadataLoading: loading
            }),
        setMetadataError: (error)=>set({
                metadataError: error,
                metadataLoading: false
            }),
        // Tile loading actions
        setTileLoading: (key, url)=>set((state)=>({
                    loadedTiles: {
                        ...state.loadedTiles,
                        [key]: {
                            url,
                            loadedAt: Date.now(),
                            status: 'loading'
                        }
                    }
                })),
        setTileLoaded: (key)=>set((state)=>({
                    loadedTiles: {
                        ...state.loadedTiles,
                        [key]: {
                            ...state.loadedTiles[key],
                            status: 'loaded'
                        }
                    }
                })),
        setTileError: (key, error)=>set((state)=>({
                    loadedTiles: {
                        ...state.loadedTiles,
                        [key]: {
                            ...state.loadedTiles[key],
                            status: 'error',
                            error
                        }
                    }
                })),
        // Utility functions
        getTileKey: (year, resolution)=>`h3_${year}_${resolution}`,
        getKommuneTileKey: (year)=>`kommune_${year}`,
        getTileStatus: (year, resolution)=>{
            const key = get().getTileKey(year, resolution);
            const tile = get().loadedTiles[key];
            return tile?.status || 'not-loaded';
        },
        getKommuneTileStatus: (year)=>{
            const key = get().getKommuneTileKey(year);
            const tile = get().loadedTiles[key];
            return tile?.status || 'not-loaded';
        },
        getAvailableYears: ()=>{
            const metadata = get().metadata;
            return metadata?.years || [];
        },
        getAvailableResolutions: ()=>{
            const metadata = get().metadata;
            return metadata?.resolutions || [];
        },
        getAvailableKommuneYears: ()=>{
            const metadata = get().metadata;
            return metadata?.kommune?.years || [];
        },
        isYearResolutionAvailable: (year, resolution)=>{
            const metadata = get().metadata;
            if (!metadata) return false;
            return metadata.years.includes(year) && metadata.resolutions.includes(resolution);
        },
        isKommuneYearAvailable: (year)=>{
            const metadata = get().metadata;
            if (!metadata?.kommune) return false;
            return metadata.kommune.years.includes(year);
        },
        clearOldTiles: ()=>{
            const now = Date.now();
            const maxAge = 24 * 60 * 60 * 1000 // 24 hours
            ;
            set((state)=>{
                const filteredTiles = Object.fromEntries(Object.entries(state.loadedTiles).filter(([, tile])=>now - tile.loadedAt < maxAge));
                return {
                    loadedTiles: filteredTiles
                };
            });
        }
    }), {
    name: 'pmtiles-store',
    partialize: (state)=>({
            metadata: state.metadata,
            loadedTiles: state.loadedTiles
        })
}));
}}),
"[project]/src/stores/resolution-store.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "useResolutionStore": (()=>useResolutionStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-ssr] (ecmascript)");
;
;
const useResolutionStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
        // Initial state
        currentResolution: 10,
        autoResolution: true,
        previousResolution: null,
        currentZoom: 7,
        // Actions
        setResolution: (resolution)=>set((state)=>({
                    currentResolution: resolution,
                    previousResolution: state.currentResolution
                })),
        setAutoResolution: (auto)=>set({
                autoResolution: auto
            }),
        setZoom: (zoom)=>{
            const state = get();
            const newResolution = state.getResolutionForZoom(zoom);
            if (state.autoResolution && newResolution !== state.currentResolution) {
                set({
                    currentZoom: zoom,
                    currentResolution: newResolution,
                    previousResolution: state.currentResolution
                });
            } else {
                set({
                    currentZoom: zoom
                });
            }
        },
        // Utility functions
        getResolutionForZoom: (zoom)=>{
            // Map zoom levels to H3 resolutions - higher zoom = higher resolution
            if (zoom >= 12) return 10 // Field-level detail
            ;
            if (zoom >= 10) return 9 // Municipal detail
            ;
            if (zoom >= 8) return 8 // Sub-regional
            ;
            return 7 // Regional overview
            ;
        },
        shouldUpdateResolution: (newZoom)=>{
            const state = get();
            if (!state.autoResolution) return false;
            const newResolution = state.getResolutionForZoom(newZoom);
            return newResolution !== state.currentResolution;
        },
        getResolutionInfo: (resolution)=>{
            const resolutionInfo = {
                7: {
                    name: 'Regional',
                    description: 'County/regional overview',
                    zoomRange: [
                        4,
                        7
                    ],
                    cellSize: '~5,000 ha'
                },
                8: {
                    name: 'Sub-regional',
                    description: 'Large municipal areas',
                    zoomRange: [
                        8,
                        9
                    ],
                    cellSize: '~700 ha'
                },
                9: {
                    name: 'Municipal',
                    description: 'Municipal/city detail',
                    zoomRange: [
                        10,
                        11
                    ],
                    cellSize: '~100 ha'
                },
                10: {
                    name: 'Field-level',
                    description: 'Individual field analysis',
                    zoomRange: [
                        12,
                        15
                    ],
                    cellSize: '~15 ha'
                }
            };
            return resolutionInfo[resolution] || {
                name: 'Unknown',
                description: 'Unknown resolution',
                zoomRange: [
                    0,
                    15
                ],
                cellSize: 'Unknown'
            };
        }
    }), {
    name: 'resolution-store',
    // Persist resolution preferences
    partialize: (state)=>({
            currentResolution: state.currentResolution,
            autoResolution: state.autoResolution
        })
}));
}}),
"[project]/src/stores/temporal-store.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "useTemporalStore": (()=>useTemporalStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-ssr] (ecmascript)");
;
;
const useTemporalStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
        // Initial state
        currentYear: 2023,
        availableYears: [],
        isAnimating: false,
        animationSpeed: 1000,
        animationDirection: 'forward',
        comparisonMode: false,
        comparisonYear: null,
        cumulativeMode: false,
        cumulativeStartYear: null,
        animationInterval: null,
        // Basic actions
        setCurrentYear: (year)=>set({
                currentYear: year
            }),
        setAvailableYears: (years)=>set({
                availableYears: years
            }),
        // Animation controls
        startAnimation: ()=>{
            const state = get();
            if (state.isAnimating) return;
            set({
                isAnimating: true
            });
            const interval = setInterval(()=>{
                const currentState = get();
                if (!currentState.isAnimating) {
                    clearInterval(interval);
                    return;
                }
                if (currentState.animationDirection === 'forward') {
                    if (currentState.canGoNext()) {
                        currentState.goToNextYear();
                    } else {
                        // Loop back to first year
                        currentState.goToFirstYear();
                    }
                } else {
                    if (currentState.canGoPrevious()) {
                        currentState.goToPreviousYear();
                    } else {
                        // Loop back to last year
                        currentState.goToLastYear();
                    }
                }
            }, state.animationSpeed);
            set({
                animationInterval: interval
            });
        },
        stopAnimation: ()=>{
            const state = get();
            if (state.animationInterval) {
                clearInterval(state.animationInterval);
            }
            set({
                isAnimating: false,
                animationInterval: null
            });
        },
        setAnimationSpeed: (speed)=>set({
                animationSpeed: speed
            }),
        setAnimationDirection: (direction)=>set({
                animationDirection: direction
            }),
        // Comparison mode
        setComparisonMode: (enabled)=>set({
                comparisonMode: enabled,
                comparisonYear: enabled ? get().currentYear : null
            }),
        setComparisonYear: (year)=>set({
                comparisonYear: year
            }),
        // Cumulative mode
        setCumulativeMode: (enabled)=>set({
                cumulativeMode: enabled,
                cumulativeStartYear: enabled ? get().availableYears[0] : null
            }),
        setCumulativeStartYear: (year)=>set({
                cumulativeStartYear: year
            }),
        // Navigation
        goToNextYear: ()=>{
            const state = get();
            const currentIndex = state.getYearIndex(state.currentYear);
            if (currentIndex < state.availableYears.length - 1) {
                set({
                    currentYear: state.availableYears[currentIndex + 1]
                });
            }
        },
        goToPreviousYear: ()=>{
            const state = get();
            const currentIndex = state.getYearIndex(state.currentYear);
            if (currentIndex > 0) {
                set({
                    currentYear: state.availableYears[currentIndex - 1]
                });
            }
        },
        goToFirstYear: ()=>{
            const state = get();
            if (state.availableYears.length > 0) {
                set({
                    currentYear: state.availableYears[0]
                });
            }
        },
        goToLastYear: ()=>{
            const state = get();
            if (state.availableYears.length > 0) {
                set({
                    currentYear: state.availableYears[state.availableYears.length - 1]
                });
            }
        },
        // Utility functions
        getYearIndex: (year)=>{
            const state = get();
            return state.availableYears.indexOf(year);
        },
        canGoNext: ()=>{
            const state = get();
            const currentIndex = state.getYearIndex(state.currentYear);
            return currentIndex < state.availableYears.length - 1;
        },
        canGoPrevious: ()=>{
            const state = get();
            const currentIndex = state.getYearIndex(state.currentYear);
            return currentIndex > 0;
        },
        getYearRange: ()=>{
            const state = get();
            if (state.availableYears.length === 0) return null;
            const sortedYears = [
                ...state.availableYears
            ].sort((a, b)=>a - b);
            return [
                sortedYears[0],
                sortedYears[sortedYears.length - 1]
            ];
        }
    }), {
    name: 'temporal-store',
    // Persist temporal preferences
    partialize: (state)=>({
            currentYear: state.currentYear,
            animationSpeed: state.animationSpeed,
            animationDirection: state.animationDirection,
            comparisonMode: state.comparisonMode,
            comparisonYear: state.comparisonYear,
            cumulativeMode: state.cumulativeMode,
            cumulativeStartYear: state.cumulativeStartYear
        })
}));
}}),
"[project]/src/stores/ui-store.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "useUIStore": (()=>useUIStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-ssr] (ecmascript)");
;
;
const useUIStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["persist"])((set)=>({
        // Initial state
        sidebarOpen: false,
        theme: 'system',
        isMobile: false,
        showDataPanel: false,
        reduceMotion: false,
        performanceMode: false,
        // Actions
        setSidebarOpen: (open)=>set({
                sidebarOpen: open
            }),
        setTheme: (theme)=>set({
                theme
            }),
        setIsMobile: (isMobile)=>set({
                isMobile
            }),
        setShowDataPanel: (show)=>set({
                showDataPanel: show
            }),
        setReduceMotion: (reduce)=>set({
                reduceMotion: reduce
            }),
        setPerformanceMode: (enabled)=>set({
                performanceMode: enabled
            })
    }), {
    name: 'ui-store',
    partialize: (state)=>({
            theme: state.theme,
            showDataPanel: state.showDataPanel,
            reduceMotion: state.reduceMotion,
            performanceMode: state.performanceMode
        })
}));
}}),
"[project]/src/lib/basemaps.ts [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "BASEMAPS": (()=>BASEMAPS),
    "getBasemapForTheme": (()=>getBasemapForTheme),
    "getBasemapStyleForTheme": (()=>getBasemapStyleForTheme),
    "getDenmarkOptimizedStyle": (()=>getDenmarkOptimizedStyle)
});
// Denmark bounding box for optimized basemap coverage
const DENMARK_BOUNDS = {
    west: 7.5,
    south: 54.5,
    east: 15.5,
    north: 57.8
};
// Denmark PMTiles URL on GCS
const DENMARK_PMTILES_URL = 'pmtiles://https://storage.googleapis.com/landbrugsdata-raw-data/pmtiles/protomaps_denmark.pmtiles';
// Create basemap styles using the actual Denmark PMTiles
function createDenmarkBasemapStyle(flavor) {
    const isDark = flavor === 'black';
    return {
        version: 8,
        name: `Denmark ${flavor}`,
        glyphs: 'https://fonts.openmaptiles.org/{fontstack}/{range}.pbf',
        sources: {
            'protomaps-denmark': {
                type: 'vector',
                url: DENMARK_PMTILES_URL,
                attribution: '© Protomaps © OpenStreetMap'
            }
        },
        layers: [
            {
                id: 'background',
                type: 'background',
                paint: {
                    'background-color': isDark ? '#000000' : '#f8f9fa'
                }
            },
            // Water features
            {
                id: 'water',
                type: 'fill',
                source: 'protomaps-denmark',
                'source-layer': 'water',
                paint: {
                    'fill-color': isDark ? '#1a1a1a' : '#a8d8ff',
                    'fill-opacity': 0.8
                }
            },
            // Land features
            {
                id: 'land',
                type: 'fill',
                source: 'protomaps-denmark',
                'source-layer': 'land',
                paint: {
                    'fill-color': isDark ? '#0a0a0a' : '#f0f0f0',
                    'fill-opacity': 1
                }
            },
            // Roads for context (minimal styling)
            {
                id: 'roads',
                type: 'line',
                source: 'protomaps-denmark',
                'source-layer': 'roads',
                filter: [
                    'in',
                    'pmap:kind',
                    'highway',
                    'major_road'
                ],
                paint: {
                    'line-color': isDark ? '#333333' : '#cccccc',
                    'line-width': {
                        base: 1.2,
                        stops: [
                            [
                                6,
                                0.5
                            ],
                            [
                                20,
                                30
                            ]
                        ]
                    },
                    'line-opacity': isDark ? 0.6 : 0.8
                }
            },
            // Places for minimal labeling
            {
                id: 'places',
                type: 'symbol',
                source: 'protomaps-denmark',
                'source-layer': 'places',
                filter: [
                    'in',
                    'pmap:kind',
                    'city',
                    'town'
                ],
                layout: {
                    'text-field': [
                        'get',
                        'name'
                    ],
                    'text-font': [
                        'Noto Sans Regular'
                    ],
                    'text-size': {
                        base: 1.2,
                        stops: [
                            [
                                7,
                                12
                            ],
                            [
                                11,
                                16
                            ]
                        ]
                    }
                },
                paint: {
                    'text-color': isDark ? '#ffffff' : '#333333',
                    'text-halo-color': isDark ? '#000000' : '#ffffff',
                    'text-halo-width': 1
                }
            }
        ]
    };
}
const BASEMAPS = {
    light: {
        id: 'light',
        name: 'Light',
        style: createDenmarkBasemapStyle('light'),
        attribution: '© Protomaps © OpenStreetMap',
        flavor: 'light'
    },
    dark: {
        id: 'dark',
        name: 'Dark',
        style: createDenmarkBasemapStyle('black'),
        attribution: '© Protomaps © OpenStreetMap',
        flavor: 'black'
    }
};
function getBasemapForTheme(theme) {
    if (theme === 'system') {
        // Check system preference
        const isDark = "undefined" !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches;
        return ("TURBOPACK compile-time falsy", 0) ? ("TURBOPACK unreachable", undefined) : BASEMAPS.light;
    }
    return BASEMAPS[theme] || BASEMAPS.light;
}
function getBasemapStyleForTheme(theme) {
    return getBasemapForTheme(theme).style;
}
function getDenmarkOptimizedStyle(theme) {
    const baseStyle = getBasemapStyleForTheme(theme);
    // Enhance the style for better data visualization
    return {
        ...baseStyle,
        center: [
            10.0,
            56.0
        ],
        zoom: 7,
        bounds: [
            [
                DENMARK_BOUNDS.west,
                DENMARK_BOUNDS.south
            ],
            [
                DENMARK_BOUNDS.east,
                DENMARK_BOUNDS.north
            ]
        ]
    };
}
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
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/pmtiles/dist/index.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/pmtiles-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/resolution-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/temporal-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/ui-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$basemaps$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/basemaps.ts [app-ssr] (ecmascript)");
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
function PMTilesMap({ className = '', pmtilesBaseUrl = '/api/pmtiles', initialYear = 2023, initialResolution = 10, showBNBO = false, bnboOpacity = 0.4, showKommune = false, kommuneOpacity = 0.6, onBNBOToggle, onKommuneToggle, activeDataLayer = 'pfas' }) {
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const map = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const [isLoaded, setIsLoaded] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [hoverInfo, setHoverInfo] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    const [bnboHoverInfo, setBNBOHoverInfo] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    const [kommuneHoverInfo, setKommuneHoverInfo] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    const [mousePosition, setMousePosition] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])({
        x: 0,
        y: 0
    });
    const [currentZoom, setCurrentZoom] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(7);
    // Store hooks
    const { setTileLoading, setTileLoaded, setTileError, getTileKey, getKommuneTileKey, getTileStatus, getKommuneTileStatus } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["usePMTilesStore"])();
    const { currentResolution, setResolution, setZoom, getResolutionForZoom } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useResolutionStore"])();
    const { currentYear, setCurrentYear } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useTemporalStore"])();
    const { theme } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useUIStore"])();
    // Determine if we should use dark mode
    const isDarkMode = theme === 'dark' || theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches;
    // Auto-show municipality layer when zoomed out
    const shouldShowKommune = showKommune || currentZoom <= 8;
    // Initialize map with PMTiles protocol
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!mapContainer.current || map.current) return;
        // Register PMTiles protocol
        const protocol = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["Protocol"]();
        __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].addProtocol('pmtiles', protocol.tile);
        // Create map with theme-based style
        map.current = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].Map({
            container: mapContainer.current,
            style: (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$basemaps$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["getBasemapStyleForTheme"])(theme),
            center: [
                10.0,
                56.0
            ],
            zoom: 7,
            minZoom: 4,
            maxZoom: 15
        });
        // Add controls
        map.current.addControl(new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].NavigationControl(), 'top-right');
        map.current.addControl(new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].ScaleControl(), 'bottom-left');
        map.current.on('load', ()=>{
            setIsLoaded(true);
        });
        // Auto-resolution based on zoom
        map.current.on('zoom', ()=>{
            if (!map.current) return;
            const zoom = map.current.getZoom();
            setZoom(zoom);
            setCurrentZoom(zoom);
        });
        // Track mouse position for hover tooltip
        map.current.on('mousemove', (e)=>{
            setMousePosition({
                x: e.point.x,
                y: e.point.y
            });
        });
        // Cleanup
        return ()=>{
            if (map.current) {
                map.current.remove();
                map.current = null;
            }
            __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].removeProtocol('pmtiles');
        };
    }, [
        setZoom
    ]);
    // Update map style when theme changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!map.current || !isLoaded) return;
        // Get new style and preserve existing data sources/layers
        const currentStyle = map.current.getStyle();
        const newBaseStyle = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$basemaps$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["getBasemapStyleForTheme"])(theme);
        // Preserve existing data sources
        const dataSources = Object.fromEntries(Object.entries(currentStyle.sources || {}).filter(([key])=>key.includes('h3-') || key.includes('bnbo-')));
        // Preserve existing data layers
        const dataLayers = Object.values(currentStyle.layers || []).filter((layer)=>layer.id.includes('h3-') || layer.id.includes('bnbo-'));
        const updatedStyle = {
            ...newBaseStyle,
            sources: {
                ...newBaseStyle.sources,
                ...dataSources
            },
            layers: [
                ...newBaseStyle.layers,
                ...dataLayers
            ]
        };
        map.current.setStyle(updatedStyle);
    }, [
        theme,
        isLoaded
    ]);
    // Update PMTiles layer when year, resolution, or data layer changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!map.current || !isLoaded) return;
        // Only update H3 layers when not showing municipality layer
        if (!(showKommune || currentZoom <= 8)) {
            updatePMTilesLayer(currentYear, currentResolution, activeDataLayer);
        }
    }, [
        currentYear,
        currentResolution,
        activeDataLayer,
        isLoaded,
        pmtilesBaseUrl,
        currentZoom,
        showKommune
    ]);
    // Update BNBO layer when visibility or opacity changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!map.current || !isLoaded) return;
        updateBNBOLayer(showBNBO, bnboOpacity);
    }, [
        showBNBO,
        bnboOpacity,
        isLoaded,
        pmtilesBaseUrl
    ]);
    // Update Kommune layer when visibility, opacity, or data layer changes
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!map.current || !isLoaded) return;
        updateKommuneLayer(shouldShowKommune, kommuneOpacity, currentYear);
    }, [
        shouldShowKommune,
        kommuneOpacity,
        currentYear,
        activeDataLayer,
        isLoaded,
        pmtilesBaseUrl
    ]);
    function getDataLayerColorExpression(dataLayer) {
        console.log('Switching to data layer:', dataLayer); // Debug log
        // Get the intensity field name for the current data layer (grams per hectare)
        const getIntensityFieldName = (layer)=>{
            switch(layer){
                case 'pfas':
                    return 'pfas_intensity';
                case 'total_pesticide':
                    return 'pesticide_intensity';
                case 'diquat':
                    return 'diquat_intensity';
                case 'glyphosate':
                    return 'glyphosate_intensity';
                default:
                    return 'pfas_intensity';
            }
        };
        const intensityFieldName = getIntensityFieldName(dataLayer);
        // Different color scales based on data layer to handle different intensity ranges
        if (dataLayer === 'diquat' || dataLayer === 'glyphosate') {
            // More sensitive scale for diquat and glyphosate (typically lower values)
            return [
                'case',
                [
                    '<=',
                    [
                        'get',
                        intensityFieldName
                    ],
                    0
                ],
                'rgba(0, 0, 0, 0)',
                [
                    'step',
                    [
                        'get',
                        intensityFieldName
                    ],
                    '#fef2f2',
                    0.001,
                    '#fecaca',
                    0.01,
                    '#f87171',
                    0.05,
                    '#ef4444',
                    0.1,
                    '#dc2626',
                    0.5,
                    '#b91c1c',
                    1.0,
                    '#991b1b',
                    2.0,
                    '#7f1d1d',
                    5.0,
                    '#450a0a' // Almost black red (5+ g/ha)
                ]
            ];
        } else {
            // Standard scale for PFAS and total pesticides (typically higher values)
            return [
                'case',
                [
                    '<=',
                    [
                        'get',
                        intensityFieldName
                    ],
                    0
                ],
                'rgba(0, 0, 0, 0)',
                [
                    'step',
                    [
                        'get',
                        intensityFieldName
                    ],
                    '#fef2f2',
                    0.01,
                    '#fecaca',
                    0.1,
                    '#f87171',
                    0.5,
                    '#ef4444',
                    1.0,
                    '#dc2626',
                    2.0,
                    '#b91c1c',
                    5.0,
                    '#991b1b',
                    10.0,
                    '#7f1d1d',
                    20.0,
                    '#450a0a' // Almost black red (20+ g/ha)
                ]
            ];
        }
    }
    function getKommuneColorExpression(dataLayer) {
        // Municipality data uses absolute grams (not per hectare) due to aggregation across entire municipality
        const getKommuneFieldName = (layer)=>{
            switch(layer){
                case 'pfas':
                    return 'pfas_grams';
                case 'total_pesticide':
                    return 'pesticide_load';
                case 'diquat':
                    return 'diquat_grams';
                case 'glyphosate':
                    return 'glyphosate_grams';
                default:
                    return 'pfas_grams';
            }
        };
        const fieldName = getKommuneFieldName(dataLayer);
        // Different scales for municipality data based on typical ranges
        if (dataLayer === 'diquat' || dataLayer === 'glyphosate') {
            // More sensitive scale for diquat and glyphosate at municipality level
            return [
                'case',
                [
                    '<=',
                    [
                        'get',
                        fieldName
                    ],
                    0
                ],
                'rgba(0, 0, 0, 0)',
                [
                    'step',
                    [
                        'get',
                        fieldName
                    ],
                    '#fef2f2',
                    0.01,
                    '#fecaca',
                    1,
                    '#f87171',
                    10,
                    '#ef4444',
                    100,
                    '#dc2626',
                    1000,
                    '#b91c1c',
                    10000,
                    '#991b1b',
                    100000,
                    '#7f1d1d',
                    1000000,
                    '#450a0a' // Black red for extreme values
                ]
            ];
        } else {
            // Standard scale for PFAS and total pesticides at municipality level
            return [
                'case',
                [
                    '<=',
                    [
                        'get',
                        fieldName
                    ],
                    0
                ],
                'rgba(0, 0, 0, 0)',
                [
                    'step',
                    [
                        'get',
                        fieldName
                    ],
                    '#fef2f2',
                    0.1,
                    '#fecaca',
                    100,
                    '#f87171',
                    1000,
                    '#ef4444',
                    10000,
                    '#dc2626',
                    100000,
                    '#b91c1c',
                    1000000,
                    '#991b1b',
                    5000000,
                    '#7f1d1d',
                    10000000,
                    '#450a0a' // Black red for extreme values
                ]
            ];
        }
    }
    function updatePMTilesLayer(year, resolution, dataLayer) {
        if (!map.current) return;
        console.log('Updating PMTiles layer:', {
            year,
            resolution,
            dataLayer
        }); // Debug log
        // Check if municipality layer should be visible (zoom <= 8)
        const shouldShowKommune = showKommune || currentZoom <= 8;
        // Hide low resolution H3 layers (7-8) when municipality layer should be visible
        if (shouldShowKommune && resolution <= 8) {
            console.log(`Skipping H3 resolution ${resolution} because municipality layer should be visible at zoom ${currentZoom}`);
            // Remove any existing H3 layers when municipality should be visible
            const layerId = 'h3-pfas';
            const sourceId = 'h3-pfas-source';
            if (map.current.getLayer(layerId)) {
                map.current.removeLayer(layerId);
                console.log('Removed H3 layer for municipality visibility');
            }
            if (map.current.getLayer(`${layerId}-outline`)) {
                map.current.removeLayer(`${layerId}-outline`);
                console.log('Removed H3 outline layer for municipality visibility');
            }
            if (map.current.getSource(sourceId)) {
                map.current.removeSource(sourceId);
                console.log('Removed H3 source for municipality visibility');
            }
            return;
        }
        const layerId = 'h3-pfas';
        const sourceId = 'h3-pfas-source';
        const tileKey = getTileKey(year, resolution);
        // Get intensity field name for current data layer (grams per hectare)
        const getIntensityFieldName = (layer)=>{
            switch(layer){
                case 'pfas':
                    return 'pfas_intensity';
                case 'total_pesticide':
                    return 'pesticide_intensity';
                case 'diquat':
                    return 'diquat_intensity';
                case 'glyphosate':
                    return 'glyphosate_intensity';
                default:
                    return 'pfas_intensity';
            }
        };
        const intensityFieldName = getIntensityFieldName(dataLayer);
        // Remove existing layer and source
        if (map.current.getLayer(layerId)) {
            map.current.removeLayer(layerId);
        }
        if (map.current.getLayer(`${layerId}-outline`)) {
            map.current.removeLayer(`${layerId}-outline`);
        }
        if (map.current.getSource(sourceId)) {
            map.current.removeSource(sourceId);
        }
        // Add new PMTiles source
        const pmtilesUrl = `pmtiles://${pmtilesBaseUrl}/h3_pfas_${year}_res${resolution}.pmtiles`;
        // Track loading state
        setTileLoading(tileKey, pmtilesUrl);
        try {
            map.current.addSource(sourceId, {
                type: 'vector',
                url: pmtilesUrl
            });
            // Add fill layer with resolution-aware styling
            map.current.addLayer({
                id: layerId,
                type: 'fill',
                source: sourceId,
                'source-layer': `h3_pfas_${year}_res${resolution}`,
                paint: {
                    'fill-color': getDataLayerColorExpression(dataLayer),
                    'fill-opacity': 0.8
                }
            });
            // Add outline with resolution-aware width
            map.current.addLayer({
                id: `${layerId}-outline`,
                type: 'line',
                source: sourceId,
                'source-layer': `h3_pfas_${year}_res${resolution}`,
                paint: {
                    'line-color': '#64748b',
                    'line-width': resolution <= 8 ? 0.8 : 0.4,
                    'line-opacity': 0.4
                }
            });
            // Enhanced hover functionality
            map.current.on('mouseenter', layerId, (e)=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = 'pointer';
                }
                if (e.features && e.features.length > 0) {
                    const feature = e.features[0];
                    const props = feature.properties;
                    if (props) {
                        setHoverInfo({
                            h3_id: props.h3_id || '',
                            year: props.year || year,
                            resolution: props.resolution || resolution,
                            pfas_grams: props.pfas_grams || 0,
                            pesticide_load: props.pesticide_load || 0,
                            diquat_grams: props.diquat_grams || 0,
                            glyphosate_grams: props.glyphosate_grams || 0,
                            applications: props.applications || 0,
                            pfas_applications: props.pfas_applications || 0,
                            diquat_applications: props.diquat_applications || 0,
                            glyphosate_applications: props.glyphosate_applications || 0,
                            field_count: props.field_count || 0,
                            coverage: props.coverage || 0,
                            area_ha: props.area_ha || 0,
                            pfas_intensity: props.pfas_intensity || 0,
                            pesticide_intensity: props.pesticide_intensity || 0,
                            diquat_intensity: props.diquat_intensity || 0,
                            glyphosate_intensity: props.glyphosate_intensity || 0
                        });
                    }
                }
            });
            map.current.on('mouseleave', layerId, ()=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = '';
                }
                setHoverInfo(null);
            });
            // Remove click handler - we only want hover tooltips
            setTileLoaded(tileKey);
        } catch (error) {
            console.error('Failed to load PMTiles:', error);
            setTileError(tileKey, error instanceof Error ? error.message : 'Unknown error');
        }
    }
    function updateBNBOLayer(visible, opacity) {
        if (!map.current) return;
        const layerId = 'bnbo-areas';
        const sourceId = 'bnbo-areas-source';
        // Remove existing layer and source
        if (map.current.getLayer(layerId)) {
            map.current.removeLayer(layerId);
        }
        if (map.current.getLayer(`${layerId}-outline`)) {
            map.current.removeLayer(`${layerId}-outline`);
        }
        if (map.current.getSource(sourceId)) {
            map.current.removeSource(sourceId);
        }
        if (!visible) return;
        // Add BNBO PMTiles source - use direct GCS URL
        const pmtilesUrl = `pmtiles://https://storage.googleapis.com/landbrugsdata-raw-data/pmtiles/bnbo_areas.pmtiles`;
        try {
            map.current.addSource(sourceId, {
                type: 'vector',
                url: pmtilesUrl
            });
            // Add fill layer with status-based styling
            map.current.addLayer({
                id: layerId,
                type: 'fill',
                source: sourceId,
                'source-layer': 'bnbo_areas',
                paint: {
                    'fill-color': [
                        'case',
                        [
                            '==',
                            [
                                'get',
                                'status_code'
                            ],
                            'action_required'
                        ],
                        '#ff6b6b',
                        [
                            '==',
                            [
                                'get',
                                'status_code'
                            ],
                            'completed'
                        ],
                        '#51cf66',
                        '#868e96' // unknown
                    ],
                    'fill-opacity': opacity
                }
            });
            // Add outline
            map.current.addLayer({
                id: `${layerId}-outline`,
                type: 'line',
                source: sourceId,
                'source-layer': 'bnbo_areas',
                paint: {
                    'line-color': [
                        'case',
                        [
                            '==',
                            [
                                'get',
                                'status_code'
                            ],
                            'action_required'
                        ],
                        '#e03131',
                        [
                            '==',
                            [
                                'get',
                                'status_code'
                            ],
                            'completed'
                        ],
                        '#37b24d',
                        '#495057' // unknown
                    ],
                    'line-width': 1.5,
                    'line-opacity': 0.8
                }
            });
            // BNBO hover functionality
            map.current.on('mouseenter', layerId, (e)=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = 'pointer';
                }
                if (e.features && e.features.length > 0) {
                    const feature = e.features[0];
                    const props = feature.properties;
                    if (props) {
                        setBNBOHoverInfo({
                            bnbo_id: props.bnbo_id || '',
                            status_code: props.status_code || 'unknown',
                            status_description: props.status_description || 'Unknown',
                            area_ha: props.area_ha || 0
                        });
                    }
                }
            });
            map.current.on('mouseleave', layerId, ()=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = '';
                }
                setBNBOHoverInfo(null);
            });
            // BNBO click handler
            map.current.on('click', layerId, (e)=>{
                if (!e.features || e.features.length === 0) return;
                const feature = e.features[0];
                const properties = feature.properties;
                if (!properties) return;
                // Create detailed popup
                const popup = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].Popup({
                    closeButton: true,
                    closeOnClick: true,
                    maxWidth: '350px'
                });
                const statusColor = properties.status_code === 'action_required' ? '#ff6b6b' : properties.status_code === 'completed' ? '#51cf66' : '#868e96';
                const popupContent = `
          <div class="p-4">
            <div class="border-b pb-2 mb-3">
              <h3 class="font-bold text-lg text-gray-900">BNBO Area</h3>
              <p class="text-sm text-gray-600">Protected Environmental Area</p>
            </div>
            
            <div class="space-y-3 text-sm">
              <div>
                <span class="font-medium text-gray-700">BNBO ID:</span>
                <p class="text-gray-900 font-mono">${properties.bnbo_id || 'N/A'}</p>
              </div>
              <div>
                <span class="font-medium text-gray-700">Area:</span>
                <p class="text-gray-900">${(properties.area_ha || 0).toFixed(2)} ha</p>
              </div>
              <div>
                <span class="font-medium text-gray-700">Status:</span>
                <div class="flex items-center space-x-2 mt-1">
                  <div class="w-3 h-3 rounded-full" style="background-color: ${statusColor}"></div>
                  <span class="text-gray-900">${properties.status_description || 'Unknown'}</span>
                </div>
              </div>
            </div>
          </div>
        `;
                popup.setLngLat(e.lngLat).setHTML(popupContent).addTo(map.current);
            });
        } catch (error) {
            console.error('Failed to load BNBO PMTiles:', error);
        }
    }
    function updateKommuneLayer(visible, opacity, year) {
        if (!map.current) return;
        console.log('updateKommuneLayer called:', {
            visible,
            opacity,
            year,
            currentZoom
        }); // Debug log
        const layerId = 'kommune-areas';
        const sourceId = 'kommune-areas-source';
        // Remove existing layer and source
        if (map.current.getLayer(layerId)) {
            map.current.removeLayer(layerId);
            console.log('Removed existing kommune layer'); // Debug log
        }
        if (map.current.getLayer(`${layerId}-outline`)) {
            map.current.removeLayer(`${layerId}-outline`);
            console.log('Removed existing kommune outline layer'); // Debug log
        }
        if (map.current.getSource(sourceId)) {
            map.current.removeSource(sourceId);
            console.log('Removed existing kommune source'); // Debug log
        }
        if (!visible) {
            console.log('Kommune layer not visible, returning'); // Debug log
            return;
        }
        // Add Kommune PMTiles source
        const pmtilesUrl = `pmtiles://${pmtilesBaseUrl}/kommune_pfas_${year}.pmtiles`;
        const tileKey = getKommuneTileKey(year);
        console.log('Adding kommune source:', pmtilesUrl);
        // Track loading state
        setTileLoading(tileKey, pmtilesUrl);
        try {
            map.current.addSource(sourceId, {
                type: 'vector',
                url: pmtilesUrl
            });
            console.log('Added kommune source successfully'); // Debug log
            // Debug: Check what layers are available in the source
            setTimeout(()=>{
                if (map.current && map.current.getSource(sourceId)) {
                    const source = map.current.getSource(sourceId);
                    console.log('Kommune source details:', source);
                    // Try to get source data to see available layers
                    map.current.on('sourcedata', (e)=>{
                        if (e.sourceId === sourceId && e.isSourceLoaded) {
                            console.log('Kommune source loaded, checking layers...');
                        }
                    });
                }
            }, 1000);
            // Find the first H3 layer to insert the kommune layer before it
            let beforeLayerId = undefined;
            const layers = map.current.getStyle().layers || [];
            for (const layer of layers){
                if (layer.id.startsWith('h3-pfas')) {
                    beforeLayerId = layer.id;
                    break;
                }
            }
            // Add the municipality fill layer BEFORE any H3 layers (should be Polygon geometry)
            map.current.addLayer({
                id: layerId,
                type: 'fill',
                source: sourceId,
                'source-layer': `kommune_pfas_${year}`,
                paint: {
                    'fill-color': getKommuneColorExpression(activeDataLayer),
                    'fill-opacity': opacity
                },
                layout: {
                    'visibility': 'visible' // Force visibility
                }
            }, beforeLayerId);
            console.log('Added kommune fill layer successfully with source-layer: kommune_pfas_' + year, 'before layer:', beforeLayerId); // Debug log
            // Add outline with municipality boundaries BEFORE any H3 layers
            map.current.addLayer({
                id: `${layerId}-outline`,
                type: 'line',
                source: sourceId,
                'source-layer': `kommune_pfas_${year}`,
                paint: {
                    'line-color': '#00ff00',
                    'line-width': 3,
                    'line-opacity': 1.0 // Full opacity for debugging
                },
                layout: {
                    'visibility': 'visible' // Force visibility
                }
            }, beforeLayerId);
            console.log('Added kommune outline layer successfully'); // Debug log
            // Debug: List all layers to see the current layer stack
            setTimeout(()=>{
                if (map.current) {
                    const layers = map.current.getStyle().layers || [];
                    console.log('Current map layers:', layers.map((l)=>l.id));
                    console.log('Kommune layer visibility:', map.current.getLayoutProperty(layerId, 'visibility'));
                    console.log('Kommune outline layer visibility:', map.current.getLayoutProperty(`${layerId}-outline`, 'visibility'));
                    // Check if layers exist
                    const kommuneLayer = map.current.getLayer(layerId);
                    const kommuneOutlineLayer = map.current.getLayer(`${layerId}-outline`);
                    console.log('Kommune layer exists:', !!kommuneLayer);
                    console.log('Kommune outline layer exists:', !!kommuneOutlineLayer);
                    // Check paint properties
                    if (kommuneLayer) {
                        console.log('Kommune fill color:', map.current.getPaintProperty(layerId, 'fill-color'));
                        console.log('Kommune fill opacity:', map.current.getPaintProperty(layerId, 'fill-opacity'));
                    }
                    if (kommuneOutlineLayer) {
                        console.log('Kommune line color:', map.current.getPaintProperty(`${layerId}-outline`, 'line-color'));
                        console.log('Kommune line width:', map.current.getPaintProperty(`${layerId}-outline`, 'line-width'));
                    }
                }
            }, 500);
            // Debug: Add a sourcedata listener to see when data loads
            map.current.on('sourcedata', (e)=>{
                if (e.sourceId === sourceId && e.isSourceLoaded) {
                    console.log('Kommune source data loaded successfully');
                    // Query the source to see what features are available
                    setTimeout(()=>{
                        if (map.current) {
                            const features = map.current.querySourceFeatures(sourceId, {
                                sourceLayer: `kommune_pfas_${year}`
                            });
                            console.log('Kommune features found:', features.length);
                            if (features.length > 0) {
                                console.log('Sample kommune feature:', features[0]);
                                console.log('Sample kommune properties:', features[0].properties);
                                // Check all features for geometry types
                                const geometryTypes = new Set();
                                features.slice(0, 10).forEach((f, i)=>{
                                    geometryTypes.add(f.geometry?.type);
                                    console.log(`Feature ${i} geometry type:`, f.geometry?.type);
                                    if (f.geometry?.type === 'Polygon' || f.geometry?.type === 'MultiPolygon') {
                                        console.log(`Feature ${i} has polygon geometry - should be visible!`);
                                    }
                                });
                                console.log('All geometry types found:', Array.from(geometryTypes));
                                // Check if any features have visible geometry in current viewport
                                const bounds = map.current.getBounds();
                                console.log('Current map bounds:', bounds);
                                console.log('Map bounds SW:', bounds.getSouthWest());
                                console.log('Map bounds NE:', bounds.getNorthEast());
                                console.log('Sample feature geometry type:', features[0].geometry?.type);
                                // Get feature bounds to see if they overlap with map viewport
                                const feature = features[0];
                                if (feature.geometry) {
                                    console.log('Sample feature geometry type:', feature.geometry.type);
                                    // Try to manually check if we're in Denmark area
                                    const denmarkBounds = {
                                        west: 8.0,
                                        east: 15.2,
                                        south: 54.5,
                                        north: 57.8
                                    };
                                    const mapSW = bounds.getSouthWest();
                                    const mapNE = bounds.getNorthEast();
                                    const mapInDenmark = mapSW.lng >= denmarkBounds.west && mapNE.lng <= denmarkBounds.east && mapSW.lat >= denmarkBounds.south && mapNE.lat <= denmarkBounds.north;
                                    console.log('Map viewport overlaps Denmark bounds:', mapInDenmark);
                                    console.log('Map center:', map.current.getCenter());
                                    // Test: Try to fit bounds to show all kommune features
                                    if (!mapInDenmark) {
                                        console.log('Map not in Denmark bounds, trying to fit to features...');
                                        setTimeout(()=>{
                                            if (map.current) {
                                                // Try to fit to Denmark bounds
                                                map.current.fitBounds([
                                                    [
                                                        denmarkBounds.west,
                                                        denmarkBounds.south
                                                    ],
                                                    [
                                                        denmarkBounds.east,
                                                        denmarkBounds.north
                                                    ]
                                                ], {
                                                    padding: 20
                                                });
                                            }
                                        }, 1000);
                                    }
                                }
                            }
                        }
                    }, 1000);
                }
            });
            // Kommune hover functionality
            map.current.on('mouseenter', layerId, (e)=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = 'pointer';
                }
                if (e.features && e.features.length > 0) {
                    const feature = e.features[0];
                    const props = feature.properties;
                    console.log('Kommune feature properties on hover:', props); // Debug log to see actual data
                    if (props) {
                        setKommuneHoverInfo({
                            kommune_id: props.kommune_code || '',
                            kommune_name: props.kommune_name || 'Unknown Municipality',
                            pfas_grams: props.pfas_grams || 0,
                            pesticide_load: props.pesticide_load || 0,
                            agricultural_area_ha: props.agricultural_area_ha || 0,
                            kommune_area_ha: props.kommune_area_ha || 0,
                            field_count: props.field_count || 0,
                            company_count: props.company_count || 0,
                            year: props.year || year
                        });
                    }
                }
            });
            map.current.on('mouseleave', layerId, ()=>{
                if (map.current) {
                    map.current.getCanvas().style.cursor = '';
                }
                setKommuneHoverInfo(null);
            });
            setTileLoaded(tileKey);
            console.log('Kommune layer setup completed successfully');
        } catch (error) {
            console.error('Failed to load Kommune PMTiles:', error);
            setTileError(tileKey, error instanceof Error ? error.message : 'Unknown error');
        }
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `relative ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                ref: mapContainer,
                className: "w-full h-full"
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 887,
                columnNumber: 7
            }, this),
            (hoverInfo || bnboHoverInfo || kommuneHoverInfo) && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute bg-white/95 backdrop-blur-sm border border-slate-200 rounded-lg shadow-xl p-4 pointer-events-none z-10 max-w-sm",
                style: {
                    left: mousePosition.x + 15,
                    top: mousePosition.y - 15,
                    transform: mousePosition.x > window.innerWidth - 400 ? 'translateX(-100%)' : 'none'
                },
                children: [
                    hoverInfo && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["Fragment"], {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center justify-between mb-3",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-sm font-semibold text-slate-900",
                                                children: "H3 Cell Data"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 905,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-xs text-slate-500",
                                                children: [
                                                    "Resolution ",
                                                    hoverInfo.resolution,
                                                    " • ",
                                                    hoverInfo.year
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 906,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 904,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-xs text-slate-400 font-mono",
                                        children: [
                                            hoverInfo.h3_id.substring(0, 8),
                                            "..."
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 908,
                                        columnNumber: 17
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 903,
                                columnNumber: 15
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "space-y-3",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "bg-slate-50 rounded-md p-3",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between items-center mb-2",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-sm font-medium text-slate-700",
                                                        children: activeDataLayer === 'pfas' ? 'PFAS Intensity' : activeDataLayer === 'total_pesticide' ? 'Pesticide Intensity' : activeDataLayer === 'diquat' ? 'Diquat Intensity' : 'Glyphosate Intensity'
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 917,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "text-sm font-bold text-slate-900",
                                                        children: activeDataLayer === 'pfas' ? `${hoverInfo.pfas_intensity.toFixed(3)} g/ha` : activeDataLayer === 'total_pesticide' ? `${hoverInfo.pesticide_intensity.toFixed(3)} g/ha` : activeDataLayer === 'diquat' ? `${hoverInfo.diquat_intensity.toFixed(3)} g/ha` : `${hoverInfo.glyphosate_intensity.toFixed(3)} g/ha`
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 923,
                                                        columnNumber: 21
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 916,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between items-center text-xs text-slate-600",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        children: "Total Load:"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 931,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "font-medium",
                                                        children: activeDataLayer === 'pfas' ? `${hoverInfo.pfas_grams.toFixed(2)} g` : activeDataLayer === 'total_pesticide' ? `${(hoverInfo.pesticide_load * 1000).toFixed(1)} g` : activeDataLayer === 'diquat' ? `${hoverInfo.diquat_grams.toFixed(2)} g` : `${hoverInfo.glyphosate_grams.toFixed(2)} g`
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 932,
                                                        columnNumber: 21
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 930,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 915,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "space-y-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "text-xs font-medium text-slate-700 border-b pb-1",
                                                children: "Complete Cell Data"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 943,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "grid grid-cols-1 gap-2 text-xs",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "bg-red-50 rounded p-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "text-xs font-medium text-red-700 mb-1",
                                                                children: "Chemical Loads (grams)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 948,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "grid grid-cols-2 gap-1",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-red-600",
                                                                                children: "PFAS:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 951,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-red-900",
                                                                                children: [
                                                                                    hoverInfo.pfas_grams.toFixed(2),
                                                                                    " g"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 952,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 950,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-red-600",
                                                                                children: "Total Pesticides:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 955,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-red-900",
                                                                                children: [
                                                                                    (hoverInfo.pesticide_load * 1000).toFixed(1),
                                                                                    " g"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 956,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 954,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-red-600",
                                                                                children: "Diquat:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 959,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-red-900",
                                                                                children: [
                                                                                    hoverInfo.diquat_grams.toFixed(2),
                                                                                    " g"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 960,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 958,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-red-600",
                                                                                children: "Glyphosate:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 963,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-red-900",
                                                                                children: [
                                                                                    hoverInfo.glyphosate_grams.toFixed(2),
                                                                                    " g"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 964,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 962,
                                                                        columnNumber: 25
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 949,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 947,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "bg-orange-50 rounded p-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "text-xs font-medium text-orange-700 mb-1",
                                                                children: "Application Counts"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 971,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "grid grid-cols-2 gap-1",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-orange-600",
                                                                                children: "Total:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 974,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-orange-900",
                                                                                children: hoverInfo.applications
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 975,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 973,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-orange-600",
                                                                                children: "PFAS:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 978,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-orange-900",
                                                                                children: hoverInfo.pfas_applications
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 979,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 977,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-orange-600",
                                                                                children: "Diquat:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 982,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-orange-900",
                                                                                children: hoverInfo.diquat_applications
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 983,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 981,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-orange-600",
                                                                                children: "Glyphosate:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 986,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-orange-900",
                                                                                children: hoverInfo.glyphosate_applications
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 987,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 985,
                                                                        columnNumber: 25
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 972,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 970,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "bg-green-50 rounded p-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "text-xs font-medium text-green-700 mb-1",
                                                                children: "Agricultural Data"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 994,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "grid grid-cols-2 gap-1",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-green-600",
                                                                                children: "Field Count:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 997,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-green-900",
                                                                                children: hoverInfo.field_count
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 998,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 996,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-green-600",
                                                                                children: "Area:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1001,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-green-900",
                                                                                children: [
                                                                                    hoverInfo.area_ha.toFixed(1),
                                                                                    " ha"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1002,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1000,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-green-600",
                                                                                children: "Coverage:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1005,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-green-900",
                                                                                children: [
                                                                                    (hoverInfo.coverage * 100).toFixed(0),
                                                                                    "%"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1006,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1004,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-green-600",
                                                                                children: "Quality:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1009,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-green-900",
                                                                                children: hoverInfo.coverage > 0.8 ? 'High' : hoverInfo.coverage > 0.5 ? 'Medium' : 'Low'
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1010,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1008,
                                                                        columnNumber: 25
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 995,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 993,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "bg-blue-50 rounded p-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "text-xs font-medium text-blue-700 mb-1",
                                                                children: "Intensity Metrics (g/ha)"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 1017,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "grid grid-cols-2 gap-1",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-blue-600",
                                                                                children: "PFAS:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1020,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-blue-900",
                                                                                children: [
                                                                                    hoverInfo.pfas_intensity.toFixed(3),
                                                                                    " g/ha"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1021,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1019,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-blue-600",
                                                                                children: "Pesticides:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1024,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-blue-900",
                                                                                children: [
                                                                                    hoverInfo.pesticide_intensity.toFixed(3),
                                                                                    " g/ha"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1025,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1023,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-blue-600",
                                                                                children: "Diquat:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1028,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-blue-900",
                                                                                children: [
                                                                                    hoverInfo.diquat_intensity.toFixed(3),
                                                                                    " g/ha"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1029,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1027,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-blue-600",
                                                                                children: "Glyphosate:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1032,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-blue-900",
                                                                                children: [
                                                                                    hoverInfo.glyphosate_intensity.toFixed(3),
                                                                                    " g/ha"
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1033,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1031,
                                                                        columnNumber: 25
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 1018,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 1016,
                                                        columnNumber: 21
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "bg-gray-50 rounded p-2",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "text-xs font-medium text-gray-700 mb-1",
                                                                children: "Technical Info"
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 1040,
                                                                columnNumber: 23
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "grid grid-cols-2 gap-1",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-gray-600",
                                                                                children: "H3 ID:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1043,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-gray-900 font-mono text-xs",
                                                                                children: [
                                                                                    hoverInfo.h3_id.substring(0, 12),
                                                                                    "..."
                                                                                ]
                                                                            }, void 0, true, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1044,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1042,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-gray-600",
                                                                                children: "Resolution:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1047,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-gray-900",
                                                                                children: hoverInfo.resolution
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1048,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1046,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-gray-600",
                                                                                children: "Year:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1051,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-gray-900",
                                                                                children: hoverInfo.year
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1052,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1050,
                                                                        columnNumber: 25
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: "flex justify-between",
                                                                        children: [
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "text-gray-600",
                                                                                children: "Data Quality:"
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1055,
                                                                                columnNumber: 27
                                                                            }, this),
                                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                                className: "font-medium text-gray-900",
                                                                                children: hoverInfo.coverage > 0.8 ? 'High' : hoverInfo.coverage > 0.5 ? 'Medium' : 'Low'
                                                                            }, void 0, false, {
                                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                                lineNumber: 1056,
                                                                                columnNumber: 27
                                                                            }, this)
                                                                        ]
                                                                    }, void 0, true, {
                                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                        lineNumber: 1054,
                                                                        columnNumber: 25
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                                lineNumber: 1041,
                                                                columnNumber: 23
                                                            }, this)
                                                        ]
                                                    }, void 0, true, {
                                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                        lineNumber: 1039,
                                                        columnNumber: 21
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 945,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 942,
                                        columnNumber: 17
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 913,
                                columnNumber: 15
                            }, this)
                        ]
                    }, void 0, true),
                    bnboHoverInfo && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["Fragment"], {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-sm font-semibold text-slate-900 mb-2",
                                children: "BNBO Protected Area"
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 1068,
                                columnNumber: 15
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "space-y-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "ID:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1073,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900 font-mono text-xs",
                                                children: bnboHoverInfo.bnbo_id
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1074,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1072,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Status:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1077,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: bnboHoverInfo.status_description
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1078,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1076,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Area:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1081,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: [
                                                    bnboHoverInfo.area_ha.toFixed(1),
                                                    " ha"
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1082,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1080,
                                        columnNumber: 17
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 1071,
                                columnNumber: 15
                            }, this)
                        ]
                    }, void 0, true),
                    kommuneHoverInfo && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["Fragment"], {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "text-sm font-semibold text-slate-900 mb-2",
                                children: kommuneHoverInfo.kommune_name
                            }, void 0, false, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 1090,
                                columnNumber: 15
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "space-y-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Municipality ID:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1095,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900 font-mono text-xs",
                                                children: kommuneHoverInfo.kommune_id
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1096,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1094,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Total PFAS:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1099,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: [
                                                    kommuneHoverInfo.pfas_grams.toFixed(1),
                                                    " g"
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1100,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1098,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Pesticides:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1103,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: [
                                                    kommuneHoverInfo.pesticide_load.toFixed(1),
                                                    " kg"
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1104,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1102,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Fields:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1107,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: kommuneHoverInfo.field_count
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1108,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1106,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Companies:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1111,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: kommuneHoverInfo.company_count
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1112,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1110,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-sm",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "text-slate-600",
                                                children: "Agricultural Area:"
                                            }, void 0, false, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1115,
                                                columnNumber: 19
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-medium text-slate-900",
                                                children: [
                                                    kommuneHoverInfo.agricultural_area_ha.toFixed(0),
                                                    " ha"
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                                lineNumber: 1116,
                                                columnNumber: 19
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                        lineNumber: 1114,
                                        columnNumber: 17
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                                lineNumber: 1093,
                                columnNumber: 15
                            }, this)
                        ]
                    }, void 0, true)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 893,
                columnNumber: 9
            }, this),
            !isLoaded && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute inset-0 bg-gray-100 flex items-center justify-center",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-center",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-2"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 1130,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "text-sm text-gray-600",
                            children: "Loading map..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/PMTilesMap.tsx",
                            lineNumber: 1131,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/PMTilesMap.tsx",
                    lineNumber: 1129,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/PMTilesMap.tsx",
                lineNumber: 1128,
                columnNumber: 9
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/PMTilesMap.tsx",
        lineNumber: 886,
        columnNumber: 5
    }, this);
}
}}),
"[project]/src/components/controls/ThemeToggle.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "ThemeToggle": (()=>ThemeToggle)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/ui-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$sun$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Sun$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/sun.js [app-ssr] (ecmascript) <export default as Sun>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$moon$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Moon$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/moon.js [app-ssr] (ecmascript) <export default as Moon>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$monitor$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Monitor$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/monitor.js [app-ssr] (ecmascript) <export default as Monitor>");
'use client';
;
;
;
function ThemeToggle({ className = '' }) {
    const { theme, setTheme } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useUIStore"])();
    const themes = [
        {
            id: 'light',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$sun$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Sun$3e$__["Sun"],
            label: 'Light'
        },
        {
            id: 'dark',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$moon$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Moon$3e$__["Moon"],
            label: 'Dark'
        },
        {
            id: 'system',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$monitor$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Monitor$3e$__["Monitor"],
            label: 'System'
        }
    ];
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `${className}`,
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "flex items-center space-x-1 bg-black/80 backdrop-blur-sm rounded-lg border border-white/20 p-1",
            children: themes.map(({ id, icon: Icon, label })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: ()=>setTheme(id),
                    className: `flex items-center space-x-1 px-2 py-1 rounded text-xs transition-colors ${theme === id ? 'bg-white/20 text-white' : 'text-gray-300 hover:text-white hover:bg-white/10'}`,
                    title: `Switch to ${label} theme`,
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(Icon, {
                            className: "w-3 h-3"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/ThemeToggle.tsx",
                            lineNumber: 33,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                            className: "hidden sm:inline",
                            children: label
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/ThemeToggle.tsx",
                            lineNumber: 34,
                            columnNumber: 13
                        }, this)
                    ]
                }, id, true, {
                    fileName: "[project]/src/components/controls/ThemeToggle.tsx",
                    lineNumber: 23,
                    columnNumber: 11
                }, this))
        }, void 0, false, {
            fileName: "[project]/src/components/controls/ThemeToggle.tsx",
            lineNumber: 21,
            columnNumber: 7
        }, this)
    }, void 0, false, {
        fileName: "[project]/src/components/controls/ThemeToggle.tsx",
        lineNumber: 20,
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$ThemeToggle$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/ThemeToggle.tsx [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/pmtiles-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/temporal-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/resolution-store.ts [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/play.js [app-ssr] (ecmascript) <export default as Play>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/pause.js [app-ssr] (ecmascript) <export default as Pause>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-left.js [app-ssr] (ecmascript) <export default as ChevronLeft>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-right.js [app-ssr] (ecmascript) <export default as ChevronRight>");
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
    const [error, setError] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    const [showBNBO, setShowBNBO] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [showKommune, setShowKommune] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(false);
    const [bnboOpacity, setBnboOpacity] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(0.4);
    const [kommuneOpacity, setKommuneOpacity] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(0.6);
    const [activeDataLayer, setActiveDataLayer] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])('pfas');
    // Store hooks
    const { metadata, metadataLoading, metadataError, setMetadata, setMetadataLoading, setMetadataError } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["usePMTilesStore"])();
    const { currentYear, availableYears, isAnimating, setAvailableYears, setCurrentYear, startAnimation, stopAnimation, goToNextYear, goToPreviousYear, canGoNext, canGoPrevious } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useTemporalStore"])();
    const { setResolution, currentResolution } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useResolutionStore"])();
    // Load PMTiles metadata on mount
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        async function loadMetadata() {
            try {
                setMetadataLoading(true);
                setError(null);
                const response = await fetch('/api/metadata');
                if (!response.ok) {
                    throw new Error(`Failed to load metadata: ${response.statusText}`);
                }
                const data = await response.json();
                // Update stores with metadata
                setMetadata(data);
                setAvailableYears(data.years);
                // Set initial year to most recent
                if (data.years.length > 0) {
                    const latestYear = Math.max(...data.years);
                    setCurrentYear(latestYear);
                }
                // Set initial resolution to highest available
                if (data.resolutions.length > 0) {
                    const highestRes = Math.max(...data.resolutions);
                    setResolution(highestRes);
                }
                setIsInitialized(true);
            } catch (err) {
                const errorMessage = err instanceof Error ? err.message : 'Unknown error';
                setError(errorMessage);
                setMetadataError(errorMessage);
            } finally{
                setMetadataLoading(false);
            }
        }
        loadMetadata();
    }, [
        setMetadata,
        setMetadataLoading,
        setMetadataError,
        setAvailableYears,
        setCurrentYear,
        setResolution
    ]);
    // Loading state
    if (metadataLoading || !isInitialized) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-black flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-6 h-6 border border-white border-t-transparent rounded-full animate-spin mx-auto mb-3"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 96,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                        className: "text-white text-sm font-light",
                        children: "Loading..."
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 97,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 95,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 94,
            columnNumber: 7
        }, this);
    }
    // Error state
    if (error || metadataError) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-black flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-sm mb-3",
                        children: error || metadataError
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 108,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-3 py-1 bg-white text-black text-sm rounded hover:bg-gray-100 transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 111,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 107,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 106,
            columnNumber: 7
        }, this);
    }
    const dataLayerConfig = {
        total_pesticide: {
            name: 'Total Pesticide Load',
            description: 'Total pesticide application load',
            field: 'pesticide_load',
            unit: 'g',
            colors: [
                {
                    color: 'bg-white',
                    label: 'None',
                    value: 0
                },
                {
                    color: 'bg-orange-200',
                    label: 'Low',
                    value: 1000
                },
                {
                    color: 'bg-orange-400',
                    label: 'Medium',
                    value: 10000
                },
                {
                    color: 'bg-orange-600',
                    label: 'High',
                    value: 50000
                },
                {
                    color: 'bg-orange-800',
                    label: 'Very High',
                    value: 100000
                }
            ]
        },
        pfas: {
            name: 'PFAS Load',
            description: 'PFAS contamination levels',
            field: 'pfas_grams',
            unit: 'g',
            colors: [
                {
                    color: 'bg-white',
                    label: 'None',
                    value: 0
                },
                {
                    color: 'bg-red-200',
                    label: 'Low',
                    value: 0.1
                },
                {
                    color: 'bg-red-400',
                    label: 'Medium',
                    value: 1
                },
                {
                    color: 'bg-red-600',
                    label: 'High',
                    value: 10
                },
                {
                    color: 'bg-red-800',
                    label: 'Very High',
                    value: 50
                }
            ]
        },
        diquat: {
            name: 'Diquat Load',
            description: 'Diquat herbicide active ingredient',
            field: 'diquat_grams',
            unit: 'g',
            colors: [
                {
                    color: 'bg-white',
                    label: 'None',
                    value: 0
                },
                {
                    color: 'bg-blue-200',
                    label: 'Low',
                    value: 0.1
                },
                {
                    color: 'bg-blue-400',
                    label: 'Medium',
                    value: 1
                },
                {
                    color: 'bg-blue-600',
                    label: 'High',
                    value: 10
                },
                {
                    color: 'bg-blue-800',
                    label: 'Very High',
                    value: 100
                }
            ]
        },
        glyphosate: {
            name: 'Glyphosate Load',
            description: 'Glyphosate herbicide active ingredient',
            field: 'glyphosate_grams',
            unit: 'g',
            colors: [
                {
                    color: 'bg-white',
                    label: 'None',
                    value: 0
                },
                {
                    color: 'bg-green-200',
                    label: 'Low',
                    value: 0.1
                },
                {
                    color: 'bg-green-400',
                    label: 'Medium',
                    value: 1
                },
                {
                    color: 'bg-green-600',
                    label: 'High',
                    value: 10
                },
                {
                    color: 'bg-green-800',
                    label: 'Very High',
                    value: 100
                }
            ]
        }
    };
    const currentLayerConfig = dataLayerConfig[activeDataLayer];
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-black text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-black/90 backdrop-blur-sm border-b border-white/10 px-6 py-4 z-50",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center justify-between",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                        className: "text-lg font-semibold",
                                        children: "PFAS Environmental Impact"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 186,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                        className: "text-sm text-gray-400",
                                        children: "Pesticide contamination • Denmark"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 187,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 185,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center space-x-2 bg-white/10 rounded-lg p-1",
                                children: Object.entries(dataLayerConfig).map(([key, config])=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                        onClick: ()=>setActiveDataLayer(key),
                                        className: `px-4 py-2 rounded-md text-sm font-medium transition-all ${activeDataLayer === key ? 'bg-white text-black' : 'text-white hover:bg-white/20'}`,
                                        children: config.name
                                    }, key, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 193,
                                        columnNumber: 15
                                    }, this))
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 191,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center space-x-4",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$ThemeToggle$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["ThemeToggle"], {}, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 209,
                                        columnNumber: 13
                                    }, this),
                                    metadata && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-right text-xs text-gray-400 font-light",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            children: [
                                                metadata.years.length,
                                                " years • ",
                                                metadata.resolutions.length,
                                                " resolutions"
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 212,
                                            columnNumber: 17
                                        }, this)
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 211,
                                        columnNumber: 15
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 208,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 183,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-4 flex items-center justify-center space-x-6",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: goToPreviousYear,
                                disabled: !canGoPrevious() || isAnimating,
                                className: "p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__["ChevronLeft"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 225,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 220,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: isAnimating ? stopAnimation : startAnimation,
                                disabled: availableYears.length <= 1,
                                className: "p-2 rounded-full bg-white text-black hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: isAnimating ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__["Pause"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 233,
                                    columnNumber: 28
                                }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__["Play"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 233,
                                    columnNumber: 60
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 228,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: goToNextYear,
                                disabled: !canGoNext() || isAnimating,
                                className: "p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__["ChevronRight"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 241,
                                    columnNumber: 13
                                }, this)
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 236,
                                columnNumber: 11
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex-1 max-w-md",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-xs text-gray-400 mb-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                children: Math.min(...availableYears)
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 247,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-bold text-white",
                                                children: currentYear
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 248,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                children: Math.max(...availableYears)
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 249,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 246,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "relative",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                type: "range",
                                                min: Math.min(...availableYears),
                                                max: Math.max(...availableYears),
                                                step: "1",
                                                value: currentYear,
                                                onChange: (e)=>setCurrentYear(parseInt(e.target.value)),
                                                className: "w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer slider",
                                                disabled: isAnimating
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 252,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between mt-1",
                                                children: availableYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: `w-1 h-2 rounded-full ${year === currentYear ? 'bg-white' : 'bg-white/30'}`
                                                    }, year, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 264,
                                                        columnNumber: 19
                                                    }, this))
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 262,
                                                columnNumber: 15
                                            }, this)
                                        ]
                                    }, void 0, true, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 251,
                                        columnNumber: 13
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 245,
                                columnNumber: 11
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 219,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 182,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1 flex",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-80 bg-black/90 backdrop-blur-sm border-r border-white/10 p-6 overflow-y-auto",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "space-y-6",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                            className: "text-lg font-semibold mb-2",
                                            children: currentLayerConfig.name
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 284,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                            className: "text-sm text-gray-400 mb-4",
                                            children: currentLayerConfig.description
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 285,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                    className: "text-sm font-medium text-gray-300",
                                                    children: "Legend"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 289,
                                                    columnNumber: 17
                                                }, this),
                                                currentLayerConfig.colors.map((item, index)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center justify-between",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "flex items-center space-x-3",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: `w-4 h-4 ${item.color} rounded border border-white/20`
                                                                    }, void 0, false, {
                                                                        fileName: "[project]/src/app/page.tsx",
                                                                        lineNumber: 293,
                                                                        columnNumber: 23
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                        className: "text-sm",
                                                                        children: item.label
                                                                    }, void 0, false, {
                                                                        fileName: "[project]/src/app/page.tsx",
                                                                        lineNumber: 294,
                                                                        columnNumber: 23
                                                                    }, this)
                                                                ]
                                                            }, void 0, true, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 292,
                                                                columnNumber: 21
                                                            }, this),
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                className: "text-xs text-gray-400",
                                                                children: item.value > 0 ? `${item.value}+ ${currentLayerConfig.unit}` : 'No data'
                                                            }, void 0, false, {
                                                                fileName: "[project]/src/app/page.tsx",
                                                                lineNumber: 296,
                                                                columnNumber: 21
                                                            }, this)
                                                        ]
                                                    }, index, true, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 291,
                                                        columnNumber: 19
                                                    }, this))
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 288,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 283,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "flex items-center justify-between mb-3",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                            className: "text-sm font-medium",
                                                            children: "Municipality Boundaries"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 308,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                                            className: "text-xs text-gray-400",
                                                            children: "Auto-shown when zoomed out (≤ zoom 8)"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 309,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 307,
                                                    columnNumber: 17
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("label", {
                                                    className: "relative inline-flex items-center cursor-pointer",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                            type: "checkbox",
                                                            checked: showKommune,
                                                            onChange: (e)=>setShowKommune(e.target.checked),
                                                            className: "sr-only peer"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 312,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 318,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 311,
                                                    columnNumber: 17
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 306,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "text-xs text-gray-400 mb-3",
                                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "inline-flex items-center space-x-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "w-2 h-2 bg-blue-500 rounded-full animate-pulse"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 324,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        children: "Municipalities visible when zoom ≤ 8"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 325,
                                                        columnNumber: 19
                                                    }, this)
                                                ]
                                            }, void 0, true, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 323,
                                                columnNumber: 17
                                            }, this)
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 322,
                                            columnNumber: 15
                                        }, this),
                                        showKommune && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex items-center justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-sm",
                                                            children: "Opacity"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 332,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-xs text-gray-400",
                                                            children: [
                                                                Math.round(kommuneOpacity * 100),
                                                                "%"
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 333,
                                                            columnNumber: 21
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 331,
                                                    columnNumber: 19
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                    type: "range",
                                                    min: "0",
                                                    max: "1",
                                                    step: "0.1",
                                                    value: kommuneOpacity,
                                                    onChange: (e)=>setKommuneOpacity(parseFloat(e.target.value)),
                                                    className: "w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 335,
                                                    columnNumber: 19
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "space-y-2 mt-4",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "text-xs text-gray-400 mb-2",
                                                            children: "Municipal PFAS levels:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 346,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-800 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 348,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Very High (1000+ g)"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 349,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 347,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-600 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 352,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "High (100-1000 g)"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 353,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 351,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-400 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 356,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Medium (10-100 g)"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 357,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 355,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-200 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 360,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Low (0-10 g)"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 361,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 359,
                                                            columnNumber: 21
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 345,
                                                    columnNumber: 19
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 330,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 305,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "flex items-center justify-between mb-3",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                    className: "text-sm font-medium",
                                                    children: "BNBO Protected Areas"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 371,
                                                    columnNumber: 17
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("label", {
                                                    className: "relative inline-flex items-center cursor-pointer",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                            type: "checkbox",
                                                            checked: showBNBO,
                                                            onChange: (e)=>setShowBNBO(e.target.checked),
                                                            className: "sr-only peer"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 373,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 379,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 372,
                                                    columnNumber: 17
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 370,
                                            columnNumber: 15
                                        }, this),
                                        showBNBO && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex items-center justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-sm",
                                                            children: "Opacity"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 386,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-xs text-gray-400",
                                                            children: [
                                                                Math.round(bnboOpacity * 100),
                                                                "%"
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 387,
                                                            columnNumber: 21
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 385,
                                                    columnNumber: 19
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                    type: "range",
                                                    min: "0",
                                                    max: "1",
                                                    step: "0.1",
                                                    value: bnboOpacity,
                                                    onChange: (e)=>setBnboOpacity(parseFloat(e.target.value)),
                                                    className: "w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 389,
                                                    columnNumber: 19
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "space-y-2 mt-4",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 401,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Action Required"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 402,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 400,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-green-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 405,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Completed"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 406,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 404,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-gray-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 409,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                                    className: "text-sm",
                                                                    children: "Unknown"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 410,
                                                                    columnNumber: 23
                                                                }, this)
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 408,
                                                            columnNumber: 21
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 399,
                                                    columnNumber: 19
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 384,
                                            columnNumber: 17
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 369,
                                    columnNumber: 13
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-sm font-medium mb-3",
                                            children: "Current View"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 419,
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
                                                            lineNumber: 422,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "font-medium",
                                                            children: currentYear
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 423,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 421,
                                                    columnNumber: 17
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-gray-400",
                                                            children: "Resolution:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 426,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "font-medium",
                                                            children: [
                                                                "H3-",
                                                                currentResolution
                                                            ]
                                                        }, void 0, true, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 427,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 425,
                                                    columnNumber: 17
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-gray-400",
                                                            children: "Data Layer:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 430,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "font-medium",
                                                            children: currentLayerConfig.name
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 431,
                                                            columnNumber: 19
                                                        }, this)
                                                    ]
                                                }, void 0, true, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 429,
                                                    columnNumber: 17
                                                }, this)
                                            ]
                                        }, void 0, true, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 420,
                                            columnNumber: 15
                                        }, this)
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 418,
                                    columnNumber: 13
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 281,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 280,
                        columnNumber: 9
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$PMTilesMap$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["PMTilesMap"], {
                            className: "w-full h-full",
                            pmtilesBaseUrl: "/api/pmtiles",
                            showBNBO: showBNBO,
                            bnboOpacity: bnboOpacity,
                            showKommune: showKommune,
                            kommuneOpacity: kommuneOpacity,
                            onBNBOToggle: setShowBNBO,
                            onKommuneToggle: setShowKommune,
                            activeDataLayer: activeDataLayer
                        }, void 0, false, {
                            fileName: "[project]/src/app/page.tsx",
                            lineNumber: 440,
                            columnNumber: 11
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 439,
                        columnNumber: 9
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 278,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/app/page.tsx",
        lineNumber: 180,
        columnNumber: 5
    }, this);
}
}}),

};

//# sourceMappingURL=src_dc4cc197._.js.map