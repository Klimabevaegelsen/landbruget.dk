module.exports = {

"[project]/src/components/map/NewPMTilesMap.tsx [app-ssr] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "default": (()=>NewPMTilesMap)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react-jsx-dev-runtime.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/server/route-modules/app-page/vendored/ssr/react.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-ssr] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/pmtiles/dist/index.js [app-ssr] (ecmascript)");
'use client';
;
;
;
;
;
// Register PMTiles protocol
const protocol = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["Protocol"]();
__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].addProtocol('pmtiles', protocol.tile);
function NewPMTilesMap({ className = '', onMapLoad, onMapError }) {
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const map = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useRef"])(null);
    const [status, setStatus] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])('Initializing...');
    const [error, setError] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useEffect"])(()=>{
        if (!mapContainer.current) return;
        try {
            setStatus('Creating map...');
            // Initialize map with dark background
            map.current = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].Map({
                container: mapContainer.current,
                style: {
                    version: 8,
                    sources: {},
                    layers: [
                        {
                            id: 'background',
                            type: 'background',
                            paint: {
                                'background-color': '#1a1a1a'
                            }
                        }
                    ]
                },
                center: [
                    10.0,
                    56.0
                ],
                zoom: 6,
                maxZoom: 15,
                minZoom: 4
            });
            map.current.on('load', ()=>{
                setStatus('Map loaded, adding data sources...');
                if (map.current) {
                    setStatus('Adding kommune data...');
                    // Add kommune data source
                    map.current.addSource('kommune-data', {
                        type: 'vector',
                        url: 'pmtiles://http://localhost:3000/api/pmtiles/kommune_pfas_2023.pmtiles'
                    });
                    // Try to add kommune layer with proper styling
                    try {
                        map.current.addLayer({
                            id: 'kommune-layer',
                            source: 'kommune-data',
                            'source-layer': 'kommune_pfas_2023',
                            type: 'fill',
                            paint: {
                                'fill-color': [
                                    'interpolate',
                                    [
                                        'linear'
                                    ],
                                    [
                                        'get',
                                        'pesticide_belastning_per_ha'
                                    ],
                                    0,
                                    'rgba(255,255,255,0)',
                                    1,
                                    '#ffeeee',
                                    10,
                                    '#ffcccc',
                                    50,
                                    '#ff9999',
                                    100,
                                    '#ff6666',
                                    500,
                                    '#ff3333',
                                    1000,
                                    '#ff0000'
                                ],
                                'fill-opacity': 0.8
                            }
                        });
                        // Add kommune borders
                        map.current.addLayer({
                            id: 'kommune-borders',
                            source: 'kommune-data',
                            'source-layer': 'kommune_pfas_2023',
                            type: 'line',
                            paint: {
                                'line-color': '#ffffff',
                                'line-width': 1,
                                'line-opacity': 0.5
                            }
                        });
                        setStatus('Kommune layer added successfully');
                    } catch (e) {
                        console.log('Failed to add kommune layer with default source-layer, trying alternatives...', e);
                        // Try alternative source layer names
                        const alternativeNames = [
                            'default',
                            'kommune',
                            'data',
                            'layer0'
                        ];
                        for (const sourceLayer of alternativeNames){
                            try {
                                map.current.addLayer({
                                    id: `kommune-layer-${sourceLayer}`,
                                    source: 'kommune-data',
                                    'source-layer': sourceLayer,
                                    type: 'fill',
                                    paint: {
                                        'fill-color': [
                                            'interpolate',
                                            [
                                                'linear'
                                            ],
                                            [
                                                'get',
                                                'pesticide_belastning_per_ha'
                                            ],
                                            0,
                                            'rgba(255,255,255,0)',
                                            1,
                                            '#ffeeee',
                                            10,
                                            '#ffcccc',
                                            50,
                                            '#ff9999',
                                            100,
                                            '#ff6666',
                                            500,
                                            '#ff3333',
                                            1000,
                                            '#ff0000'
                                        ],
                                        'fill-opacity': 0.8
                                    }
                                });
                                setStatus(`Kommune layer added with source-layer: ${sourceLayer}`);
                                break;
                            } catch (err) {
                                console.log(`Failed with source-layer: ${sourceLayer}`, err);
                            }
                        }
                    }
                    setStatus('Adding H3 data...');
                    // Add H3 data source
                    map.current.addSource('h3-data', {
                        type: 'vector',
                        url: 'pmtiles://http://localhost:3000/api/pmtiles/h3_pfas_2023_res8.pmtiles'
                    });
                    // Add H3 layer
                    try {
                        map.current.addLayer({
                            id: 'h3-layer',
                            source: 'h3-data',
                            'source-layer': 'h3_pfas_2023_res8',
                            type: 'fill',
                            paint: {
                                'fill-color': [
                                    'interpolate',
                                    [
                                        'linear'
                                    ],
                                    [
                                        'get',
                                        'pesticide_belastning_per_ha'
                                    ],
                                    0,
                                    'rgba(255,255,255,0)',
                                    1,
                                    '#ffeeee',
                                    10,
                                    '#ffcccc',
                                    50,
                                    '#ff9999',
                                    100,
                                    '#ff6666',
                                    500,
                                    '#ff3333',
                                    1000,
                                    '#ff0000'
                                ],
                                'fill-opacity': 0.7
                            },
                            layout: {
                                visibility: 'none' // Start hidden, show at high zoom
                            }
                        });
                        setStatus('H3 layer added successfully');
                    } catch (e) {
                        console.log('Failed to add H3 layer, trying alternatives...', e);
                        const h3AlternativeNames = [
                            'default',
                            'h3',
                            'data',
                            'layer0'
                        ];
                        for (const sourceLayer of h3AlternativeNames){
                            try {
                                map.current.addLayer({
                                    id: `h3-layer-${sourceLayer}`,
                                    source: 'h3-data',
                                    'source-layer': sourceLayer,
                                    type: 'fill',
                                    paint: {
                                        'fill-color': [
                                            'interpolate',
                                            [
                                                'linear'
                                            ],
                                            [
                                                'get',
                                                'pesticide_belastning_per_ha'
                                            ],
                                            0,
                                            'rgba(255,255,255,0)',
                                            1,
                                            '#ffeeee',
                                            10,
                                            '#ffcccc',
                                            50,
                                            '#ff9999',
                                            100,
                                            '#ff6666',
                                            500,
                                            '#ff3333',
                                            1000,
                                            '#ff0000'
                                        ],
                                        'fill-opacity': 0.7
                                    },
                                    layout: {
                                        visibility: 'none'
                                    }
                                });
                                setStatus(`H3 layer added with source-layer: ${sourceLayer}`);
                                break;
                            } catch (err) {
                                console.log(`Failed H3 with source-layer: ${sourceLayer}`, err);
                            }
                        }
                    }
                    // Add zoom-based layer switching
                    map.current.on('zoom', ()=>{
                        if (map.current) {
                            const zoom = map.current.getZoom();
                            // Show kommune at low zoom, H3 at high zoom
                            if (zoom < 9) {
                                // Show kommune layers
                                const kommuneLayers = map.current.getStyle().layers.filter((l)=>l.id.startsWith('kommune-'));
                                kommuneLayers.forEach((layer)=>{
                                    map.current.setLayoutProperty(layer.id, 'visibility', 'visible');
                                });
                                // Hide H3 layers
                                const h3Layers = map.current.getStyle().layers.filter((l)=>l.id.startsWith('h3-'));
                                h3Layers.forEach((layer)=>{
                                    map.current.setLayoutProperty(layer.id, 'visibility', 'none');
                                });
                            } else {
                                // Hide kommune layers
                                const kommuneLayers = map.current.getStyle().layers.filter((l)=>l.id.startsWith('kommune-'));
                                kommuneLayers.forEach((layer)=>{
                                    map.current.setLayoutProperty(layer.id, 'visibility', 'none');
                                });
                                // Show H3 layers
                                const h3Layers = map.current.getStyle().layers.filter((l)=>l.id.startsWith('h3-'));
                                h3Layers.forEach((layer)=>{
                                    map.current.setLayoutProperty(layer.id, 'visibility', 'visible');
                                });
                            }
                        }
                    });
                    // Add navigation controls
                    map.current.addControl(new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"].NavigationControl(), 'top-right');
                    setStatus('Map setup complete!');
                    onMapLoad?.(map.current);
                }
            });
            map.current.on('error', (e)=>{
                console.error('Map error:', e);
                const errorMsg = e.error?.message || 'Unknown map error';
                setError(errorMsg);
                onMapError?.(new Error(errorMsg));
            });
            map.current.on('sourcedata', (e)=>{
                if (e.isSourceLoaded) {
                    console.log(`Source loaded: ${e.sourceId}`);
                    setStatus(`Source loaded: ${e.sourceId}`);
                }
            });
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : 'Failed to initialize map';
            console.error('Map initialization error:', e);
            setError(errorMsg);
            onMapError?.(new Error(errorMsg));
        }
        // Cleanup
        return ()=>{
            if (map.current) {
                map.current.remove();
                map.current = null;
            }
        };
    }, [
        onMapLoad,
        onMapError
    ]);
    if (error) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `${className} bg-black flex items-center justify-center`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center text-white",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-sm mb-2",
                        children: [
                            "Error: ",
                            error
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 297,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-3 py-1 bg-white text-black text-sm rounded hover:bg-gray-100 transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 298,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 296,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
            lineNumber: 295,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `${className} relative`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                ref: mapContainer,
                className: "w-full h-full"
            }, void 0, false, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 311,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute top-4 left-4 bg-black bg-opacity-75 text-white text-xs px-2 py-1 rounded",
                children: [
                    "Status: ",
                    status
                ]
            }, void 0, true, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 312,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
        lineNumber: 310,
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$NewPMTilesMap$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/NewPMTilesMap.tsx [app-ssr] (ecmascript)");
'use client';
;
;
;
function Home() {
    const [mapError, setMapError] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["useState"])(null);
    if (mapError) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-black flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-sm mb-3",
                        children: [
                            "Map Error: ",
                            mapError
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 13,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                        onClick: ()=>window.location.reload(),
                        className: "px-3 py-1 bg-white text-black text-sm rounded hover:bg-gray-100 transition-colors",
                        children: "Retry"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 16,
                        columnNumber: 11
                    }, this)
                ]
            }, void 0, true, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 12,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/app/page.tsx",
            lineNumber: 11,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-black text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-black/90 backdrop-blur-sm border-b border-white/10 px-6 py-4 z-50",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "flex items-center justify-between",
                    children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                className: "text-lg font-semibold",
                                children: "PMTiles Map Test"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 33,
                                columnNumber: 13
                            }, this),
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                className: "text-sm text-gray-400",
                                children: "Testing PMTiles integration"
                            }, void 0, false, {
                                fileName: "[project]/src/app/page.tsx",
                                lineNumber: 34,
                                columnNumber: 13
                            }, this)
                        ]
                    }, void 0, true, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 32,
                        columnNumber: 11
                    }, this)
                }, void 0, false, {
                    fileName: "[project]/src/app/page.tsx",
                    lineNumber: 31,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 30,
                columnNumber: 7
            }, this),
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$server$2f$route$2d$modules$2f$app$2d$page$2f$vendored$2f$ssr$2f$react$2d$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$NewPMTilesMap$2e$tsx__$5b$app$2d$ssr$5d$__$28$ecmascript$29$__["default"], {
                    className: "w-full h-full",
                    onMapLoad: (map)=>{
                        console.log('Map loaded successfully:', map);
                    },
                    onMapError: (error)=>{
                        console.error('Map error:', error);
                        setMapError(error.message);
                    }
                }, void 0, false, {
                    fileName: "[project]/src/app/page.tsx",
                    lineNumber: 41,
                    columnNumber: 9
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/app/page.tsx",
                lineNumber: 40,
                columnNumber: 7
            }, this)
        ]
    }, void 0, true, {
        fileName: "[project]/src/app/page.tsx",
        lineNumber: 28,
        columnNumber: 5
    }, this);
}
}}),

};

//# sourceMappingURL=src_d308a558._.js.map