(globalThis.TURBOPACK = globalThis.TURBOPACK || []).push([typeof document === "object" ? document.currentScript : undefined, {

"[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
/**
 * PMTiles Discovery Service
 * Automatically discovers the latest versions of PMTiles files from GCS bucket
 */ __turbopack_context__.s({
    "default": (()=>__TURBOPACK__default__export__),
    "pmtilesDiscovery": (()=>pmtilesDiscovery)
});
class PMTilesDiscoveryService {
    config;
    cache = new Map();
    cacheExpiry = 5 * 60 * 1000;
    constructor(config){
        this.config = config;
    }
    /**
   * Discover all available PMTiles files
   */ async discoverAllTiles() {
        const cacheKey = 'all-tiles';
        const cached = this.cache.get(cacheKey);
        if (cached && Date.now() - cached.timestamp < this.cacheExpiry) {
            return cached;
        }
        const discovered = {
            h3: {},
            kommune: {},
            bnbo: await this.discoverBNBOTiles(),
            basemap: await this.discoverBasemapTiles()
        };
        // Discover H3 tiles for all years and resolutions
        const years = await this.getAvailableYears();
        for (const year of years){
            discovered.h3[year] = {};
            const resolutions = await this.getAvailableResolutions(year);
            for (const resolution of resolutions){
                const metadata = await this.discoverLatestH3Tiles(year, resolution);
                if (metadata) {
                    discovered.h3[year][resolution] = metadata;
                }
            }
            // Discover Kommune tiles for this year
            const kommuneMetadata = await this.discoverLatestKommuneTiles(year);
            if (kommuneMetadata) {
                discovered.kommune[year] = kommuneMetadata;
            }
        }
        const cachedDiscovered = {
            ...discovered,
            timestamp: Date.now()
        };
        this.cache.set(cacheKey, cachedDiscovered);
        return discovered;
    }
    /**
   * Discover latest H3 tiles for a specific year and resolution
   */ async discoverLatestH3Tiles(year, resolution) {
        try {
            const pattern = `gold/pmtiles/h3_pfas_${year}_res${resolution}/`;
            const directories = await this.listGCSDirectories(pattern);
            if (directories.length === 0) {
                return null;
            }
            // Sort directories by timestamp (newest first)
            const sortedDirs = directories.sort((a, b)=>b.localeCompare(a));
            const latestDir = sortedDirs[0];
            const url = `gs://${this.config.bucketName}/${pattern}${latestDir}/h3_pfas_${year}_res${resolution}.pmtiles`;
            return {
                url,
                timestamp: latestDir,
                year,
                resolution,
                type: 'h3'
            };
        } catch (error) {
            console.error(`Error discovering H3 tiles for year ${year}, resolution ${resolution}:`, error);
            return null;
        }
    }
    /**
   * Discover latest Kommune tiles for a specific year
   */ async discoverLatestKommuneTiles(year) {
        try {
            const pattern = `gold/pmtiles/kommune_pfas_${year}/`;
            const directories = await this.listGCSDirectories(pattern);
            if (directories.length === 0) {
                return null;
            }
            // Sort directories by timestamp (newest first)
            const sortedDirs = directories.sort((a, b)=>b.localeCompare(a));
            const latestDir = sortedDirs[0];
            const url = `gs://${this.config.bucketName}/${pattern}${latestDir}/kommune_pfas_${year}.pmtiles`;
            return {
                url,
                timestamp: latestDir,
                year,
                type: 'kommune'
            };
        } catch (error) {
            console.error(`Error discovering Kommune tiles for year ${year}:`, error);
            return null;
        }
    }
    /**
   * Discover BNBO tiles
   */ async discoverBNBOTiles() {
        const url = `gs://${this.config.bucketName}/pmtiles/bnbo_areas.pmtiles`;
        return {
            url,
            timestamp: 'static',
            year: 0,
            type: 'bnbo'
        };
    }
    /**
   * Discover basemap tiles
   */ async discoverBasemapTiles() {
        const url = `gs://${this.config.bucketName}/pmtiles/protomaps_denmark.pmtiles`;
        return {
            url,
            timestamp: 'static',
            year: 0,
            type: 'basemap'
        };
    }
    /**
   * Get all available years from the GCS bucket
   */ async getAvailableYears() {
        try {
            const pattern = 'gold/pmtiles/';
            const directories = await this.listGCSDirectories(pattern);
            const years = new Set();
            for (const dir of directories){
                // Extract year from patterns like "h3_pfas_2023_res10" or "kommune_pfas_2023"
                const h3Match = dir.match(/h3_pfas_(\d{4})_res\d+/);
                const kommuneMatch = dir.match(/kommune_pfas_(\d{4})/);
                if (h3Match) {
                    years.add(parseInt(h3Match[1]));
                } else if (kommuneMatch) {
                    years.add(parseInt(kommuneMatch[1]));
                }
            }
            return Array.from(years).sort((a, b)=>b - a); // Newest first
        } catch (error) {
            console.error('Error getting available years:', error);
            return [
                2023,
                2022,
                2021,
                2020,
                2019,
                2018,
                2017,
                2016,
                2015
            ]; // Fallback
        }
    }
    /**
   * Get available resolutions for a specific year
   */ async getAvailableResolutions(year) {
        try {
            const pattern = `gold/pmtiles/`;
            const directories = await this.listGCSDirectories(pattern);
            const resolutions = new Set();
            for (const dir of directories){
                const match = dir.match(new RegExp(`h3_pfas_${year}_res(\\d+)`));
                if (match) {
                    resolutions.add(parseInt(match[1]));
                }
            }
            return Array.from(resolutions).sort((a, b)=>a - b); // Ascending order
        } catch (error) {
            console.error(`Error getting available resolutions for year ${year}:`, error);
            return [
                7,
                8,
                9,
                10
            ]; // Fallback
        }
    }
    /**
   * Preload tiles for a specific year (for faster switching)
   */ async preloadYearData(year) {
        const resolutions = await this.getAvailableResolutions(year);
        // Preload all resolutions for this year
        const preloadPromises = resolutions.map((resolution)=>this.discoverLatestH3Tiles(year, resolution));
        // Also preload kommune data for this year
        preloadPromises.push(this.discoverLatestKommuneTiles(year));
        await Promise.all(preloadPromises);
    }
    /**
   * List directories in GCS bucket (mock implementation)
   * In a real implementation, this would use GCS API
   */ async listGCSDirectories(pattern) {
        // This is a mock implementation
        // In production, you would use the Google Cloud Storage API
        // For now, we'll return hardcoded directory structures based on the pattern
        if (pattern.includes('h3_pfas_')) {
            // Extract year and resolution from pattern
            const match = pattern.match(/h3_pfas_(\d{4})_res(\d+)/);
            if (match) {
                const year = parseInt(match[1]);
                const resolution = parseInt(match[2]);
                // Return mock timestamp directories
                return [
                    `${year}0101_120000`,
                    `${year}0615_120000`,
                    `${year}1201_120000`
                ];
            }
        }
        if (pattern.includes('kommune_pfas_')) {
            const match = pattern.match(/kommune_pfas_(\d{4})/);
            if (match) {
                const year = parseInt(match[1]);
                return [
                    `${year}0101_120000`,
                    `${year}0615_120000`,
                    `${year}1201_120000`
                ];
            }
        }
        if (pattern === 'gold/pmtiles/') {
            // Return all available directory patterns
            const directories = [];
            // Add H3 directories for years 2015-2023 and resolutions 7-10
            for(let year = 2015; year <= 2023; year++){
                for(let res = 7; res <= 10; res++){
                    directories.push(`h3_pfas_${year}_res${res}`);
                }
                directories.push(`kommune_pfas_${year}`);
            }
            return directories;
        }
        return [];
    }
    /**
   * Convert GCS URL to HTTP URL for PMTiles access
   */ convertToHttpUrl(gcsUrl) {
        if (gcsUrl.startsWith('gs://')) {
            return gcsUrl.replace('gs://', 'https://storage.googleapis.com/');
        }
        return gcsUrl;
    }
    /**
   * Clear cache
   */ clearCache() {
        this.cache.clear();
    }
}
// Default configuration
const defaultConfig = {
    bucketName: 'landbrugsdata-raw-data',
    basePath: ''
};
const pmtilesDiscovery = new PMTilesDiscoveryService(defaultConfig);
const __TURBOPACK__default__export__ = PMTilesDiscoveryService;
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/utils/color-utils.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
/**
 * Color Scaling Utilities
 * Implements data-driven color schemes with transparent zero values and white-to-red gradient
 */ __turbopack_context__.s({
    "DATA_FIELD_MAPPINGS": (()=>DATA_FIELD_MAPPINGS),
    "calculateDataStats": (()=>calculateDataStats),
    "createBNBOColorScale": (()=>createBNBOColorScale),
    "createColorExpression": (()=>createColorExpression),
    "createLegendData": (()=>createLegendData),
    "createPesticideColorScale": (()=>createPesticideColorScale),
    "formatLegendValue": (()=>formatLegendValue),
    "getColorScaleForMode": (()=>getColorScaleForMode),
    "getDataField": (()=>getDataField),
    "interpolateColor": (()=>interpolateColor),
    "parseColor": (()=>parseColor),
    "rgbToHex": (()=>rgbToHex)
});
function calculateDataStats(values) {
    const nonZeroValues = values.filter((v)=>v > 0);
    const sortedValues = [
        ...values
    ].sort((a, b)=>a - b);
    const sortedNonZeroValues = [
        ...nonZeroValues
    ].sort((a, b)=>a - b);
    const percentiles = {};
    for(let p = 10; p <= 90; p += 10){
        const index = Math.floor(p / 100 * (sortedValues.length - 1));
        percentiles[p] = sortedValues[index];
    }
    return {
        min: Math.min(...values),
        max: Math.max(...values),
        mean: values.reduce((sum, v)=>sum + v, 0) / values.length,
        median: sortedValues[Math.floor(sortedValues.length / 2)],
        percentiles,
        nonZeroMin: nonZeroValues.length > 0 ? Math.min(...nonZeroValues) : 0,
        nonZeroMax: nonZeroValues.length > 0 ? Math.max(...nonZeroValues) : 0
    };
}
function createPesticideColorScale(stats, buckets = 10) {
    const stops = [];
    // First stop: transparent for zero values
    stops.push([
        0,
        'rgba(0, 0, 0, 0)'
    ]);
    // If we have non-zero values, create gradient from white to red
    if (stats.nonZeroMax > 0) {
        const nonZeroValues = [];
        // Create synthetic data points for decile calculation
        // This is a simplified approach - in production you'd use actual data
        const step = (stats.nonZeroMax - stats.nonZeroMin) / (buckets - 1);
        for(let i = 0; i < buckets; i++){
            nonZeroValues.push(stats.nonZeroMin + step * i);
        }
        // Generate white-to-red gradient
        for(let i = 0; i < buckets; i++){
            const value = nonZeroValues[i];
            const intensity = i / (buckets - 1); // 0 to 1
            // White to red gradient
            const red = 255;
            const green = Math.round(255 * (1 - intensity));
            const blue = Math.round(255 * (1 - intensity));
            const alpha = 0.7 + 0.3 * intensity; // Increase opacity with intensity
            stops.push([
                value,
                `rgba(${red}, ${green}, ${blue}, ${alpha})`
            ]);
        }
    }
    return {
        stops,
        domain: [
            stats.min,
            stats.max
        ],
        buckets
    };
}
function createBNBOColorScale() {
    return {
        stops: [
            [
                0,
                'rgba(0, 0, 0, 0)'
            ],
            [
                1,
                'rgba(255, 107, 107, 0.8)'
            ],
            [
                2,
                'rgba(81, 207, 102, 0.8)'
            ],
            [
                3,
                'rgba(134, 142, 150, 0.8)'
            ] // Unknown - gray
        ],
        domain: [
            0,
            3
        ],
        buckets: 4
    };
}
function createColorExpression(colorScale, dataField) {
    const expression = [
        'case'
    ];
    // Add condition for each stop
    for(let i = 0; i < colorScale.stops.length - 1; i++){
        const [value, color] = colorScale.stops[i];
        const [nextValue] = colorScale.stops[i + 1];
        if (i === 0 && value === 0) {
            // Special case for zero values (transparent)
            expression.push([
                '==',
                [
                    'get',
                    dataField
                ],
                0
            ], color);
        } else {
            // Range condition
            expression.push([
                'all',
                [
                    '>=',
                    [
                        'get',
                        dataField
                    ],
                    value
                ],
                [
                    '<',
                    [
                        'get',
                        dataField
                    ],
                    nextValue
                ]
            ], color);
        }
    }
    // Add final condition for maximum value
    const [lastValue, lastColor] = colorScale.stops[colorScale.stops.length - 1];
    expression.push([
        '>=',
        [
            'get',
            dataField
        ],
        lastValue
    ], lastColor);
    // Default fallback
    expression.push('rgba(0, 0, 0, 0)');
    return expression;
}
function createLegendData(colorScale, unit = 'g/ha') {
    return colorScale.stops.filter(([value])=>value > 0) // Skip transparent zero value
    .map(([value, color])=>({
            value,
            color,
            label: `${value.toFixed(2)} ${unit}`
        }));
}
function getColorScaleForMode(mode, h3Stats, kommuneStats) {
    // Create separate scales for H3 and Kommune data since they have different value ranges
    const h3Scale = h3Stats ? createPesticideColorScale(h3Stats) : createDefaultColorScale();
    const kommuneScale = kommuneStats ? createPesticideColorScale(kommuneStats) : createDefaultColorScale();
    return {
        h3Scale,
        kommuneScale
    };
}
/**
 * Create default color scale when no data is available
 */ function createDefaultColorScale() {
    return {
        stops: [
            [
                0,
                'rgba(0, 0, 0, 0)'
            ],
            [
                1,
                'rgba(255, 255, 255, 0.7)'
            ],
            [
                100,
                'rgba(255, 0, 0, 0.9)'
            ]
        ],
        domain: [
            0,
            100
        ],
        buckets: 10
    };
}
function interpolateColor(color1, color2, factor) {
    const r = Math.round(color1[0] + factor * (color2[0] - color1[0]));
    const g = Math.round(color1[1] + factor * (color2[1] - color1[1]));
    const b = Math.round(color1[2] + factor * (color2[2] - color1[2]));
    return [
        r,
        g,
        b
    ];
}
function rgbToHex(r, g, b) {
    return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
}
function parseColor(colorString) {
    const rgbaMatch = colorString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*[\d.]+)?\)/);
    if (rgbaMatch) {
        return [
            parseInt(rgbaMatch[1]),
            parseInt(rgbaMatch[2]),
            parseInt(rgbaMatch[3])
        ];
    }
    const hexMatch = colorString.match(/^#([0-9a-f]{6})$/i);
    if (hexMatch) {
        const hex = hexMatch[1];
        return [
            parseInt(hex.slice(0, 2), 16),
            parseInt(hex.slice(2, 4), 16),
            parseInt(hex.slice(4, 6), 16)
        ];
    }
    return null;
}
function formatLegendValue(value, unit = 'g/ha') {
    if (value === 0) return '0';
    if (value < 0.01) return `< 0.01 ${unit}`;
    if (value < 1) return `${value.toFixed(2)} ${unit}`;
    if (value < 10) return `${value.toFixed(1)} ${unit}`;
    return `${Math.round(value)} ${unit}`;
}
const DATA_FIELD_MAPPINGS = {
    h3: {
        total: 'pesticide_belastning_per_ha',
        pfas: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
        diquat: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
        glyphosate: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha'
    },
    kommune: {
        total: 'pesticide_belastning_per_ha',
        pfas: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
        diquat: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
        glyphosate: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha'
    }
};
function getDataField(mode, layerType) {
    return DATA_FIELD_MAPPINGS[layerType][mode];
}
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/map/LayerManager.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "LayerManager": (()=>LayerManager)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/utils/color-utils.ts [app-client] (ecmascript)");
;
class LayerManager {
    map;
    currentLayers = new Set();
    colorScales = new Map();
    loadedSources = new Set();
    constructor(map){
        this.map = map;
    }
    /**
   * Initialize the basemap layer
   */ async initializeBasemap(basemapMetadata) {
        const sourceId = 'basemap';
        const layerId = 'basemap-layer';
        // Convert GCS URL to HTTP URL
        const url = this.convertGCSUrl(basemapMetadata.url);
        // Add basemap source
        this.map.addSource(sourceId, {
            type: 'vector',
            url: `pmtiles://${url}`
        });
        // Add basemap layer
        this.map.addLayer({
            id: layerId,
            type: 'fill',
            source: sourceId,
            'source-layer': 'land',
            paint: {
                'fill-color': '#1a1a1a',
                'fill-opacity': 1
            }
        });
        this.currentLayers.add(layerId);
        this.loadedSources.add(sourceId);
    }
    /**
   * Update layer visibility based on zoom level
   */ async updateLayerVisibility(visibility, options) {
        // Handle Kommune layer
        if (visibility.kommune) {
            await this.showKommuneLayer(options);
        } else {
            this.hideKommuneLayer();
        }
        // Handle H3 layer
        if (visibility.h3) {
            await this.showH3Layer(options);
        } else {
            this.hideH3Layer();
        }
    }
    /**
   * Show Kommune layer for low zoom levels
   */ async showKommuneLayer(options) {
        const sourceId = `kommune-${options.year}`;
        const layerId = `kommune-layer-${options.year}`;
        // Remove existing Kommune layers
        this.hideKommuneLayer();
        try {
            // Mock Kommune data URL (in production, use actual discovery service)
            const url = `https://storage.googleapis.com/landbrugsdata-raw-data/gold/pmtiles/kommune_pfas_${options.year}/latest/kommune_pfas_${options.year}.pmtiles`;
            // Add source if not already loaded
            if (!this.loadedSources.has(sourceId)) {
                this.map.addSource(sourceId, {
                    type: 'vector',
                    url: `pmtiles://${url}`
                });
                this.loadedSources.add(sourceId);
            }
            // Create color scale for Kommune data
            const dataField = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getDataField"])(options.dataMode, 'kommune');
            const colorScale = this.getOrCreateColorScale(`kommune-${options.dataMode}`, 'kommune');
            const colorExpression = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createColorExpression"])(colorScale, dataField);
            // Add Kommune layer
            this.map.addLayer({
                id: layerId,
                type: 'fill',
                source: sourceId,
                'source-layer': 'kommune',
                paint: {
                    'fill-color': colorExpression,
                    'fill-opacity': 0.8,
                    'fill-outline-color': '#ffffff'
                }
            });
            this.currentLayers.add(layerId);
        } catch (error) {
            console.error('Error showing Kommune layer:', error);
        }
    }
    /**
   * Hide Kommune layer
   */ hideKommuneLayer() {
        // Remove all Kommune layers
        for (const layerId of this.currentLayers){
            if (layerId.startsWith('kommune-layer-')) {
                if (this.map.getLayer(layerId)) {
                    this.map.removeLayer(layerId);
                }
                this.currentLayers.delete(layerId);
            }
        }
    }
    /**
   * Show H3 layer for high zoom levels
   */ async showH3Layer(options) {
        const sourceId = `h3-${options.year}-res${options.resolution}`;
        const layerId = `h3-layer-${options.year}-res${options.resolution}`;
        // Remove existing H3 layers
        this.hideH3Layer();
        try {
            // Mock H3 data URL (in production, use actual discovery service)
            const url = `https://storage.googleapis.com/landbrugsdata-raw-data/gold/pmtiles/h3_pfas_${options.year}_res${options.resolution}/latest/h3_pfas_${options.year}_res${options.resolution}.pmtiles`;
            // Add source if not already loaded
            if (!this.loadedSources.has(sourceId)) {
                this.map.addSource(sourceId, {
                    type: 'vector',
                    url: `pmtiles://${url}`
                });
                this.loadedSources.add(sourceId);
            }
            // Create color scale for H3 data
            const dataField = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getDataField"])(options.dataMode, 'h3');
            const colorScale = this.getOrCreateColorScale(`h3-${options.dataMode}`, 'h3');
            const colorExpression = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createColorExpression"])(colorScale, dataField);
            // Add H3 layer
            this.map.addLayer({
                id: layerId,
                type: 'fill',
                source: sourceId,
                'source-layer': 'h3',
                paint: {
                    'fill-color': colorExpression,
                    'fill-opacity': 0.7
                }
            });
            this.currentLayers.add(layerId);
        } catch (error) {
            console.error('Error showing H3 layer:', error);
        }
    }
    /**
   * Hide H3 layer
   */ hideH3Layer() {
        // Remove all H3 layers
        for (const layerId of this.currentLayers){
            if (layerId.startsWith('h3-layer-')) {
                if (this.map.getLayer(layerId)) {
                    this.map.removeLayer(layerId);
                }
                this.currentLayers.delete(layerId);
            }
        }
    }
    /**
   * Update data mode (pesticide type) for current layers
   */ async updateDataMode(mode, options) {
        const fullOptions = {
            ...options,
            dataMode: mode
        };
        // Update visible layers
        for (const layerId of this.currentLayers){
            if (layerId.startsWith('kommune-layer-')) {
                const dataField = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getDataField"])(mode, 'kommune');
                const colorScale = this.getOrCreateColorScale(`kommune-${mode}`, 'kommune');
                const colorExpression = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createColorExpression"])(colorScale, dataField);
                this.map.setPaintProperty(layerId, 'fill-color', colorExpression);
            } else if (layerId.startsWith('h3-layer-')) {
                const dataField = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["getDataField"])(mode, 'h3');
                const colorScale = this.getOrCreateColorScale(`h3-${mode}`, 'h3');
                const colorExpression = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createColorExpression"])(colorScale, dataField);
                this.map.setPaintProperty(layerId, 'fill-color', colorExpression);
            }
        }
    }
    /**
   * Update year for current layers
   */ async updateYear(year, options) {
        const fullOptions = {
            ...options,
            year
        };
        // Determine which layers need to be updated
        const needsKommuneUpdate = Array.from(this.currentLayers).some((id)=>id.startsWith('kommune-layer-'));
        const needsH3Update = Array.from(this.currentLayers).some((id)=>id.startsWith('h3-layer-'));
        // Update layers with new year data
        if (needsKommuneUpdate) {
            await this.showKommuneLayer(fullOptions);
        }
        if (needsH3Update) {
            await this.showH3Layer(fullOptions);
        }
    }
    /**
   * Update BNBO layer visibility
   */ updateBNBOVisibility(visible, bnboMetadata) {
        const sourceId = 'bnbo';
        const layerId = 'bnbo-layer';
        if (visible) {
            // Add BNBO layer
            if (!this.loadedSources.has(sourceId)) {
                const url = this.convertGCSUrl(bnboMetadata.url);
                this.map.addSource(sourceId, {
                    type: 'vector',
                    url: `pmtiles://${url}`
                });
                this.loadedSources.add(sourceId);
            }
            if (!this.map.getLayer(layerId)) {
                const colorScale = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createBNBOColorScale"])();
                const colorExpression = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createColorExpression"])(colorScale, 'status_code');
                this.map.addLayer({
                    id: layerId,
                    type: 'fill',
                    source: sourceId,
                    'source-layer': 'bnbo',
                    paint: {
                        'fill-color': colorExpression,
                        'fill-opacity': 0.6,
                        'fill-outline-color': '#ffffff'
                    }
                });
                this.currentLayers.add(layerId);
            }
        } else {
            // Remove BNBO layer
            if (this.map.getLayer(layerId)) {
                this.map.removeLayer(layerId);
                this.currentLayers.delete(layerId);
            }
        }
    }
    /**
   * Get or create color scale for a specific data mode and layer type
   */ getOrCreateColorScale(key, layerType) {
        if (this.colorScales.has(key)) {
            return this.colorScales.get(key);
        }
        // Create mock data statistics for color scale generation
        // In production, this would use actual data statistics
        const mockStats = {
            min: 0,
            max: layerType === 'h3' ? 100 : 50,
            mean: layerType === 'h3' ? 25 : 15,
            median: layerType === 'h3' ? 20 : 12,
            percentiles: {
                10: 1,
                20: 3,
                30: 5,
                40: 8,
                50: layerType === 'h3' ? 20 : 12,
                60: layerType === 'h3' ? 30 : 18,
                70: layerType === 'h3' ? 45 : 25,
                80: layerType === 'h3' ? 65 : 35,
                90: layerType === 'h3' ? 85 : 45
            },
            nonZeroMin: 0.1,
            nonZeroMax: layerType === 'h3' ? 100 : 50
        };
        const colorScale = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["createPesticideColorScale"])(mockStats);
        this.colorScales.set(key, colorScale);
        return colorScale;
    }
    /**
   * Convert GCS URL to HTTP URL for PMTiles access
   */ convertGCSUrl(gcsUrl) {
        if (gcsUrl.startsWith('gs://')) {
            return gcsUrl.replace('gs://', 'https://storage.googleapis.com/');
        }
        return gcsUrl;
    }
    /**
   * Get current layer IDs
   */ getCurrentLayers() {
        return Array.from(this.currentLayers);
    }
    /**
   * Clean up resources
   */ dispose() {
        // Remove all layers
        for (const layerId of this.currentLayers){
            if (this.map.getLayer(layerId)) {
                this.map.removeLayer(layerId);
            }
        }
        // Remove all sources
        for (const sourceId of this.loadedSources){
            if (this.map.getSource(sourceId)) {
                this.map.removeSource(sourceId);
            }
        }
        this.currentLayers.clear();
        this.loadedSources.clear();
        this.colorScales.clear();
    }
}
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/map/Tooltips.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "Tooltips": (()=>Tooltips)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/utils/color-utils.ts [app-client] (ecmascript)");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
;
function Tooltips({ map, layerVisibility, dataMode, year }) {
    _s();
    const [tooltip, setTooltip] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({
        x: 0,
        y: 0,
        visible: false,
        content: null
    });
    const [popup, setPopup] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({
        visible: false,
        content: null,
        position: null
    });
    const popupRef = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    // Set up map event listeners
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Tooltips.useEffect": ()=>{
            const handleMouseMove = {
                "Tooltips.useEffect.handleMouseMove": (e)=>{
                    const features = map.queryRenderedFeatures(e.point);
                    const relevantFeature = findRelevantFeature(features);
                    if (relevantFeature) {
                        setTooltip({
                            x: e.point.x,
                            y: e.point.y,
                            visible: true,
                            content: {
                                type: getFeatureType(relevantFeature),
                                properties: relevantFeature.properties || {}
                            }
                        });
                        // Change cursor to pointer
                        map.getCanvas().style.cursor = 'pointer';
                    } else {
                        setTooltip({
                            "Tooltips.useEffect.handleMouseMove": (prev)=>({
                                    ...prev,
                                    visible: false
                                })
                        }["Tooltips.useEffect.handleMouseMove"]);
                        map.getCanvas().style.cursor = '';
                    }
                }
            }["Tooltips.useEffect.handleMouseMove"];
            const handleClick = {
                "Tooltips.useEffect.handleClick": (e)=>{
                    const features = map.queryRenderedFeatures(e.point);
                    const relevantFeature = findRelevantFeature(features);
                    if (relevantFeature) {
                        const content = {
                            type: getFeatureType(relevantFeature),
                            properties: relevantFeature.properties || {}
                        };
                        // Create or update popup
                        if (popupRef.current) {
                            popupRef.current.remove();
                        }
                        popupRef.current = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["default"].Popup({
                            closeButton: true,
                            closeOnClick: false,
                            maxWidth: '400px'
                        }).setLngLat(e.lngLat).setHTML(renderDetailedPopupContent(content, dataMode, year)).addTo(map);
                        setPopup({
                            visible: true,
                            content,
                            position: [
                                e.lngLat.lng,
                                e.lngLat.lat
                            ]
                        });
                    }
                }
            }["Tooltips.useEffect.handleClick"];
            const handleMouseLeave = {
                "Tooltips.useEffect.handleMouseLeave": ()=>{
                    setTooltip({
                        "Tooltips.useEffect.handleMouseLeave": (prev)=>({
                                ...prev,
                                visible: false
                            })
                    }["Tooltips.useEffect.handleMouseLeave"]);
                    map.getCanvas().style.cursor = '';
                }
            }["Tooltips.useEffect.handleMouseLeave"];
            // Add event listeners
            map.on('mousemove', handleMouseMove);
            map.on('click', handleClick);
            map.on('mouseleave', handleMouseLeave);
            return ({
                "Tooltips.useEffect": ()=>{
                    map.off('mousemove', handleMouseMove);
                    map.off('click', handleClick);
                    map.off('mouseleave', handleMouseLeave);
                    if (popupRef.current) {
                        popupRef.current.remove();
                        popupRef.current = null;
                    }
                }
            })["Tooltips.useEffect"];
        }
    }["Tooltips.useEffect"], [
        map,
        layerVisibility,
        dataMode,
        year
    ]);
    // Find the most relevant feature based on current layer visibility
    const findRelevantFeature = (features)=>{
        if (!features.length) return null;
        // Prioritize based on current visibility
        if (layerVisibility.bnbo) {
            const bnboFeature = features.find((f)=>f.layer.id.includes('bnbo') || f.source === 'bnbo');
            if (bnboFeature) return bnboFeature;
        }
        if (layerVisibility.h3) {
            const h3Feature = features.find((f)=>f.layer.id.includes('h3') || f.source?.toString().includes('h3'));
            if (h3Feature) return h3Feature;
        }
        if (layerVisibility.kommune) {
            const kommuneFeature = features.find((f)=>f.layer.id.includes('kommune') || f.source?.toString().includes('kommune'));
            if (kommuneFeature) return kommuneFeature;
        }
        return features[0];
    };
    // Determine feature type based on layer information
    const getFeatureType = (feature)=>{
        if (feature.layer.id.includes('bnbo') || feature.source === 'bnbo') {
            return 'bnbo';
        }
        if (feature.layer.id.includes('h3') || feature.source?.toString().includes('h3')) {
            return 'h3';
        }
        return 'kommune';
    };
    // Render tooltip content based on feature type
    const renderTooltipContent = (content)=>{
        const { type, properties } = content;
        switch(type){
            case 'h3':
                return renderH3Tooltip(properties, dataMode);
            case 'kommune':
                return renderKommuneTooltip(properties, dataMode);
            case 'bnbo':
                return renderBNBOTooltip(properties);
            default:
                return null;
        }
    };
    // Render H3 cell tooltip
    const renderH3Tooltip = (properties, mode)=>{
        const getValue = (field)=>{
            const value = properties[field];
            return value !== undefined && value !== null ? value : 'N/A';
        };
        const getIntensityField = ()=>{
            switch(mode){
                case 'pfas':
                    return 'pfas_containing_active_ingredient_intensity_grams_per_ha';
                case 'diquat':
                    return 'diquat_containing_active_ingredient_intensity_grams_per_ha';
                case 'glyphosate':
                    return 'glyphosate_containing_active_ingredient_intensity_grams_per_ha';
                default:
                    return 'pesticide_belastning_per_ha';
            }
        };
        const intensityValue = getValue(getIntensityField());
        const formattedValue = typeof intensityValue === 'number' ? (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(intensityValue, 'g/ha') : intensityValue;
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-black/90 text-white p-3 rounded-lg text-sm max-w-xs",
            children: [
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-semibold mb-2",
                    children: "H3 Cell"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 215,
                    columnNumber: 9
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "space-y-1",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Cell ID:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 217,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('h3_cell')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 217,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Area:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 218,
                                    columnNumber: 16
                                }, this),
                                " ",
                                (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('h3_cell_area_ha'), 'ha')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 218,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Fields:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 219,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('unique_field_count')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 219,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: [
                                        mode.toUpperCase(),
                                        " Intensity:"
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 220,
                                    columnNumber: 16
                                }, this),
                                " ",
                                formattedValue
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 220,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Coverage:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 221,
                                    columnNumber: 16
                                }, this),
                                " ",
                                (getValue('actual_coverage_ratio') * 100).toFixed(1),
                                "%"
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 221,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 216,
                    columnNumber: 9
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/Tooltips.tsx",
            lineNumber: 214,
            columnNumber: 7
        }, this);
    };
    // Render Kommune tooltip
    const renderKommuneTooltip = (properties, mode)=>{
        const getValue = (field)=>{
            const value = properties[field];
            return value !== undefined && value !== null ? value : 'N/A';
        };
        const getIntensityField = ()=>{
            switch(mode){
                case 'pfas':
                    return 'pfas_containing_active_ingredient_intensity_grams_per_ha';
                case 'diquat':
                    return 'diquat_containing_active_ingredient_intensity_grams_per_ha';
                case 'glyphosate':
                    return 'glyphosate_containing_active_ingredient_intensity_grams_per_ha';
                default:
                    return 'pesticide_belastning_per_ha';
            }
        };
        const intensityValue = getValue(getIntensityField());
        const formattedValue = typeof intensityValue === 'number' ? (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(intensityValue, 'g/ha') : intensityValue;
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-black/90 text-white p-3 rounded-lg text-sm max-w-xs",
            children: [
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-semibold mb-2",
                    children: "Kommune"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 250,
                    columnNumber: 9
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "space-y-1",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Name:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 252,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('kommune_name')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 252,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Code:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 253,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('kommune_code')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 253,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Agri. Area:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 254,
                                    columnNumber: 16
                                }, this),
                                " ",
                                (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('total_agricultural_area_ha'), 'ha')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 254,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Fields:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 255,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('unique_field_count')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 255,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: [
                                        mode.toUpperCase(),
                                        " Intensity:"
                                    ]
                                }, void 0, true, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 256,
                                    columnNumber: 16
                                }, this),
                                " ",
                                formattedValue
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 256,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 251,
                    columnNumber: 9
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/Tooltips.tsx",
            lineNumber: 249,
            columnNumber: 7
        }, this);
    };
    // Render BNBO tooltip
    const renderBNBOTooltip = (properties)=>{
        const getValue = (field)=>{
            const value = properties[field];
            return value !== undefined && value !== null ? value : 'N/A';
        };
        const getStatusText = (code)=>{
            switch(code){
                case 1:
                    return 'Action Required';
                case 2:
                    return 'Completed';
                case 3:
                    return 'Unknown';
                default:
                    return 'Unknown';
            }
        };
        const statusCode = getValue('status_code');
        const statusText = typeof statusCode === 'number' ? getStatusText(statusCode) : 'Unknown';
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "bg-black/90 text-white p-3 rounded-lg text-sm max-w-xs",
            children: [
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "font-semibold mb-2",
                    children: "BNBO Protected Area"
                }, void 0, false, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 283,
                    columnNumber: 9
                }, this),
                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "space-y-1",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "ID:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 285,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('bnbo_id')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 285,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Status:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 286,
                                    columnNumber: 16
                                }, this),
                                " ",
                                statusText
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 286,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Area:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 287,
                                    columnNumber: 16
                                }, this),
                                " ",
                                (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('area_ha'), 'ha')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 287,
                            columnNumber: 11
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                    className: "text-gray-300",
                                    children: "Description:"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/Tooltips.tsx",
                                    lineNumber: 288,
                                    columnNumber: 16
                                }, this),
                                " ",
                                getValue('description')
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/Tooltips.tsx",
                            lineNumber: 288,
                            columnNumber: 11
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/Tooltips.tsx",
                    lineNumber: 284,
                    columnNumber: 9
                }, this)
            ]
        }, void 0, true, {
            fileName: "[project]/src/components/map/Tooltips.tsx",
            lineNumber: 282,
            columnNumber: 7
        }, this);
    };
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["Fragment"], {
        children: tooltip.visible && tooltip.content && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "fixed pointer-events-none z-50",
            style: {
                left: tooltip.x + 10,
                top: tooltip.y - 10,
                transform: 'translateY(-100%)'
            },
            children: renderTooltipContent(tooltip.content)
        }, void 0, false, {
            fileName: "[project]/src/components/map/Tooltips.tsx",
            lineNumber: 298,
            columnNumber: 9
        }, this)
    }, void 0, false);
}
_s(Tooltips, "fnBKi3eg22rgh5Yzi1Pd/b0NDWE=");
_c = Tooltips;
// Helper function to render detailed popup content (used by MapLibre popup)
function renderDetailedPopupContent(content, dataMode, year) {
    const { type, properties } = content;
    const getValue = (field)=>{
        const value = properties[field];
        return value !== undefined && value !== null ? value : 'N/A';
    };
    switch(type){
        case 'h3':
            return `
        <div class="p-4">
          <h3 class="text-lg font-bold mb-3">H3 Cell Details</h3>
          <div class="space-y-2 text-sm">
            <div><strong>Year:</strong> ${year}</div>
            <div><strong>Cell ID:</strong> ${getValue('h3_cell')}</div>
            <div><strong>Center:</strong> ${getValue('center_lat')?.toFixed(4)}, ${getValue('center_lon')?.toFixed(4)}</div>
            <div><strong>Cell Area:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('h3_cell_area_ha'), 'ha')}</div>
            <div><strong>Coverage Area:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('total_intersection_area_ha'), 'ha')}</div>
            <div><strong>Coverage Ratio:</strong> ${(getValue('actual_coverage_ratio') * 100).toFixed(1)}%</div>
            <div><strong>Unique Fields:</strong> ${getValue('unique_field_count')}</div>
            <div><strong>Crop Diversity:</strong> ${getValue('crop_diversity')}</div>
            <hr class="my-3">
            <div><strong>Total Pesticide:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('pesticide_belastning_per_ha'), 'g/ha')}</div>
            <div><strong>PFAS Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('pfas_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <div><strong>Diquat Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('diquat_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <div><strong>Glyphosate Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('glyphosate_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <hr class="my-3">
            <div><strong>Total Applications:</strong> ${getValue('total_pesticide_applications')}</div>
            <div><strong>PFAS Applications:</strong> ${getValue('pfas_containing_applications')}</div>
            <div><strong>Diquat Applications:</strong> ${getValue('diquat_containing_applications')}</div>
            <div><strong>Glyphosate Applications:</strong> ${getValue('glyphosate_containing_applications')}</div>
          </div>
        </div>
      `;
        case 'kommune':
            return `
        <div class="p-4">
          <h3 class="text-lg font-bold mb-3">Kommune Details</h3>
          <div class="space-y-2 text-sm">
            <div><strong>Year:</strong> ${year}</div>
            <div><strong>Name:</strong> ${getValue('kommune_name')}</div>
            <div><strong>Code:</strong> ${getValue('kommune_code')}</div>
            <div><strong>Region:</strong> ${getValue('region_code')}</div>
            <div><strong>Total Area:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('kommune_area_ha'), 'ha')}</div>
            <div><strong>Agricultural Area:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('total_agricultural_area_ha'), 'ha')}</div>
            <div><strong>Agri. Coverage:</strong> ${getValue('agricultural_coverage_pct')?.toFixed(1)}%</div>
            <div><strong>Unique Fields:</strong> ${getValue('unique_field_count')}</div>
            <div><strong>Unique Companies:</strong> ${getValue('unique_company_count')}</div>
            <div><strong>Crop Diversity:</strong> ${getValue('crop_diversity')}</div>
            <hr class="my-3">
            <div><strong>Total Pesticide:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('pesticide_belastning_per_ha'), 'g/ha')}</div>
            <div><strong>PFAS Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('pfas_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <div><strong>Diquat Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('diquat_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <div><strong>Glyphosate Intensity:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('glyphosate_containing_active_ingredient_intensity_grams_per_ha'), 'g/ha')}</div>
            <hr class="my-3">
            <div><strong>Total Applications:</strong> ${getValue('total_pesticide_applications')}</div>
            <div><strong>PFAS Applications:</strong> ${getValue('pfas_containing_applications')}</div>
            <div><strong>Diquat Applications:</strong> ${getValue('diquat_containing_applications')}</div>
            <div><strong>Glyphosate Applications:</strong> ${getValue('glyphosate_containing_applications')}</div>
            <div><strong>Unique Products:</strong> ${getValue('unique_pesticide_products')}</div>
          </div>
        </div>
      `;
        case 'bnbo':
            const statusCode = getValue('status_code');
            const statusText = typeof statusCode === 'number' ? statusCode === 1 ? 'Action Required' : statusCode === 2 ? 'Completed' : 'Unknown' : 'Unknown';
            return `
        <div class="p-4">
          <h3 class="text-lg font-bold mb-3">BNBO Protected Area</h3>
          <div class="space-y-2 text-sm">
            <div><strong>ID:</strong> ${getValue('bnbo_id')}</div>
            <div><strong>Status:</strong> ${statusText}</div>
            <div><strong>Area:</strong> ${(0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$utils$2f$color$2d$utils$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["formatLegendValue"])(getValue('area_ha'), 'ha')}</div>
            <div><strong>Description:</strong> ${getValue('description')}</div>
            <div><strong>Designation Date:</strong> ${getValue('designation_date')}</div>
            <div><strong>Management Plan:</strong> ${getValue('management_plan') || 'N/A'}</div>
          </div>
        </div>
      `;
        default:
            return '<div class="p-4">No data available</div>';
    }
}
var _c;
__turbopack_context__.k.register(_c, "Tooltips");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/map/NewPMTilesMap.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "default": (()=>NewPMTilesMap)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/maplibre-gl/dist/maplibre-gl.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/pmtiles/dist/index.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/services/pmtiles-discovery.ts [app-client] (ecmascript)");
// Color utilities are handled by LayerManager;
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$LayerManager$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/LayerManager.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$Tooltips$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/Tooltips.tsx [app-client] (ecmascript)");
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
// Register PMTiles protocol
const protocol = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$pmtiles$2f$dist$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["Protocol"]();
__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["default"].addProtocol('pmtiles', protocol.tile);
function NewPMTilesMap({ className = '', onMapLoad, onMapError }) {
    _s();
    const mapContainer = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    const mapRef = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useRef"])(null);
    // State management
    const [mapState, setMapState] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({
        zoom: 6,
        center: [
            12.0,
            56.0
        ],
        bearing: 0,
        pitch: 0,
        isLoaded: false,
        isLoading: true,
        error: null
    });
    const [layerVisibility, setLayerVisibility] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({
        basemap: true,
        kommune: false,
        h3: false,
        bnbo: false
    });
    const [dataMode, setDataMode] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])({
        current: 'total',
        year: 2023,
        availableYears: []
    });
    const [discoveredTiles, setDiscoveredTiles] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    const [layerManager, setLayerManager] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    // Initialize map
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "NewPMTilesMap.useEffect": ()=>{
            if (!mapContainer.current || mapRef.current) return;
            const map = new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$maplibre$2d$gl$2f$dist$2f$maplibre$2d$gl$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["default"].Map({
                container: mapContainer.current,
                style: {
                    version: 8,
                    sources: {},
                    layers: []
                },
                center: mapState.center,
                zoom: mapState.zoom,
                bearing: mapState.bearing,
                pitch: mapState.pitch,
                maxZoom: 18,
                minZoom: 4,
                maxBounds: [
                    [
                        7.0,
                        54.0
                    ],
                    [
                        16.0,
                        58.0
                    ] // Northeast coordinates
                ]
            });
            mapRef.current = map;
            // Initialize layer manager
            const manager = new __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$LayerManager$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["LayerManager"](map);
            setLayerManager(manager);
            // Map event listeners
            map.on('load', {
                "NewPMTilesMap.useEffect": async ()=>{
                    try {
                        setMapState({
                            "NewPMTilesMap.useEffect": (prev)=>({
                                    ...prev,
                                    isLoading: true
                                })
                        }["NewPMTilesMap.useEffect"]);
                        // Discover available tiles
                        const tiles = await __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].discoverAllTiles();
                        setDiscoveredTiles(tiles);
                        // Get available years
                        const years = await __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$services$2f$pmtiles$2d$discovery$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["pmtilesDiscovery"].getAvailableYears();
                        setDataMode({
                            "NewPMTilesMap.useEffect": (prev)=>({
                                    ...prev,
                                    availableYears: years
                                })
                        }["NewPMTilesMap.useEffect"]);
                        // Initialize basemap
                        await manager.initializeBasemap(tiles.basemap);
                        // Set initial layer visibility based on zoom
                        updateLayerVisibility(map.getZoom());
                        setMapState({
                            "NewPMTilesMap.useEffect": (prev)=>({
                                    ...prev,
                                    isLoaded: true,
                                    isLoading: false,
                                    error: null
                                })
                        }["NewPMTilesMap.useEffect"]);
                        onMapLoad?.(map);
                    } catch (error) {
                        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                        setMapState({
                            "NewPMTilesMap.useEffect": (prev)=>({
                                    ...prev,
                                    isLoading: false,
                                    error: errorMessage
                                })
                        }["NewPMTilesMap.useEffect"]);
                        onMapError?.(error instanceof Error ? error : new Error(errorMessage));
                    }
                }
            }["NewPMTilesMap.useEffect"]);
            map.on('zoom', {
                "NewPMTilesMap.useEffect": ()=>{
                    const zoom = map.getZoom();
                    setMapState({
                        "NewPMTilesMap.useEffect": (prev)=>({
                                ...prev,
                                zoom
                            })
                    }["NewPMTilesMap.useEffect"]);
                    updateLayerVisibility(zoom);
                }
            }["NewPMTilesMap.useEffect"]);
            map.on('move', {
                "NewPMTilesMap.useEffect": ()=>{
                    const center = map.getCenter();
                    setMapState({
                        "NewPMTilesMap.useEffect": (prev)=>({
                                ...prev,
                                center: [
                                    center.lng,
                                    center.lat
                                ],
                                bearing: map.getBearing(),
                                pitch: map.getPitch()
                            })
                    }["NewPMTilesMap.useEffect"]);
                }
            }["NewPMTilesMap.useEffect"]);
            map.on('error', {
                "NewPMTilesMap.useEffect": (e)=>{
                    const errorMessage = e.error?.message || 'Map error occurred';
                    setMapState({
                        "NewPMTilesMap.useEffect": (prev)=>({
                                ...prev,
                                error: errorMessage
                            })
                    }["NewPMTilesMap.useEffect"]);
                    onMapError?.(e.error || new Error(errorMessage));
                }
            }["NewPMTilesMap.useEffect"]);
            return ({
                "NewPMTilesMap.useEffect": ()=>{
                    map.remove();
                    mapRef.current = null;
                }
            })["NewPMTilesMap.useEffect"];
        }
    }["NewPMTilesMap.useEffect"], [
        onMapLoad,
        onMapError
    ]);
    // Update layer visibility based on zoom level
    const updateLayerVisibility = (zoom)=>{
        const newVisibility = {
            basemap: true,
            kommune: zoom >= 4 && zoom < 9,
            h3: zoom >= 9,
            bnbo: layerVisibility.bnbo // Maintain user setting
        };
        setLayerVisibility((prev)=>({
                ...prev,
                basemap: newVisibility.basemap,
                kommune: newVisibility.kommune,
                h3: newVisibility.h3
            }));
        // Update actual map layers
        if (layerManager && discoveredTiles) {
            layerManager.updateLayerVisibility(newVisibility, {
                dataMode: dataMode.current,
                year: dataMode.year,
                resolution: getH3ResolutionForZoom(zoom)
            });
        }
    };
    // Get appropriate H3 resolution based on zoom level
    const getH3ResolutionForZoom = (zoom)=>{
        if (zoom < 9) return 7;
        if (zoom < 12) return 8;
        if (zoom < 14) return 9;
        return 10;
    };
    // Handle data mode change
    const handleDataModeChange = async (mode)=>{
        if (!layerManager || !discoveredTiles) return;
        setDataMode((prev)=>({
                ...prev,
                current: mode
            }));
        // Update layer styling for new data mode
        await layerManager.updateDataMode(mode, {
            year: dataMode.year,
            resolution: getH3ResolutionForZoom(mapState.zoom)
        });
    };
    // Handle year change
    const handleYearChange = async (year)=>{
        if (!layerManager || !discoveredTiles) return;
        setDataMode((prev)=>({
                ...prev,
                year
            }));
        // Update layers for new year
        await layerManager.updateYear(year, {
            dataMode: dataMode.current,
            resolution: getH3ResolutionForZoom(mapState.zoom)
        });
    };
    // Handle BNBO toggle
    const handleBNBOToggle = (visible)=>{
        setLayerVisibility((prev)=>({
                ...prev,
                bnbo: visible
            }));
        if (layerManager && discoveredTiles) {
            layerManager.updateBNBOVisibility(visible, discoveredTiles.bnbo);
        }
    };
    // Render loading state
    if (mapState.isLoading) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute inset-0 bg-gray-900 flex items-center justify-center",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-white text-center",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "animate-spin rounded-full h-12 w-12 border-b-2 border-white mx-auto mb-4"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 255,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            children: "Loading map data..."
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 256,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                    lineNumber: 254,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 253,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
            lineNumber: 252,
            columnNumber: 7
        }, this);
    }
    // Render error state
    if (mapState.error) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: `relative ${className}`,
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "absolute inset-0 bg-red-900 flex items-center justify-center",
                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                    className: "text-white text-center max-w-md p-4",
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                            className: "text-xl font-bold mb-2",
                            children: "Map Error"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 269,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                            className: "mb-4",
                            children: mapState.error
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 270,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                            onClick: ()=>window.location.reload(),
                            className: "px-4 py-2 bg-red-700 hover:bg-red-600 rounded transition-colors",
                            children: "Reload Page"
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 271,
                            columnNumber: 13
                        }, this)
                    ]
                }, void 0, true, {
                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                    lineNumber: 268,
                    columnNumber: 11
                }, this)
            }, void 0, false, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 267,
                columnNumber: 9
            }, this)
        }, void 0, false, {
            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
            lineNumber: 266,
            columnNumber: 7
        }, this);
    }
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `relative ${className}`,
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                ref: mapContainer,
                className: "absolute inset-0",
                style: {
                    width: '100%',
                    height: '100%'
                }
            }, void 0, false, {
                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                lineNumber: 286,
                columnNumber: 7
            }, this),
            mapState.isLoaded && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["Fragment"], {
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute top-4 left-4 z-10",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "bg-black/80 backdrop-blur-sm rounded-lg p-3",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                    className: "text-white text-sm font-medium mb-2",
                                    children: "Data Mode"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                    lineNumber: 298,
                                    columnNumber: 15
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "space-y-1",
                                    children: [
                                        'total',
                                        'pfas',
                                        'diquat',
                                        'glyphosate'
                                    ].map((mode)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                            onClick: ()=>handleDataModeChange(mode),
                                            className: `w-full text-left px-3 py-1 rounded text-sm transition-colors ${dataMode.current === mode ? 'bg-white text-black' : 'text-white hover:bg-white/20'}`,
                                            children: mode === 'total' ? 'Total Pesticide' : mode.toUpperCase()
                                        }, mode, false, {
                                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                            lineNumber: 301,
                                            columnNumber: 19
                                        }, this))
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                    lineNumber: 299,
                                    columnNumber: 15
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 297,
                            columnNumber: 13
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 296,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute top-4 left-48 z-10",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "bg-black/80 backdrop-blur-sm rounded-lg p-3",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                    className: "text-white text-sm font-medium mb-2",
                                    children: "Year"
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                    lineNumber: 320,
                                    columnNumber: 15
                                }, this),
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("select", {
                                    value: dataMode.year,
                                    onChange: (e)=>handleYearChange(Number(e.target.value)),
                                    className: "bg-white/20 text-white rounded px-2 py-1 text-sm",
                                    children: dataMode.availableYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("option", {
                                            value: year,
                                            className: "text-black",
                                            children: year
                                        }, year, false, {
                                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                            lineNumber: 327,
                                            columnNumber: 19
                                        }, this))
                                }, void 0, false, {
                                    fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                    lineNumber: 321,
                                    columnNumber: 15
                                }, this)
                            ]
                        }, void 0, true, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 319,
                            columnNumber: 13
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 318,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute top-4 right-4 z-10",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "bg-black/80 backdrop-blur-sm rounded-lg p-3",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("label", {
                                className: "flex items-center space-x-2",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                        type: "checkbox",
                                        checked: layerVisibility.bnbo,
                                        onChange: (e)=>handleBNBOToggle(e.target.checked),
                                        className: "rounded"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                        lineNumber: 339,
                                        columnNumber: 17
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                        className: "text-white text-sm",
                                        children: "BNBO Protected Areas"
                                    }, void 0, false, {
                                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                        lineNumber: 345,
                                        columnNumber: 17
                                    }, this)
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                lineNumber: 338,
                                columnNumber: 15
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 337,
                            columnNumber: 13
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 336,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "absolute bottom-4 left-4 z-10",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "bg-black/80 backdrop-blur-sm rounded px-2 py-1",
                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                className: "text-white text-xs",
                                children: [
                                    "Zoom: ",
                                    mapState.zoom.toFixed(1),
                                    " |",
                                    layerVisibility.kommune ? ' Kommune' : '',
                                    layerVisibility.h3 ? ` H3 (res ${getH3ResolutionForZoom(mapState.zoom)})` : ''
                                ]
                            }, void 0, true, {
                                fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                                lineNumber: 353,
                                columnNumber: 15
                            }, this)
                        }, void 0, false, {
                            fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                            lineNumber: 352,
                            columnNumber: 13
                        }, this)
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 351,
                        columnNumber: 11
                    }, this),
                    mapRef.current && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$Tooltips$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["Tooltips"], {
                        map: mapRef.current,
                        layerVisibility: layerVisibility,
                        dataMode: dataMode.current,
                        year: dataMode.year
                    }, void 0, false, {
                        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
                        lineNumber: 363,
                        columnNumber: 13
                    }, this)
                ]
            }, void 0, true)
        ]
    }, void 0, true, {
        fileName: "[project]/src/components/map/NewPMTilesMap.tsx",
        lineNumber: 284,
        columnNumber: 5
    }, this);
}
_s(NewPMTilesMap, "LCZFfpP8E554fyn6o+viLG04LII=");
_c = NewPMTilesMap;
var _c;
__turbopack_context__.k.register(_c, "NewPMTilesMap");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/stores/ui-store.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "useUIStore": (()=>useUIStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-client] (ecmascript)");
;
;
const useUIStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["persist"])((set)=>({
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
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/components/controls/ThemeToggle.tsx [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "ThemeToggle": (()=>ThemeToggle)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/dist/compiled/react/jsx-dev-runtime.js [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/ui-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$sun$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Sun$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/sun.js [app-client] (ecmascript) <export default as Sun>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$moon$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Moon$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/moon.js [app-client] (ecmascript) <export default as Moon>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$monitor$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Monitor$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/monitor.js [app-client] (ecmascript) <export default as Monitor>");
;
var _s = __turbopack_context__.k.signature();
'use client';
;
;
function ThemeToggle({ className = '' }) {
    _s();
    const { theme, setTheme } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useUIStore"])();
    const themes = [
        {
            id: 'light',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$sun$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Sun$3e$__["Sun"],
            label: 'Light'
        },
        {
            id: 'dark',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$moon$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Moon$3e$__["Moon"],
            label: 'Dark'
        },
        {
            id: 'system',
            icon: __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$monitor$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Monitor$3e$__["Monitor"],
            label: 'System'
        }
    ];
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: `${className}`,
        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "flex items-center space-x-1 bg-black/80 backdrop-blur-sm rounded-lg border border-white/20 p-1",
            children: themes.map(({ id, icon: Icon, label })=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                    onClick: ()=>setTheme(id),
                    className: `flex items-center space-x-1 px-2 py-1 rounded text-xs transition-colors ${theme === id ? 'bg-white/20 text-white' : 'text-gray-300 hover:text-white hover:bg-white/10'}`,
                    title: `Switch to ${label} theme`,
                    children: [
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(Icon, {
                            className: "w-3 h-3"
                        }, void 0, false, {
                            fileName: "[project]/src/components/controls/ThemeToggle.tsx",
                            lineNumber: 33,
                            columnNumber: 13
                        }, this),
                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
_s(ThemeToggle, "9nNFt/ng/Xm4auprrPWJ9IE7vSA=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$ui$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useUIStore"]
    ];
});
_c = ThemeToggle;
var _c;
__turbopack_context__.k.register(_c, "ThemeToggle");
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/stores/pmtiles-store.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "usePMTilesStore": (()=>usePMTilesStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-client] (ecmascript)");
;
;
const usePMTilesStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
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
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/stores/temporal-store.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "useTemporalStore": (()=>useTemporalStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-client] (ecmascript)");
;
;
const useTemporalStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
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
if (typeof globalThis.$RefreshHelpers$ === 'object' && globalThis.$RefreshHelpers !== null) {
    __turbopack_context__.k.registerExports(module, globalThis.$RefreshHelpers$);
}
}}),
"[project]/src/stores/resolution-store.ts [app-client] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname, k: __turbopack_refresh__, m: module } = __turbopack_context__;
{
__turbopack_context__.s({
    "useResolutionStore": (()=>useResolutionStore)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/react.mjs [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/zustand/esm/middleware.mjs [app-client] (ecmascript)");
;
;
const useResolutionStore = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$react$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["create"])()((0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$zustand$2f$esm$2f$middleware$2e$mjs__$5b$app$2d$client$5d$__$28$ecmascript$29$__["persist"])((set, get)=>({
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
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$NewPMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/map/NewPMTilesMap.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$ThemeToggle$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/components/controls/ThemeToggle.tsx [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/pmtiles-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/temporal-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/stores/resolution-store.ts [app-client] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/play.js [app-client] (ecmascript) <export default as Play>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/pause.js [app-client] (ecmascript) <export default as Pause>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-left.js [app-client] (ecmascript) <export default as ChevronLeft>");
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__ = __turbopack_context__.i("[project]/node_modules/lucide-react/dist/esm/icons/chevron-right.js [app-client] (ecmascript) <export default as ChevronRight>");
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
    const [error, setError] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(null);
    const [showBNBO, setShowBNBO] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [showKommune, setShowKommune] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(false);
    const [bnboOpacity, setBnboOpacity] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(0.4);
    const [kommuneOpacity, setKommuneOpacity] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])(0.6);
    const [activeDataLayer, setActiveDataLayer] = (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useState"])('pfas');
    // Store hooks
    const { metadata, metadataLoading, metadataError, setMetadata, setMetadataLoading, setMetadataError } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["usePMTilesStore"])();
    const { currentYear, availableYears, isAnimating, setAvailableYears, setCurrentYear, startAnimation, stopAnimation, goToNextYear, goToPreviousYear, canGoNext, canGoPrevious } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTemporalStore"])();
    const { setResolution, currentResolution } = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useResolutionStore"])();
    // Load PMTiles metadata on mount
    (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$index$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useEffect"])({
        "Home.useEffect": ()=>{
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
        }
    }["Home.useEffect"], [
        setMetadata,
        setMetadataLoading,
        setMetadataError,
        setAvailableYears,
        setCurrentYear,
        setResolution
    ]);
    // Loading state
    if (metadataLoading || !isInitialized) {
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-black flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-6 h-6 border border-white border-t-transparent rounded-full animate-spin mx-auto mb-3"
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 96,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
        return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
            className: "min-h-screen bg-black flex items-center justify-center",
            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "text-center",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "text-red-400 text-sm mb-3",
                        children: error || metadataError
                    }, void 0, false, {
                        fileName: "[project]/src/app/page.tsx",
                        lineNumber: 108,
                        columnNumber: 11
                    }, this),
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
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
    return /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
        className: "min-h-screen bg-black text-white flex flex-col",
        children: [
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "bg-black/90 backdrop-blur-sm border-b border-white/10 px-6 py-4 z-50",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex items-center justify-between",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h1", {
                                        className: "text-lg font-semibold",
                                        children: "PFAS Environmental Impact"
                                    }, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 186,
                                        columnNumber: 13
                                    }, this),
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center space-x-2 bg-white/10 rounded-lg p-1",
                                children: Object.entries(dataLayerConfig).map(([key, config])=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex items-center space-x-4",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$controls$2f$ThemeToggle$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["ThemeToggle"], {}, void 0, false, {
                                        fileName: "[project]/src/app/page.tsx",
                                        lineNumber: 209,
                                        columnNumber: 13
                                    }, this),
                                    metadata && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "text-right text-xs text-gray-400 font-light",
                                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "mt-4 flex items-center justify-center space-x-6",
                        children: [
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: goToPreviousYear,
                                disabled: !canGoPrevious() || isAnimating,
                                className: "p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$left$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronLeft$3e$__["ChevronLeft"], {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: isAnimating ? stopAnimation : startAnimation,
                                disabled: availableYears.length <= 1,
                                className: "p-2 rounded-full bg-white text-black hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: isAnimating ? /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$pause$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Pause$3e$__["Pause"], {
                                    className: "w-4 h-4"
                                }, void 0, false, {
                                    fileName: "[project]/src/app/page.tsx",
                                    lineNumber: 233,
                                    columnNumber: 28
                                }, this) : /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$play$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__Play$3e$__["Play"], {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("button", {
                                onClick: goToNextYear,
                                disabled: !canGoNext() || isAnimating,
                                className: "p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all",
                                children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$lucide$2d$react$2f$dist$2f$esm$2f$icons$2f$chevron$2d$right$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__$3c$export__default__as__ChevronRight$3e$__["ChevronRight"], {
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
                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                className: "flex-1 max-w-md",
                                children: [
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "flex justify-between text-xs text-gray-400 mb-2",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                children: Math.min(...availableYears)
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 247,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "font-bold text-white",
                                                children: currentYear
                                            }, void 0, false, {
                                                fileName: "[project]/src/app/page.tsx",
                                                lineNumber: 248,
                                                columnNumber: 15
                                            }, this),
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                        className: "relative",
                                        children: [
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
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
                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                className: "flex justify-between mt-1",
                                                children: availableYears.map((year)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                className: "flex-1 flex",
                children: [
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "w-80 bg-black/90 backdrop-blur-sm border-r border-white/10 p-6 overflow-y-auto",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                            className: "space-y-6",
                            children: [
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h3", {
                                            className: "text-lg font-semibold mb-2",
                                            children: currentLayerConfig.name
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 284,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
                                            className: "text-sm text-gray-400 mb-4",
                                            children: currentLayerConfig.description
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 285,
                                            columnNumber: 15
                                        }, this),
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                    className: "text-sm font-medium text-gray-300",
                                                    children: "Legend"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 289,
                                                    columnNumber: 17
                                                }, this),
                                                currentLayerConfig.colors.map((item, index)=>/*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                        className: "flex items-center justify-between",
                                                        children: [
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                className: "flex items-center space-x-3",
                                                                children: [
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                        className: `w-4 h-4 ${item.color} rounded border border-white/20`
                                                                    }, void 0, false, {
                                                                        fileName: "[project]/src/app/page.tsx",
                                                                        lineNumber: 293,
                                                                        columnNumber: 23
                                                                    }, this),
                                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                            /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "flex items-center justify-between mb-3",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                            className: "text-sm font-medium",
                                                            children: "Municipality Boundaries"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 308,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("p", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("label", {
                                                    className: "relative inline-flex items-center cursor-pointer",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                            type: "checkbox",
                                                            checked: showKommune,
                                                            onChange: (e)=>setShowKommune(e.target.checked),
                                                            className: "sr-only peer"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 312,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "text-xs text-gray-400 mb-3",
                                            children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                className: "inline-flex items-center space-x-1",
                                                children: [
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                        className: "w-2 h-2 bg-blue-500 rounded-full animate-pulse"
                                                    }, void 0, false, {
                                                        fileName: "[project]/src/app/page.tsx",
                                                        lineNumber: 324,
                                                        columnNumber: 19
                                                    }, this),
                                                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                        showKommune && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex items-center justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-sm",
                                                            children: "Opacity"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 332,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "space-y-2 mt-4",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "text-xs text-gray-400 mb-2",
                                                            children: "Municipal PFAS levels:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 346,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-800 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 348,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-600 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 352,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-400 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 356,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-200 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 360,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "flex items-center justify-between mb-3",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                                    className: "text-sm font-medium",
                                                    children: "BNBO Protected Areas"
                                                }, void 0, false, {
                                                    fileName: "[project]/src/app/page.tsx",
                                                    lineNumber: 371,
                                                    columnNumber: 17
                                                }, this),
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("label", {
                                                    className: "relative inline-flex items-center cursor-pointer",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
                                                            type: "checkbox",
                                                            checked: showBNBO,
                                                            onChange: (e)=>setShowBNBO(e.target.checked),
                                                            className: "sr-only peer"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 373,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
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
                                        showBNBO && /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                            className: "space-y-2",
                                            children: [
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex items-center justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-sm",
                                                            children: "Opacity"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 386,
                                                            columnNumber: 21
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("input", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "space-y-2 mt-4",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-red-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 401,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-green-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 405,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                            className: "flex items-center space-x-3",
                                                            children: [
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                                    className: "w-4 h-4 bg-gray-500 rounded border border-white/20"
                                                                }, void 0, false, {
                                                                    fileName: "[project]/src/app/page.tsx",
                                                                    lineNumber: 409,
                                                                    columnNumber: 23
                                                                }, this),
                                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                    className: "border-t border-white/10 pt-4",
                                    children: [
                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("h4", {
                                            className: "text-sm font-medium mb-3",
                                            children: "Current View"
                                        }, void 0, false, {
                                            fileName: "[project]/src/app/page.tsx",
                                            lineNumber: 419,
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
                                                            lineNumber: 422,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-gray-400",
                                                            children: "Resolution:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 426,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                                                /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                                                    className: "flex justify-between",
                                                    children: [
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
                                                            className: "text-gray-400",
                                                            children: "Data Layer:"
                                                        }, void 0, false, {
                                                            fileName: "[project]/src/app/page.tsx",
                                                            lineNumber: 430,
                                                            columnNumber: 19
                                                        }, this),
                                                        /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("span", {
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
                    /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])("div", {
                        className: "flex-1",
                        children: /*#__PURE__*/ (0, __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$dist$2f$compiled$2f$react$2f$jsx$2d$dev$2d$runtime$2e$js__$5b$app$2d$client$5d$__$28$ecmascript$29$__["jsxDEV"])(__TURBOPACK__imported__module__$5b$project$5d2f$src$2f$components$2f$map$2f$NewPMTilesMap$2e$tsx__$5b$app$2d$client$5d$__$28$ecmascript$29$__["default"], {
                            className: "w-full h-full",
                            onMapLoad: (map)=>{
                                console.log('Map loaded:', map);
                            },
                            onMapError: (error)=>{
                                console.error('Map error:', error);
                                setError(error.message);
                            }
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
_s(Home, "2GqbA8lfR/ExtumhYR/Fte5gDn4=", false, function() {
    return [
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$pmtiles$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["usePMTilesStore"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$temporal$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useTemporalStore"],
        __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$stores$2f$resolution$2d$store$2e$ts__$5b$app$2d$client$5d$__$28$ecmascript$29$__["useResolutionStore"]
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

//# sourceMappingURL=src_31ef9a1b._.js.map