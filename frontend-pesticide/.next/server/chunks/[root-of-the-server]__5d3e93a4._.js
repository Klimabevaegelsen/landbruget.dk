module.exports = {

"[project]/.next-internal/server/app/api/metadata/route/actions.js [app-rsc] (server actions loader, ecmascript)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
}}),
"[externals]/next/dist/compiled/next-server/app-route-turbo.runtime.dev.js [external] (next/dist/compiled/next-server/app-route-turbo.runtime.dev.js, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/compiled/next-server/app-route-turbo.runtime.dev.js", () => require("next/dist/compiled/next-server/app-route-turbo.runtime.dev.js"));

module.exports = mod;
}}),
"[externals]/next/dist/compiled/@opentelemetry/api [external] (next/dist/compiled/@opentelemetry/api, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/compiled/@opentelemetry/api", () => require("next/dist/compiled/@opentelemetry/api"));

module.exports = mod;
}}),
"[externals]/next/dist/compiled/next-server/app-page-turbo.runtime.dev.js [external] (next/dist/compiled/next-server/app-page-turbo.runtime.dev.js, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/compiled/next-server/app-page-turbo.runtime.dev.js", () => require("next/dist/compiled/next-server/app-page-turbo.runtime.dev.js"));

module.exports = mod;
}}),
"[externals]/next/dist/server/app-render/work-unit-async-storage.external.js [external] (next/dist/server/app-render/work-unit-async-storage.external.js, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/server/app-render/work-unit-async-storage.external.js", () => require("next/dist/server/app-render/work-unit-async-storage.external.js"));

module.exports = mod;
}}),
"[externals]/next/dist/server/app-render/work-async-storage.external.js [external] (next/dist/server/app-render/work-async-storage.external.js, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/server/app-render/work-async-storage.external.js", () => require("next/dist/server/app-render/work-async-storage.external.js"));

module.exports = mod;
}}),
"[externals]/next/dist/server/app-render/after-task-async-storage.external.js [external] (next/dist/server/app-render/after-task-async-storage.external.js, cjs)": (function(__turbopack_context__) {

var { g: global, __dirname, m: module, e: exports } = __turbopack_context__;
{
const mod = __turbopack_context__.x("next/dist/server/app-render/after-task-async-storage.external.js", () => require("next/dist/server/app-render/after-task-async-storage.external.js"));

module.exports = mod;
}}),
"[project]/src/app/api/metadata/route.ts [app-route] (ecmascript)": ((__turbopack_context__) => {
"use strict";

var { g: global, __dirname } = __turbopack_context__;
{
__turbopack_context__.s({
    "GET": (()=>GET)
});
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/server.js [app-route] (ecmascript)");
;
const BUCKET_NAME = 'landbrugsdata-raw-data';
const PMTILES_BASE_PATH = 'gold/pmtiles';
const GCS_BASE_URL = `https://storage.googleapis.com/${BUCKET_NAME}/${PMTILES_BASE_PATH}`;
async function GET(request) {
    try {
        // Create static metadata based on known file structure
        // Years: 2015-2023, Resolutions: 7-10
        const years = [
            2015,
            2016,
            2017,
            2018,
            2019,
            2020,
            2021,
            2022,
            2023
        ];
        const resolutions = [
            7,
            8,
            9,
            10
        ];
        const pmtilesFiles = [];
        // Generate file entries for all year/resolution combinations
        for (const year of years){
            for (const resolution of resolutions){
                pmtilesFiles.push({
                    year,
                    resolution,
                    timestamp: '20250705_181521',
                    size: 0,
                    url: `/api/pmtiles/h3_pfas_${year}_res${resolution}.pmtiles`,
                    lastModified: new Date().toISOString()
                });
            }
        }
        // Add BNBO PMTiles information
        const bnboFiles = [
            {
                filename: 'bnbo_areas.pmtiles',
                size: 4685085,
                url: 'https://storage.googleapis.com/landbrugsdata-raw-data/pmtiles/bnbo_areas.pmtiles',
                lastModified: new Date().toISOString(),
                type: 'bnbo_areas'
            }
        ];
        // Add Kommune PMTiles information
        const kommuneFiles = [];
        for (const year of years){
            kommuneFiles.push({
                filename: `kommune_pfas_${year}.pmtiles`,
                year,
                timestamp: '20250705_204103',
                size: 0,
                url: `/api/pmtiles/kommune_pfas_${year}.pmtiles`,
                lastModified: new Date().toISOString(),
                type: 'kommune_pfas'
            });
        }
        const metadata = {
            files: pmtilesFiles,
            years,
            resolutions,
            totalFiles: pmtilesFiles.length,
            lastUpdated: new Date().toISOString(),
            bnbo: {
                files: bnboFiles,
                available: true,
                status_codes: [
                    'Action Required',
                    'Completed',
                    'Unknown'
                ],
                status_colors: {
                    'Action Required': '#ff6b6b',
                    'Completed': '#51cf66',
                    'Unknown': '#868e96'
                }
            },
            kommune: {
                files: kommuneFiles,
                available: true,
                years,
                zoom_levels: {
                    min: 3,
                    max: 12,
                    base: 6
                }
            }
        };
        return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json(metadata, {
            headers: {
                'Cache-Control': 'public, max-age=300',
                'Content-Type': 'application/json'
            }
        });
    } catch (error) {
        console.error('Error generating PMTiles metadata:', error);
        return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
            error: 'Failed to generate metadata'
        }, {
            status: 500
        });
    }
}
}}),

};

//# sourceMappingURL=%5Broot-of-the-server%5D__5d3e93a4._.js.map