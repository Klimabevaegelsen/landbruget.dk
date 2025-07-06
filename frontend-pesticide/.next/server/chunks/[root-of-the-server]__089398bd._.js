module.exports = {

"[project]/.next-internal/server/app/api/pmtiles/[...path]/route/actions.js [app-rsc] (server actions loader, ecmascript)": (function(__turbopack_context__) {

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
"[project]/src/app/api/pmtiles/[...path]/route.ts [app-route] (ecmascript)": ((__turbopack_context__) => {
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
// Cache for 1 hour in development, 24 hours in production
const CACHE_DURATION = ("TURBOPACK compile-time falsy", 0) ? ("TURBOPACK unreachable", undefined) : 3600;
// Function to get the latest timestamp for a given H3 PMTiles path
async function getLatestH3Timestamp(year, resolution) {
    try {
        // Use the public GCS XML API to list directories without authentication
        const listUrl = `https://storage.googleapis.com/${BUCKET_NAME}?prefix=${PMTILES_BASE_PATH}/h3_pfas_${year}_res${resolution}/`;
        const response = await fetch(listUrl);
        if (!response.ok) {
            console.error(`Failed to list H3 timestamps for ${year}_${resolution}: ${response.status}`);
            return null;
        }
        const xmlText = await response.text();
        // Parse the XML to extract directory names (timestamps)
        const timestampMatches = xmlText.match(/<Key>[^<]*\/([0-9]{8}_[0-9]{6})\/[^<]*<\/Key>/g);
        if (!timestampMatches) {
            console.error(`No timestamps found in XML for ${year}_${resolution}`);
            return null;
        }
        // Extract timestamps and find the latest
        const timestamps = timestampMatches.map((match)=>{
            const timestampMatch = match.match(/([0-9]{8}_[0-9]{6})/);
            return timestampMatch ? timestampMatch[1] : null;
        }).filter((timestamp)=>timestamp !== null).sort().reverse();
        return timestamps[0] || null;
    } catch (error) {
        console.error(`Error fetching H3 timestamps for ${year}_${resolution}:`, error);
        return null;
    }
}
// Function to get the latest timestamp for kommune PMTiles
async function getLatestKommuneTimestamp(year) {
    try {
        // Use the public GCS XML API to list directories without authentication
        const listUrl = `https://storage.googleapis.com/${BUCKET_NAME}?prefix=${PMTILES_BASE_PATH}/kommune_pfas_${year}/`;
        const response = await fetch(listUrl);
        if (!response.ok) {
            console.error(`Failed to list kommune timestamps for ${year}: ${response.status}`);
            return null;
        }
        const xmlText = await response.text();
        // Parse the XML to extract directory names (timestamps)
        const timestampMatches = xmlText.match(/<Key>[^<]*\/([0-9]{8}_[0-9]{6})\/[^<]*<\/Key>/g);
        if (!timestampMatches) {
            console.error(`No timestamps found in XML for kommune ${year}`);
            return null;
        }
        // Extract timestamps and find the latest
        const timestamps = timestampMatches.map((match)=>{
            const timestampMatch = match.match(/([0-9]{8}_[0-9]{6})/);
            return timestampMatch ? timestampMatch[1] : null;
        }).filter((timestamp)=>timestamp !== null).sort().reverse();
        return timestamps[0] || null;
    } catch (error) {
        console.error(`Error fetching kommune timestamps for ${year}:`, error);
        return null;
    }
}
async function GET(request, { params }) {
    try {
        const resolvedParams = await params;
        const path = resolvedParams.path.join('/');
        let gcsUrl;
        // Check for H3 PMTiles format: h3_pfas_2023_res10.pmtiles
        const h3Match = path.match(/h3_pfas_(\d{4})_res(\d+)\.pmtiles/);
        if (h3Match) {
            const [, year, resolution] = h3Match;
            // Get the latest timestamp for this year/resolution combination
            const latestTimestamp = await getLatestH3Timestamp(year, resolution);
            if (!latestTimestamp) {
                return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                    error: `H3 PMTiles not found for year ${year} resolution ${resolution}`
                }, {
                    status: 404
                });
            }
            gcsUrl = `${GCS_BASE_URL}/h3_pfas_${year}_res${resolution}/${latestTimestamp}/h3_pfas_${year}_res${resolution}.pmtiles`;
        } else {
            const kommuneMatch = path.match(/kommune_pfas_(\d{4})\.pmtiles/);
            if (!kommuneMatch) {
                return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                    error: 'Invalid PMTiles path format. Expected h3_pfas_YYYY_resN.pmtiles or kommune_pfas_YYYY.pmtiles'
                }, {
                    status: 400
                });
            }
            const [, year] = kommuneMatch;
            // Validate year
            const validYears = [
                '2015',
                '2016',
                '2017',
                '2018',
                '2019',
                '2020',
                '2021',
                '2022',
                '2023'
            ];
            if (!validYears.includes(year)) {
                return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                    error: `Kommune PMTiles not found for year ${year}`
                }, {
                    status: 404
                });
            }
            // Get the latest timestamp for this year
            const latestTimestamp = await getLatestKommuneTimestamp(year);
            if (!latestTimestamp) {
                return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                    error: `Kommune PMTiles not found for year ${year}`
                }, {
                    status: 404
                });
            }
            gcsUrl = `${GCS_BASE_URL}/kommune_pfas_${year}/${latestTimestamp}/kommune_pfas_${year}.pmtiles`;
        }
        // Handle range requests for PMTiles
        const range = request.headers.get('range');
        const headers = {
            'Accept-Ranges': 'bytes',
            'Cache-Control': `public, max-age=${CACHE_DURATION}`
        };
        if (range) {
            headers['Range'] = range;
        }
        // Fetch from GCS
        const response = await fetch(gcsUrl, {
            headers
        });
        if (!response.ok) {
            return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                error: `Failed to fetch PMTiles from GCS: ${response.status}`
            }, {
                status: response.status
            });
        }
        // Get the response body
        const buffer = await response.arrayBuffer();
        // Copy response headers
        const responseHeaders = {
            'Content-Type': 'application/octet-stream',
            'Content-Length': buffer.byteLength.toString(),
            'Accept-Ranges': 'bytes',
            'Cache-Control': `public, max-age=${CACHE_DURATION}`
        };
        // Copy range-related headers if present
        if (response.headers.get('content-range')) {
            responseHeaders['Content-Range'] = response.headers.get('content-range');
        }
        if (response.headers.get('etag')) {
            responseHeaders['ETag'] = response.headers.get('etag');
        }
        if (response.headers.get('last-modified')) {
            responseHeaders['Last-Modified'] = response.headers.get('last-modified');
        }
        return new __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"](buffer, {
            status: response.status,
            headers: responseHeaders
        });
    } catch (error) {
        console.error('PMTiles proxy error:', error);
        return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
            error: 'Failed to fetch PMTiles',
            details: error instanceof Error ? error.message : 'Unknown error'
        }, {
            status: 500
        });
    }
}
}}),

};

//# sourceMappingURL=%5Broot-of-the-server%5D__089398bd._.js.map