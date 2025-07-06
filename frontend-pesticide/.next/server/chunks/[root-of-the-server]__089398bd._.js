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
// Known timestamps for each year/resolution combination (latest first)
const KNOWN_TIMESTAMPS = {
    '2015_7': [
        '20250705_202051',
        '20250705_195029'
    ],
    '2015_8': [
        '20250705_202226',
        '20250705_195201'
    ],
    '2015_9': [
        '20250705_202609',
        '20250705_195538'
    ],
    '2015_10': [
        '20250705_204039',
        '20250705_200939',
        '20250705_184924',
        '20250705_183222',
        '20250705_181521'
    ],
    '2016_7': [
        '20250705_201958',
        '20250705_194953'
    ],
    '2016_8': [
        '20250705_202137',
        '20250705_195131'
    ],
    '2016_9': [
        '20250705_202527',
        '20250705_195518'
    ],
    '2016_10': [
        '20250705_204032',
        '20250705_201004',
        '20250705_185205',
        '20250705_183413',
        '20250705_181620'
    ],
    '2017_7': [
        '20250705_202003',
        '20250705_195202'
    ],
    '2017_8': [
        '20250705_202137',
        '20250705_195339'
    ],
    '2017_9': [
        '20250705_202515',
        '20250705_195727'
    ],
    '2017_10': [
        '20250705_203930',
        '20250705_201213',
        '20250705_184720',
        '20250705_183104',
        '20250705_181448'
    ],
    '2018_7': [
        '20250705_202052',
        '20250705_195038'
    ],
    '2018_8': [
        '20250705_202233',
        '20250705_195216'
    ],
    '2018_9': [
        '20250705_202627',
        '20250705_195606'
    ],
    '2018_10': [
        '20250705_204131',
        '20250705_201103',
        '20250705_185053',
        '20250705_183328',
        '20250705_181607'
    ],
    '2019_7': [
        '20250705_202058',
        '20250705_195120'
    ],
    '2019_8': [
        '20250705_202243',
        '20250705_195306'
    ],
    '2019_9': [
        '20250705_202642',
        '20250705_195708'
    ],
    '2019_10': [
        '20250705_204158',
        '20250705_201224',
        '20250705_183625',
        '20250705_181740'
    ],
    '2020_7': [
        '20250705_201935',
        '20250705_195125'
    ],
    '2020_8': [
        '20250705_202121',
        '20250705_195314'
    ],
    '2020_9': [
        '20250705_202524',
        '20250705_195723'
    ],
    '2020_10': [
        '20250705_204100',
        '20250705_201321',
        '20250705_185339',
        '20250705_183516',
        '20250705_181657'
    ],
    '2021_7': [
        '20250705_202027',
        '20250705_195049'
    ],
    '2021_8': [
        '20250705_202215',
        '20250705_195236'
    ],
    '2021_9': [
        '20250705_202617',
        '20250705_195639'
    ],
    '2021_10': [
        '20250705_204140',
        '20250705_201213',
        '20250705_183859',
        '20250705_181843'
    ],
    '2022_7': [
        '20250705_202116',
        '20250705_195111'
    ],
    '2022_8': [
        '20250705_202300',
        '20250705_195256'
    ],
    '2022_9': [
        '20250705_202656',
        '20250705_195655'
    ],
    '2022_10': [
        '20250705_204152',
        '20250705_201201',
        '20250705_185258',
        '20250705_183453',
        '20250705_181643'
    ],
    '2023_7': [
        '20250705_201945',
        '20250705_195008'
    ],
    '2023_8': [
        '20250705_202130',
        '20250705_195148'
    ],
    '2023_9': [
        '20250705_202529',
        '20250705_195536'
    ],
    '2023_10': [
        '20250705_204031',
        '20250705_200955',
        '20250705_185039',
        '20250705_183258',
        '20250705_181525'
    ]
};
// Known timestamps for kommune PMTiles (all years use the same timestamp)
const KOMMUNE_TIMESTAMP = '20250705_204103';
async function GET(request, { params }) {
    try {
        const resolvedParams = await params;
        const path = resolvedParams.path.join('/');
        let gcsUrl;
        // Check for H3 PMTiles format: h3_pfas_2023_res10.pmtiles
        const h3Match = path.match(/h3_pfas_(\d{4})_res(\d+)\.pmtiles/);
        if (h3Match) {
            const [, year, resolution] = h3Match;
            const key = `${year}_${resolution}`;
            // Get the latest timestamp for this year/resolution combination
            const timestamps = KNOWN_TIMESTAMPS[key];
            if (!timestamps || timestamps.length === 0) {
                return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                    error: `H3 PMTiles not found for year ${year} resolution ${resolution}`
                }, {
                    status: 404
                });
            }
            const latestTimestamp = timestamps[0];
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
            gcsUrl = `${GCS_BASE_URL}/kommune_pfas_${year}/${KOMMUNE_TIMESTAMP}/kommune_pfas_${year}.pmtiles`;
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