// Debug script to test PMTiles URL accessibility
const baseUrl = 'https://data.pesticidkortet.dk';

const testUrls = [
  `${baseUrl}/pmtiles/protomaps_denmark.pmtiles`,
  `${baseUrl}/pmtiles/bnbo_areas.pmtiles`,
];

// Test dynamic URLs
const year = 2023;
const resolution = 10;

async function testUrl(url, name) {
  try {
    console.log(`Testing ${name}: ${url}`);
    const response = await fetch(url, { method: 'HEAD' });
    console.log(`✅ ${name}: ${response.status} ${response.statusText}`);
    return response.ok;
  } catch (error) {
    console.error(`❌ ${name}: ${error.message}`);
    return false;
  }
}

async function discoverLatestTimestamp(pattern) {
  try {
    const listUrl = `https://storage.googleapis.com/storage/v1/b/landbrugsdata-raw-data/o?prefix=${pattern}/&delimiter=/`;
    console.log(`Discovering timestamps for: ${listUrl}`);

    const response = await fetch(listUrl);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log(`Discovery response:`, data);

    if (data.prefixes && data.prefixes.length > 0) {
      const timestamps = data.prefixes
        .map(prefix => {
          const parts = prefix.split('/');
          return parts[parts.length - 2];
        })
        .filter(ts => /^\d{8}_\d{6}$/.test(ts))
        .sort()
        .reverse();

      console.log(`Found timestamps:`, timestamps);
      return timestamps[0];
    }

    throw new Error('No timestamps found');
  } catch (error) {
    console.error(`Failed to discover timestamp for ${pattern}:`, error);
    throw error;
  }
}

async function testDynamicUrls() {
  try {
    // Test H3 URL
    const h3Pattern = `gold/pmtiles/h3_pfas_${year}_res${resolution}`;
    const h3Timestamp = await discoverLatestTimestamp(h3Pattern);
    const h3Url = `${baseUrl}/${h3Pattern}/${h3Timestamp}/h3_pfas_${year}_res${resolution}.pmtiles`;
    await testUrl(h3Url, 'H3 PMTiles');

    // Test Kommune URL
    const kommunePattern = `gold/pmtiles/kommune_pfas_${year}`;
    const kommuneTimestamp = await discoverLatestTimestamp(kommunePattern);
    const kommuneUrl = `${baseUrl}/${kommunePattern}/${kommuneTimestamp}/kommune_pfas_${year}.pmtiles`;
    await testUrl(kommuneUrl, 'Kommune PMTiles');

  } catch (error) {
    console.error('Error testing dynamic URLs:', error);
  }
}

async function main() {
  console.log('🔍 Testing PMTiles URL accessibility...');

  // Test static URLs
  for (const url of testUrls) {
    const name = url.split('/').pop();
    await testUrl(url, name);
  }

  // Test dynamic URLs
  await testDynamicUrls();

  console.log('✅ PMTiles URL testing complete');
}

main().catch(console.error);
