# Field Analysis Visualization - Interactive Test Guide

## Prerequisites
✅ Dev server running at http://localhost:3000
✅ Chrome DevTools MCP configured in your workspace

## Test 1: Color Consistency Between Legend and Map

### Steps:
1. Open http://localhost:3000/markanalyse in Chrome
2. Wait for the map to load completely
3. Open Chrome DevTools (F12 or Cmd+Option+I)
4. Run this test script in Console:

```javascript
// Test 1: Verify Color Schemes Match
console.log('🧪 TEST 1: Color Consistency');

// Wait for map to be ready
setTimeout(() => {
  try {
    // Find the map instance
    const mapContainer = document.querySelector('[data-testid="field-analysis-map"]');
    if (!mapContainer) {
      console.error('❌ Map container not found');
      return;
    }

    // Access MapLibre instance (it's attached to the DOM element)
    const map = mapContainer.__maplibre_map__ ||
                window.__maplibre_map__;

    if (!map) {
      console.error('❌ Map instance not found');
      return;
    }

    console.log('✅ Map instance found');

    // Check if fields layer exists
    const fieldsLayer = map.getLayer('fields-fill');
    if (!fieldsLayer) {
      console.error('❌ Fields layer not found');
      return;
    }

    console.log('✅ Fields layer found');

    // Get the fill-color expression
    const fillColor = map.getPaintProperty('fields-fill', 'fill-color');
    console.log('Fill color expression:', fillColor);

    // Get the fill-opacity expression
    const fillOpacity = map.getPaintProperty('fields-fill', 'fill-opacity');
    console.log('Fill opacity expression (should be zoom-based):', fillOpacity);

    // Check if it's zoom-based
    if (Array.isArray(fillOpacity) && fillOpacity[0] === 'interpolate') {
      console.log('✅ Zoom-based opacity is active!');
      console.log('   - At zoom 6: 30% opacity');
      console.log('   - At zoom 10: 70% opacity');
      console.log('   - At zoom 14+: 70% opacity');
    } else {
      console.warn('⚠️ Zoom-based opacity not detected');
    }

    // Query some fields to see their colors
    const features = map.queryRenderedFeatures({
      layers: ['fields-fill']
    });

    if (features.length > 0) {
      console.log(`✅ Found ${features.length} visible fields`);
      console.log('Sample field data:', features[0].properties);
    } else {
      console.warn('⚠️ No fields visible at current zoom level');
    }

  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}, 3000); // Wait 3 seconds for map to load
```

### Expected Results:
- ✅ Map instance found
- ✅ Fields layer found
- ✅ Zoom-based opacity is active
- ✅ Fill color expression shows 10-color gradient
- ✅ Sample field data displays

### Manual Verification:
1. Toggle between visualization modes:
   - Click "Total Pesticide" → "PFAS" → "Glyphosate"
2. **Check**: Legend colors match field colors on map
3. Toggle "Decile farvning" on/off
4. **Check**: Legend updates to show decile breakpoints

---

## Test 2: Zoom-Based Simplification

### Steps:
1. Ensure you're on the map view
2. Run this test in Chrome DevTools Console:

```javascript
// Test 2: Verify Zoom-Based Styling
console.log('🧪 TEST 2: Zoom-Based Simplification');

const map = document.querySelector('[data-testid="field-analysis-map"]').__maplibre_map__ ||
            window.__maplibre_map__;

if (!map) {
  console.error('❌ Map not found');
} else {
  // Get current zoom level
  const currentZoom = map.getZoom();
  console.log(`Current zoom level: ${currentZoom.toFixed(2)}`);

  // Check minzoom setting
  const fieldsLayer = map.getLayer('fields-fill');
  if (fieldsLayer && fieldsLayer.minzoom) {
    console.log(`✅ Minzoom set to: ${fieldsLayer.minzoom}`);
    console.log('   Fields will not render below zoom 6');
  } else {
    console.warn('⚠️ Minzoom not detected');
  }

  // Check line width expression
  const lineWidth = map.getPaintProperty('fields-outline', 'line-width');
  if (Array.isArray(lineWidth) && lineWidth[0] === 'interpolate') {
    console.log('✅ Zoom-based line width is active!');
    console.log('   - At zoom 6: 0px (no outline)');
    console.log('   - At zoom 10: 0.3px (thin)');
    console.log('   - At zoom 14+: 0.5px (normal)');
  } else {
    console.warn('⚠️ Zoom-based line width not detected');
  }

  // Interactive zoom test
  console.log('\n📍 Interactive Test:');
  console.log('Try these zoom levels and observe:');
  console.log('• map.easeTo({zoom: 6, duration: 1000})  - Should be faded (30% opacity)');
  console.log('• map.easeTo({zoom: 10, duration: 1000}) - Should be clear (70% opacity)');
  console.log('• map.easeTo({zoom: 14, duration: 1000}) - Should show outlines');
}
```

### Manual Verification:
1. **Zoom to level 6**:
   ```javascript
   map.easeTo({zoom: 6, duration: 1000})
   ```
   - **Expected**: Fields at 30% opacity, no outlines, less clutter

2. **Zoom to level 10**:
   ```javascript
   map.easeTo({zoom: 10, duration: 1000})
   ```
   - **Expected**: Fields at 70% opacity, thin outlines appear

3. **Zoom to level 14**:
   ```javascript
   map.easeTo({zoom: 14, duration: 1000})
   ```
   - **Expected**: Full detail with 0.5px outlines

---

## Test 3: Performance on State Changes

### Steps:
1. Run this performance monitoring script:

```javascript
// Test 3: Performance Testing
console.log('🧪 TEST 3: Performance Monitoring');

const map = document.querySelector('[data-testid="field-analysis-map"]').__maplibre_map__ ||
            window.__maplibre_map__;

if (!map) {
  console.error('❌ Map not found');
} else {
  // Monitor paint property updates
  let updateCount = 0;
  const startTime = Date.now();

  console.log('Monitoring paint property updates for 10 seconds...');
  console.log('Try changing visualization modes rapidly!');

  const originalSetPaintProperty = map.setPaintProperty.bind(map);
  map.setPaintProperty = function(...args) {
    updateCount++;
    console.log(`Paint update #${updateCount}: ${args[0]}.${args[1]}`);
    return originalSetPaintProperty(...args);
  };

  setTimeout(() => {
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`\n📊 Performance Summary (${elapsed.toFixed(1)}s):`);
    console.log(`   Total paint updates: ${updateCount}`);
    console.log(`   Average: ${(updateCount / elapsed).toFixed(2)} updates/sec`);

    if (updateCount < 20) {
      console.log('✅ Good! Updates are batched efficiently');
    } else if (updateCount < 50) {
      console.log('⚠️ Moderate - Some duplicate updates detected');
    } else {
      console.log('❌ Too many updates - optimization needed');
    }

    // Restore original function
    map.setPaintProperty = originalSetPaintProperty;
  }, 10000);
}
```

### Manual Actions During Test:
1. Rapidly click between visualization modes:
   - Total Pesticide → PFAS → Glyphosate → Applications
2. Change year selection multiple times
3. Toggle "Decile farvning" on/off several times

### Expected Results:
- ✅ Updates should be minimal (< 20 in 10 seconds)
- ✅ No visual flicker during transitions
- ✅ Smooth animations

---

## Test 4: Verify Ref-Based Guards

### Steps:
1. Run this test to verify duplicate update prevention:

```javascript
// Test 4: Duplicate Update Prevention
console.log('🧪 TEST 4: Ref-Based Guards');

const map = document.querySelector('[data-testid="field-analysis-map"]').__maplibre_map__ ||
            window.__maplibre_map__;

if (!map) {
  console.error('❌ Map not found');
} else {
  console.log('Testing duplicate update prevention...');

  // Monitor consecutive identical updates
  let lastPaintProps = null;
  let duplicateCount = 0;

  const originalSetPaintProperty = map.setPaintProperty.bind(map);
  map.setPaintProperty = function(layer, prop, value) {
    const key = `${layer}.${prop}`;
    const valueStr = JSON.stringify(value);

    if (lastPaintProps === valueStr) {
      duplicateCount++;
      console.warn(`⚠️ Duplicate update detected: ${key}`);
    } else {
      lastPaintProps = valueStr;
      console.log(`✅ New update: ${key}`);
    }

    return originalSetPaintProperty(layer, prop, value);
  };

  console.log('\nNow change visualization modes...');

  setTimeout(() => {
    console.log(`\n📊 Duplicate Update Summary:`);
    console.log(`   Duplicates detected: ${duplicateCount}`);

    if (duplicateCount === 0) {
      console.log('✅ Perfect! No duplicate updates');
    } else if (duplicateCount < 3) {
      console.log('⚠️ Minor duplicates detected');
    } else {
      console.log('❌ Too many duplicates - ref guards may not be working');
    }

    // Restore
    map.setPaintProperty = originalSetPaintProperty;
  }, 15000);
}
```

### Expected Results:
- ✅ Zero or very few duplicate updates
- ✅ Each mode change triggers exactly one update

---

## Test 5: Overall Integration Test

### Quick Interactive Test:
```javascript
// Test 5: Full Integration Test
console.log('🧪 TEST 5: Integration Test');

const runFullTest = async () => {
  const map = document.querySelector('[data-testid="field-analysis-map"]').__maplibre_map__ ||
              window.__maplibre_map__;

  if (!map) {
    console.error('❌ Map not found');
    return;
  }

  console.log('Running full integration test...\n');

  // 1. Check zoom-based opacity
  const fillOpacity = map.getPaintProperty('fields-fill', 'fill-opacity');
  const hasZoomOpacity = Array.isArray(fillOpacity) && fillOpacity[0] === 'interpolate';
  console.log(`1. Zoom-based opacity: ${hasZoomOpacity ? '✅' : '❌'}`);

  // 2. Check zoom-based line width
  const lineWidth = map.getPaintProperty('fields-outline', 'line-width');
  const hasZoomWidth = Array.isArray(lineWidth) && lineWidth[0] === 'interpolate';
  console.log(`2. Zoom-based line width: ${hasZoomWidth ? '✅' : '❌'}`);

  // 3. Check minzoom
  const fieldsLayer = map.getLayer('fields-fill');
  const hasMinzoom = fieldsLayer && fieldsLayer.minzoom === 6;
  console.log(`3. Minzoom set to 6: ${hasMinzoom ? '✅' : '❌'}`);

  // 4. Query fields
  const features = map.queryRenderedFeatures({ layers: ['fields-fill'] });
  const hasFields = features.length > 0;
  console.log(`4. Fields rendered: ${hasFields ? '✅' : '❌'} (${features.length} visible)`);

  // 5. Check for color expression
  const fillColor = map.getPaintProperty('fields-fill', 'fill-color');
  const hasColorExpr = Array.isArray(fillColor);
  console.log(`5. Color expression present: ${hasColorExpr ? '✅' : '❌'}`);

  // Summary
  const allPassed = hasZoomOpacity && hasZoomWidth && hasMinzoom && hasFields && hasColorExpr;
  console.log(`\n${'='.repeat(50)}`);
  console.log(allPassed ? '✅ ALL TESTS PASSED!' : '⚠️ Some tests failed - see above');
  console.log('='.repeat(50));
};

runFullTest();
```

---

## Summary Checklist

After running all tests, verify:

- [ ] Legend colors match map field colors
- [ ] Zoom to level 6 shows faded fields (30% opacity)
- [ ] Zoom to level 14 shows clear fields with outlines
- [ ] Changing visualization modes is smooth (no flicker)
- [ ] Changing years loads quickly
- [ ] No duplicate paint updates in console
- [ ] All 5 visualization modes work correctly
- [ ] Decile coloring toggle works

---

## Troubleshooting

If tests fail:

1. **Map not found**: Ensure you're on `/markanalyse` page
2. **No fields visible**: Zoom in closer or pan to Denmark
3. **Console errors**: Check browser console for MapLibre errors
4. **Performance issues**: Check Network tab for slow PMTiles loads

---

## Need Help?

If any test fails, share:
1. The test that failed
2. Console error messages
3. Current zoom level
4. Browser version

The dev server is running at: http://localhost:3000
