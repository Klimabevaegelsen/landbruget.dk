// Quick Test Script for Field Analysis Visualization Fixes
// Copy and paste this entire script into Chrome DevTools Console at http://localhost:3000/markanalyse

(function() {
  console.clear();
  console.log('%c🧪 Field Analysis Visualization - Quick Test Suite', 'font-size: 16px; font-weight: bold; color: #00ff00');
  console.log('%c==========================================', 'color: #00ff00');

  // Wait for map to load
  setTimeout(() => {
    try {
      // Find map instance
      const map = document.querySelector('[data-testid="field-analysis-map"]')?.__maplibre_map__ ||
                  window.__maplibre_map__;

      if (!map) {
        console.error('❌ Map instance not found. Make sure you\'re on /markanalyse page');
        return;
      }

      console.log('\n%c✅ Map instance found!', 'color: green; font-weight: bold');

      // Test 1: Zoom-based opacity
      console.log('\n%c📋 Test 1: Zoom-Based Opacity', 'font-weight: bold');
      const fillOpacity = map.getPaintProperty('fields-fill', 'fill-opacity');
      const hasZoomOpacity = Array.isArray(fillOpacity) && fillOpacity[0] === 'interpolate';

      if (hasZoomOpacity) {
        console.log('✅ Zoom-based opacity: ACTIVE');
        console.log('   Details:', fillOpacity);
      } else {
        console.log('❌ Zoom-based opacity: NOT FOUND');
      }

      // Test 2: Zoom-based line width
      console.log('\n%c📋 Test 2: Zoom-Based Line Width', 'font-weight: bold');
      const lineWidth = map.getPaintProperty('fields-outline', 'line-width');
      const hasZoomWidth = Array.isArray(lineWidth) && lineWidth[0] === 'interpolate';

      if (hasZoomWidth) {
        console.log('✅ Zoom-based line width: ACTIVE');
        console.log('   Details:', lineWidth);
      } else {
        console.log('❌ Zoom-based line width: NOT FOUND');
      }

      // Test 3: Minzoom setting
      console.log('\n%c📋 Test 3: Minzoom Setting', 'font-weight: bold');
      const fieldsLayer = map.getLayer('fields-fill');
      const hasMinzoom = fieldsLayer && fieldsLayer.minzoom === 6;

      if (hasMinzoom) {
        console.log('✅ Minzoom set to 6: CORRECT');
      } else {
        console.log('❌ Minzoom: NOT SET or INCORRECT');
      }

      // Test 4: Color expression
      console.log('\n%c📋 Test 4: Color Expression', 'font-weight: bold');
      const fillColor = map.getPaintProperty('fields-fill', 'fill-color');
      const hasColorExpr = Array.isArray(fillColor) && fillColor.length > 5;

      if (hasColorExpr) {
        console.log('✅ Color expression: PRESENT');
        console.log(`   Expression has ${fillColor.length} elements (10-color scheme)`);
      } else {
        console.log('❌ Color expression: MISSING or INCOMPLETE');
      }

      // Test 5: Rendered fields
      console.log('\n%c📋 Test 5: Rendered Fields', 'font-weight: bold');
      const features = map.queryRenderedFeatures({ layers: ['fields-fill'] });
      const currentZoom = map.getZoom();

      if (features.length > 0) {
        console.log(`✅ Fields rendered: ${features.length} visible`);
        console.log(`   Current zoom: ${currentZoom.toFixed(2)}`);
        console.log('   Sample field:', features[0].properties);
      } else {
        if (currentZoom < 6) {
          console.log('⚠️ No fields visible (zoom < 6 - expected behavior)');
        } else {
          console.log('❌ No fields visible (unexpected at this zoom level)');
        }
      }

      // Summary
      const allPassed = hasZoomOpacity && hasZoomWidth && hasMinzoom && hasColorExpr;
      console.log('\n%c' + '='.repeat(50), 'color: #00ff00');

      if (allPassed) {
        console.log('%c✅ ALL TESTS PASSED!', 'font-size: 18px; font-weight: bold; color: green; background: #e6ffe6; padding: 10px');
      } else {
        console.log('%c⚠️ SOME TESTS FAILED', 'font-size: 18px; font-weight: bold; color: orange; background: #fff4e6; padding: 10px');
      }
      console.log('%c' + '='.repeat(50), 'color: #00ff00');

      // Interactive commands
      console.log('\n%c🎮 Interactive Commands:', 'font-weight: bold; color: blue');
      console.log('%cTry these commands to test zoom behavior:', 'color: gray');
      console.log('  map.easeTo({zoom: 6, duration: 1000})  → Test faded view');
      console.log('  map.easeTo({zoom: 10, duration: 1000}) → Test medium zoom');
      console.log('  map.easeTo({zoom: 14, duration: 1000}) → Test full detail');
      console.log('\n%cThe map variable is available for further testing!', 'color: gray');

      // Make map globally available
      window.__test_map__ = map;

    } catch (error) {
      console.error('❌ Test suite failed with error:', error);
    }
  }, 2000); // Wait 2 seconds for map to initialize

  console.log('\n⏳ Waiting for map to initialize...\n');
})();
