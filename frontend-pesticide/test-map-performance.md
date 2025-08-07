# Map Performance Test Guide

## Performance Improvements Made

### 1. **Event Throttling & Debouncing**
- **Move events**: Throttled to 16ms (~60fps) to prevent excessive state updates
- **Mouse move events**: Throttled to 50ms (~20fps) to reduce feature queries
- **Tooltip hiding**: Debounced to 100ms to prevent flickering

### 2. **Optimized Map Configuration**
- **Fade duration**: Increased to 300ms for smooth transitions (was 0)
- **Drag pan**: Explicitly enabled for better dragging
- **Cooperative gestures**: Disabled for smoother interaction
- **Antialias**: Enabled for better visual quality
- **Render world copies**: Disabled to save memory

### 3. **Performance Monitoring**
- Added development-only FPS counter in top-left corner
- Event count tracking to monitor interaction frequency
- Real-time performance metrics display

### 4. **Memory & Rendering Optimizations**
- Removed excessive console logging and debug queries
- Simplified layer visibility updates
- Optimized useEffect dependencies
- Reduced unnecessary re-renders with useMemo/useCallback

## Testing Instructions

### 1. Start Development Server
```bash
cd frontend-pesticide
npm run dev
```

### 2. Test Map Dragging
- **Before**: Map would stutter or lag during drag operations
- **After**: Should be smooth and responsive
- **Test**: Click and drag the map in various directions

### 3. Test Zoom Performance
- **Before**: Zoom was very slow and janky
- **After**: Should be smooth with proper transitions
- **Test**: Use mouse wheel, zoom controls, or pinch gestures

### 4. Test Hover Performance
- **Before**: Hovering over features caused lag
- **After**: Should be responsive with throttled queries
- **Test**: Move mouse over map features and observe tooltip response

### 5. Monitor Performance Metrics
- **FPS Counter**: Should show ~60fps during smooth operations
- **Event Count**: Should increase gradually, not spike rapidly
- **Browser DevTools**: Check for reduced CPU usage and smoother frame rates

## Expected Results

### ✅ Smooth Dragging
- Map should pan smoothly without stuttering
- No lag when changing direction during drag
- Consistent frame rate during movement

### ✅ Responsive Zooming
- Zoom in/out should be smooth and predictable
- No black screens or layer conflicts
- Proper fade transitions between zoom levels

### ✅ Optimized Interactions
- Hover tooltips appear without lag
- Click interactions are immediate
- No excessive event firing

### ✅ Better Performance
- Lower CPU usage in browser
- Consistent frame rates
- Reduced memory consumption

## Compatibility Notes

### Next.js 15
- Uses proper dynamic imports for browser-only modules
- Compatible with React 19 features
- Optimized for Turbopack development

### Tailwind v4
- Uses standard Tailwind classes
- Compatible with PostCSS configuration
- No custom CSS that conflicts with v4

### MapLibre GL JS
- Uses latest version (5.6.1) with performance optimizations
- Proper event handling for smooth interactions
- Optimized rendering settings

## Troubleshooting

### If dragging is still slow:
1. Check browser console for errors
2. Verify PMTiles URLs are loading correctly
3. Ensure no other heavy processes are running

### If zoom is still janky:
1. Check the fade duration setting (should be 300ms)
2. Verify layer visibility updates are working
3. Monitor performance metrics for bottlenecks

### If performance monitor shows low FPS:
1. Check for console errors or warnings
2. Verify throttling is working correctly
3. Consider reducing data complexity or layer count 