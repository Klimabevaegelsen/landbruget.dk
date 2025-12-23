# Visual Testing Guide - What to Look For

## 🎯 Quick Visual Tests (5 minutes)

### Test 1: Color Matching ✅
**What to do:**
1. Open http://localhost:3000/markanalyse
2. Look at the legend on the left side
3. Look at the fields on the map

**What to check:**
- [ ] Legend shows colors from white → red gradient
- [ ] Field colors on map match the legend colors
- [ ] When you change from "Total Pesticide" to "PFAS", colors update smoothly

**Before (Bug):** Legend showed 5 different colors, map used 10 colors → mismatch
**After (Fixed):** Both use same 10-color scheme → perfect match

---

### Test 2: Zoom Simplification ✅
**What to do:**
1. Zoom OUT to see all of Denmark
2. Zoom IN to see individual fields

**What to check at ZOOM 6 (country view):**
- [ ] Fields are very faded (30% opacity)
- [ ] No field outlines visible
- [ ] Map is not cluttered

**What to check at ZOOM 14 (field detail):**
- [ ] Fields are bright and clear (70% opacity)
- [ ] Field outlines are visible
- [ ] You can see individual field boundaries

**Before (Bug):** All detail at all zoom levels → cluttered at low zoom
**After (Fixed):** Smart fading → clean at low zoom, detailed at high zoom

---

### Test 3: Performance ✅
**What to do:**
1. Rapidly click between visualization modes:
   - Total Pesticide → PFAS → Glyphosate → Applications
2. Change year selection: 2023 → 2022 → 2021
3. Toggle "Decile farvning" on/off multiple times

**What to check:**
- [ ] Mode changes are instant (< 100ms)
- [ ] No flickering or white flashes
- [ ] Year changes load quickly
- [ ] Toggling decile mode is smooth

**Before (Bug):** Flicker, slow transitions, duplicate updates
**After (Fixed):** Smooth, instant, no flicker

---

### Test 4: Legend Accuracy ✅
**What to do:**
1. Enable "Decile farvning" toggle
2. Look at the legend

**What to check:**
- [ ] Legend shows "Decil 1", "Decil 3", "Decil 5", "Decil 7", "Decil 10"
- [ ] Each decile shows actual breakpoint values (e.g., "0-0.8", "0.8-1.8")
- [ ] Colors progress from light to dark

**Before (Bug):** Legend showed generic ranges, didn't update for decile mode
**After (Fixed):** Legend shows actual decile breakpoints with correct colors

---

## 🔍 Detailed Visual Comparison

### Color Legend - Before vs After

**BEFORE:**
```
Legend: 5 hardcoded colors
Map:    10 COLOR_SCHEMES colors
Result: ❌ Mismatch
```

**AFTER:**
```
Legend: 10 COLOR_SCHEMES colors
Map:    10 COLOR_SCHEMES colors
Result: ✅ Perfect match
```

---

### Zoom Behavior - Before vs After

**BEFORE (Zoom 6 - Country view):**
```
Opacity:  70% (always)
Outlines: 0.5px (always)
Result:   ❌ Cluttered, hard to read
```

**AFTER (Zoom 6 - Country view):**
```
Opacity:  30% (faded)
Outlines: 0px (hidden)
Result:   ✅ Clean, easy to read
```

**BEFORE (Zoom 14 - Field detail):**
```
Opacity:  70%
Outlines: 0.5px
Result:   ✅ Good detail
```

**AFTER (Zoom 14 - Field detail):**
```
Opacity:  70%
Outlines: 0.5px
Result:   ✅ Same good detail
```

---

## 📸 Screenshot Checklist

Take screenshots to verify:

1. **Legend at different modes:**
   - [ ] Total Pesticide mode (should show white → red)
   - [ ] PFAS mode (should show white → red)
   - [ ] Applications mode (should show numbers)
   - [ ] Organic status (should show green/gray)

2. **Zoom levels:**
   - [ ] Zoom 6 (should be faded, no outlines)
   - [ ] Zoom 10 (should be medium, thin outlines)
   - [ ] Zoom 14 (should be bright, full outlines)

3. **Decile mode:**
   - [ ] Decile OFF (should show generic ranges)
   - [ ] Decile ON (should show actual breakpoints)

---

## ⚡ Performance Indicators

**Good Performance:**
- Mode changes: < 100ms
- Year changes: < 2 seconds
- No white flashes
- Smooth animations

**Bad Performance:**
- Mode changes: > 500ms
- Flickering
- Multiple redraws visible
- Stuttering animations

---

## 🎨 Color Palette Reference

The 10-color scheme (UNIFIED_PESTICIDE_COLORS):
```
1.  #ffffff (white)
2.  #fef2f2 (very light red)
3.  #fecaca (light red)
4.  #fca5a5 (light-medium red)
5.  #f87171 (medium red)
6.  #ef4444 (red)
7.  #dc2626 (dark red)
8.  #b91c1c (darker red)
9.  #991b1b (very dark red)
10. #7f1d1d (darkest red)
```

These should appear in BOTH the legend and on the map fields.

---

## ✅ Final Checklist

After all visual tests:

- [ ] Colors match between legend and map ✅
- [ ] Zoom 6 shows faded fields ✅
- [ ] Zoom 14 shows detailed fields ✅
- [ ] Mode changes are smooth ✅
- [ ] Year changes are fast ✅
- [ ] No flickering during transitions ✅
- [ ] Decile mode shows correct breakpoints ✅
- [ ] All 5 visualization modes work ✅

If all checked: **🎉 ALL FIXES VERIFIED!**
