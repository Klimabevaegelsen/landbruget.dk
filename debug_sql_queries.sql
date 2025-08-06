
-- DEBUG QUERIES FOR field_id = '9-0'

-- 1. Check Stage 2 fragments (if available)
SELECT field_uuid, toerv_pct, COUNT(*) as fragment_count,
       SUM(field_wetland_intersection_area_m2) as total_wetland_area
FROM field_wetland_intersections 
WHERE field_id = '9-0'
GROUP BY field_uuid, toerv_pct;

-- 2. Look for duplicate wetland intersections
SELECT field_uuid, toerv_pct, field_wetland_intersection_area_m2, COUNT(*) as duplicate_count
FROM field_wetland_intersections 
WHERE field_id = '9-0'
GROUP BY field_uuid, toerv_pct, field_wetland_intersection_area_m2
HAVING COUNT(*) > 1;

-- 3. Check water project coverage calculation
SELECT field_uuid, 
       SUM(field_wetland_intersection_area_m2) as total_wetland,
       -- Add field-level coverage logic here
FROM field_wetland_intersections 
WHERE field_id = '9-0'
GROUP BY field_uuid;
