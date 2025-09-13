"""
NLES5 Fertilizer Distribution Module

This module implements the official Danish fertilizer distribution algorithm as specified
in the NLES5 methodology (N2023_62, Tabel 7). The algorithm distributes organic fertilizer
across fields within a farm (CVR) based on crop priorities and N-quota requirements.

DISTRIBUTION ALGORITHM:
The algorithm follows the prioritized approach for organic fertilizer allocation:

1. Crop Priority Order (from Tabel 7):
   - Priority 1: Helsæd, majs foderroer (Whole seed, maize, fodder beets)
   - Priority 2: Græs i omdrift (Grass in rotation)
   - Priority 3: Vinterraps (Winter rape)
   - Priority 4: Vedvarende græs (Permanent grass)
   - Priority 5: Vinterhvede (Winter wheat)
   - Priority 6: Vårbyg og andet vårkorn (Spring barley and other spring grains)
   - Priority 7: Vinterbyg og andet vinterkorn (Winter barley and other winter grains)

2. Distribution Logic:
   - If organic fertilizer > 50% of total N-quota → distribute proportionally by N-quota
   - If organic fertilizer ≤ 50% of total N-quota → distribute up to 50% per crop by priority

3. Mineral fertilizer distribution follows the same proportional allocation after organic.

SOURCE: 
- N2023_62.md, Section 4.5 Gødningsfordeling, markniveau
- Tabel 7: Prioritering for fordeling af organisk gødning til forskellige afgrødegrupper
"""

import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List

import duckdb


class CropPriority(IntEnum):
    """Crop priority levels for organic fertilizer distribution (Tabel 7)."""
    HELSAED_MAJS_FODERROER = 1    # Helsæd, majs foderroer
    GRAES_I_OMDRIFT = 2           # Græs i omdrift  
    VINTERRAPS = 3                # Vinterraps
    VEDVARENDE_GRAES = 4          # Vedvarende græs
    VINTERHVEDE = 5               # Vinterhvede
    VAARBYG_ANDET_VAARKORN = 6    # Vårbyg og andet vårkorn
    VINTERBYG_ANDET_VINTERKORN = 7 # Vinterbyg og andet vinterkorn


@dataclass
class FieldFertilizerAllocation:
    """Individual field fertilizer allocation result."""
    field_id: str
    cvr_number: str
    year: int
    crop_name: str
    crop_priority: int
    area_ha: float
    n_quota_kg_ha: float
    total_n_quota_kg: float
    
    # Allocated amounts
    organic_n_allocated_kg: float
    mineral_n_allocated_kg: float
    total_n_allocated_kg: float
    
    # Allocation details
    organic_n_rate_kg_ha: float
    mineral_n_rate_kg_ha: float
    organic_quota_fraction: float  # How much of N-quota comes from organic
    allocation_method: str  # 'proportional' or 'priority_based'


@dataclass 
class FarmFertilizerBudget:
    """Farm-level fertilizer budget for allocation."""
    cvr_number: str
    year: int
    total_organic_n_kg: float
    total_mineral_n_kg: float
    total_n_kg: float


class NLES5FertilizerDistributor:
    """
    Implements the official NLES5 fertilizer distribution algorithm.
    
    This class handles the distribution of organic and mineral fertilizer
    across fields within a farm according to the Danish methodology.
    """
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, log: logging.Logger):
        self.conn = conn
        self.log = log
        
        # Crop classification mapping to priorities (based on NLES5 crop codes and Danish names)
        self.crop_priority_mapping = {
            # Priority 1: Helsæd, majs foderroer  
            'M8': CropPriority.HELSAED_MAJS_FODERROER,  # Maize
            'silage maize': CropPriority.HELSAED_MAJS_FODERROER,
            'majs': CropPriority.HELSAED_MAJS_FODERROER,
            'foderroer': CropPriority.HELSAED_MAJS_FODERROER,
            'sukkerroer': CropPriority.HELSAED_MAJS_FODERROER,
            'helsæd': CropPriority.HELSAED_MAJS_FODERROER,
            
            # Priority 2: Græs i omdrift
            'M4': CropPriority.GRAES_I_OMDRIFT,  # Grass or grass-clover
            'græs': CropPriority.GRAES_I_OMDRIFT,
            'grass': CropPriority.GRAES_I_OMDRIFT,
            'clover': CropPriority.GRAES_I_OMDRIFT,
            'kløver': CropPriority.GRAES_I_OMDRIFT,
            
            # Priority 3: Vinterraps
            'M9': CropPriority.VINTERRAPS,  # Winter oilseed rape
            'vinterraps': CropPriority.VINTERRAPS,
            'raps': CropPriority.VINTERRAPS,
            'winter rape': CropPriority.VINTERRAPS,
            
            # Priority 4: Vedvarende græs (permanent grass)
            'M5': CropPriority.VEDVARENDE_GRAES,  # Grass for seed
            'vedvarende græs': CropPriority.VEDVARENDE_GRAES,
            'permanent grass': CropPriority.VEDVARENDE_GRAES,
            
            # Priority 5: Vinterhvede
            'M1': CropPriority.VINTERHVEDE,  # Winter cereal (primarily wheat)
            'vinterhvede': CropPriority.VINTERHVEDE,
            'winter wheat': CropPriority.VINTERHVEDE,
            'hvede': CropPriority.VINTERHVEDE,
            'wheat': CropPriority.VINTERHVEDE,
            
            # Priority 6: Vårbyg og andet vårkorn
            'M2': CropPriority.VAARBYG_ANDET_VAARKORN,  # Spring cereal
            'vårbyg': CropPriority.VAARBYG_ANDET_VAARKORN,
            'spring barley': CropPriority.VAARBYG_ANDET_VAARKORN,
            'byg': CropPriority.VAARBYG_ANDET_VAARKORN,
            'barley': CropPriority.VAARBYG_ANDET_VAARKORN,
            'vårkorn': CropPriority.VAARBYG_ANDET_VAARKORN,
            'spring cereal': CropPriority.VAARBYG_ANDET_VAARKORN,
            
            # Priority 7: Vinterbyg og andet vinterkorn  
            'vinterbyg': CropPriority.VINTERBYG_ANDET_VINTERKORN,
            'winter barley': CropPriority.VINTERBYG_ANDET_VINTERKORN,
            'vinterkorn': CropPriority.VINTERBYG_ANDET_VINTERKORN,
            'winter cereal': CropPriority.VINTERBYG_ANDET_VINTERKORN,
        }
        
        # Default N quotas by crop (kg N/ha) - approximate values from Danish standards
        self.default_n_quotas = {
            CropPriority.HELSAED_MAJS_FODERROER: 200,    # High N demand crops
            CropPriority.GRAES_I_OMDRIFT: 150,           # Grass in rotation
            CropPriority.VINTERRAPS: 180,                # Winter rape
            CropPriority.VEDVARENDE_GRAES: 100,          # Permanent grass
            CropPriority.VINTERHVEDE: 180,               # Winter wheat
            CropPriority.VAARBYG_ANDET_VAARKORN: 140,    # Spring cereals
            CropPriority.VINTERBYG_ANDET_VINTERKORN: 160  # Winter cereals
        }

    def get_crop_priority(self, crop_name: str, m_code: str = None) -> int:
        """
        Determine crop priority based on crop name and M-code.
        
        Args:
            crop_name: Crop name from agricultural data
            m_code: NLES5 M-code classification
            
        Returns:
            Priority level (1-7, lower number = higher priority)
        """
        # First try M-code if available
        if m_code and m_code in self.crop_priority_mapping:
            return self.crop_priority_mapping[m_code]
        
        # Then try crop name matching (case insensitive)
        if crop_name:
            crop_lower = crop_name.lower()
            for crop_key, priority in self.crop_priority_mapping.items():
                if crop_key.lower() in crop_lower:
                    return priority
        
        # Default to lowest priority if no match
        return CropPriority.VINTERBYG_ANDET_VINTERKORN

    def get_n_quota(self, crop_priority: int, area_ha: float) -> float:
        """
        Get N quota for a field based on crop priority and area.
        
        Args:
            crop_priority: Crop priority level (1-7)
            area_ha: Field area in hectares
            
        Returns:
            Total N quota in kg for the field
        """
        quota_per_ha = self.default_n_quotas.get(crop_priority, 140)  # Default 140 kg N/ha
        return quota_per_ha * area_ha

    def distribute_fertilizer_for_farm(
        self, 
        farm_budget: FarmFertilizerBudget,
        field_data: List[Dict]
    ) -> List[FieldFertilizerAllocation]:
        """
        Distribute fertilizer across fields for a single farm according to NLES5 algorithm.
        
        Args:
            farm_budget: Farm-level fertilizer budget
            field_data: List of field information dictionaries
            
        Returns:
            List of field allocations with distributed fertilizer amounts
        """
        self.log.debug(f"Distributing fertilizer for CVR {farm_budget.cvr_number}, "
                      f"year {farm_budget.year}: {farm_budget.total_organic_n_kg:.1f} kg organic N, "
                      f"{farm_budget.total_mineral_n_kg:.1f} kg mineral N across {len(field_data)} fields")
        
        # Step 1: Prepare field data with priorities and quotas
        fields_with_priority = []
        total_n_quota = 0
        
        for field in field_data:
            priority = self.get_crop_priority(
                field.get('crop_name', ''), 
                field.get('m_code', '')
            )
            
            area_ha = field.get('area_ha', 0)
            n_quota_kg = self.get_n_quota(priority, area_ha)
            total_n_quota += n_quota_kg
            
            fields_with_priority.append({
                **field,
                'crop_priority': priority,
                'n_quota_kg_ha': n_quota_kg / area_ha if area_ha > 0 else 0,
                'n_quota_kg': n_quota_kg
            })
        
        # Sort fields by priority (lower number = higher priority)
        fields_with_priority.sort(key=lambda x: x['crop_priority'])
        
        # Step 2: Determine distribution method
        organic_quota_ratio = farm_budget.total_organic_n_kg / total_n_quota if total_n_quota > 0 else 0
        
        if organic_quota_ratio > 0.5:
            # Proportional distribution (organic > 50% of quota)
            allocations = self._distribute_proportional(farm_budget, fields_with_priority, total_n_quota)
            method = 'proportional'
        else:
            # Priority-based distribution (organic ≤ 50% of quota)
            allocations = self._distribute_priority_based(farm_budget, fields_with_priority)
            method = 'priority_based'
        
        # Set allocation method for all fields
        for allocation in allocations:
            allocation.allocation_method = method
            
        self.log.debug(f"Completed fertilizer distribution for CVR {farm_budget.cvr_number} "
                      f"using {method} method (organic ratio: {organic_quota_ratio:.2%})")
        
        return allocations

    def _distribute_proportional(
        self, 
        farm_budget: FarmFertilizerBudget,
        fields: List[Dict],
        total_n_quota: float
    ) -> List[FieldFertilizerAllocation]:
        """Distribute fertilizer proportionally based on N-quota when organic > 50%."""
        allocations = []
        
        for field in fields:
            field_quota_fraction = field['n_quota_kg'] / total_n_quota if total_n_quota > 0 else 0
            
            # Distribute organic N proportionally
            organic_n_allocated = farm_budget.total_organic_n_kg * field_quota_fraction
            
            # Distribute mineral N proportionally  
            mineral_n_allocated = farm_budget.total_mineral_n_kg * field_quota_fraction
            
            allocation = FieldFertilizerAllocation(
                field_id=field['field_id'],
                cvr_number=field['cvr_number'],
                year=field['year'],
                crop_name=field.get('crop_name', 'Unknown'),
                crop_priority=field['crop_priority'],
                area_ha=field['area_ha'],
                n_quota_kg_ha=field['n_quota_kg_ha'],
                total_n_quota_kg=field['n_quota_kg'],
                organic_n_allocated_kg=organic_n_allocated,
                mineral_n_allocated_kg=mineral_n_allocated,
                total_n_allocated_kg=organic_n_allocated + mineral_n_allocated,
                organic_n_rate_kg_ha=organic_n_allocated / field['area_ha'] if field['area_ha'] > 0 else 0,
                mineral_n_rate_kg_ha=mineral_n_allocated / field['area_ha'] if field['area_ha'] > 0 else 0,
                organic_quota_fraction=organic_n_allocated / field['n_quota_kg'] if field['n_quota_kg'] > 0 else 0,
                allocation_method='proportional'
            )
            
            allocations.append(allocation)
        
        return allocations

    def _distribute_priority_based(
        self, 
        farm_budget: FarmFertilizerBudget,
        fields: List[Dict]
    ) -> List[FieldFertilizerAllocation]:
        """Distribute fertilizer by priority when organic ≤ 50%, up to 50% of each field's quota."""
        allocations = []
        remaining_organic_n = farm_budget.total_organic_n_kg
        remaining_mineral_n = farm_budget.total_mineral_n_kg
        
        # Initialize all fields with zero allocation
        for field in fields:
            field['organic_n_allocated'] = 0.0
        
        # Step 1: Distribute organic N by priority groups, handling same-priority fields equally
        # Group fields by priority
        priority_groups = {}
        for field in fields:
            priority = field['crop_priority']
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append(field)
        
        # Process each priority group in order
        for priority in sorted(priority_groups.keys()):
            group_fields = priority_groups[priority]
            
            if remaining_organic_n <= 0:
                break
                
            # Calculate total quota capacity for this priority group (up to 50% each)
            total_group_capacity = sum(field['n_quota_kg'] * 0.5 for field in group_fields)
            
            if total_group_capacity > 0:
                # Distribute available organic N proportionally within this priority group
                organic_for_group = min(remaining_organic_n, total_group_capacity)
                
                for field in group_fields:
                    field_capacity = field['n_quota_kg'] * 0.5
                    field_fraction = field_capacity / total_group_capacity if total_group_capacity > 0 else 0
                    field['organic_n_allocated'] = organic_for_group * field_fraction
                
                remaining_organic_n -= organic_for_group
        
        # Step 2: Distribute remaining organic N proportionally if any left
        if remaining_organic_n > 0:
            total_remaining_quota = sum(field['n_quota_kg'] - field['organic_n_allocated'] for field in fields)
            
            for field in fields:
                if total_remaining_quota > 0:
                    remaining_quota_fraction = (field['n_quota_kg'] - field['organic_n_allocated']) / total_remaining_quota
                    additional_organic = remaining_organic_n * remaining_quota_fraction
                    field['organic_n_allocated'] += additional_organic
        
        # Step 3: Distribute mineral N proportionally based on remaining quota needs
        total_remaining_quota = sum(field['n_quota_kg'] - field['organic_n_allocated'] for field in fields)
        
        for field in fields:
            if total_remaining_quota > 0:
                remaining_quota_fraction = (field['n_quota_kg'] - field['organic_n_allocated']) / total_remaining_quota
                mineral_allocated = remaining_mineral_n * remaining_quota_fraction
            else:
                mineral_allocated = 0
            
            field['mineral_n_allocated'] = mineral_allocated
        
        # Step 4: Create allocation objects
        for field in fields:
            allocation = FieldFertilizerAllocation(
                field_id=field['field_id'],
                cvr_number=field['cvr_number'],
                year=field['year'],
                crop_name=field.get('crop_name', 'Unknown'),
                crop_priority=field['crop_priority'],
                area_ha=field['area_ha'],
                n_quota_kg_ha=field['n_quota_kg_ha'],
                total_n_quota_kg=field['n_quota_kg'],
                organic_n_allocated_kg=field['organic_n_allocated'],
                mineral_n_allocated_kg=field['mineral_n_allocated'],
                total_n_allocated_kg=field['organic_n_allocated'] + field['mineral_n_allocated'],
                organic_n_rate_kg_ha=field['organic_n_allocated'] / field['area_ha'] if field['area_ha'] > 0 else 0,
                mineral_n_rate_kg_ha=field['mineral_n_allocated'] / field['area_ha'] if field['area_ha'] > 0 else 0,
                organic_quota_fraction=field['organic_n_allocated'] / field['n_quota_kg'] if field['n_quota_kg'] > 0 else 0,
                allocation_method='priority_based'
            )
            
            allocations.append(allocation)
        
        return allocations

    def apply_fertilizer_distribution_to_pipeline(self, fields_table: str) -> str:
        """
        Apply the NLES5 fertilizer distribution algorithm to the pipeline data.
        
        This replaces the simple CVR-based join with the sophisticated distribution algorithm.
        
        Args:
            fields_table: Name of the table containing field data
            
        Returns:
            Name of the table with distributed fertilizer data
        """
        self.log.info("🚜 Applying NLES5 fertilizer distribution algorithm...")
        
        # Step 1: Create farm-level fertilizer budgets from fertilizer_accounts
        self.log.info("Creating farm-level fertilizer budgets...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE farm_fertilizer_budgets AS
            SELECT 
                cvr_number,
                year,
                SUM(organic_n_hus * area_ha) as total_organic_n_kg,
                SUM((mineral_n_foraar + mineral_n_eft + mineral_n_udb) * area_ha) as total_mineral_n_kg,
                SUM(tn_t_ha * area_ha * 1000) as total_n_kg  -- Convert tonnes to kg
            FROM fertilizer_accounts fa
            WHERE cvr_number IS NOT NULL
            GROUP BY cvr_number, year
        """)
        
        # Step 2: Get field data grouped by farm
        farm_fields = self.conn.execute(f"""
            SELECT 
                cvr_number,
                year,
                field_id,
                crop_name,
                m_code,
                area_ha
            FROM {fields_table}
            WHERE cvr_number IS NOT NULL
            ORDER BY cvr_number, year, field_id
        """).fetchall()
        
        # Step 3: Group fields by farm and apply distribution
        farms = {}
        for row in farm_fields:
            cvr_year_key = (row[0], row[1])  # (cvr_number, year)
            if cvr_year_key not in farms:
                farms[cvr_year_key] = []
            farms[cvr_year_key].append({
                'field_id': row[2],
                'cvr_number': row[0],
                'year': row[1],
                'crop_name': row[3],
                'm_code': row[4],
                'area_ha': row[5]
            })
        
        # Step 4: Process each farm and collect results
        all_allocations = []
        
        for (cvr_number, year), field_list in farms.items():
            # Get farm budget
            budget_row = self.conn.execute("""
                SELECT total_organic_n_kg, total_mineral_n_kg, total_n_kg
                FROM farm_fertilizer_budgets
                WHERE cvr_number = ? AND year = ?
            """, [cvr_number, year]).fetchone()
            
            if budget_row:
                farm_budget = FarmFertilizerBudget(
                    cvr_number=cvr_number,
                    year=year,
                    total_organic_n_kg=budget_row[0] or 0,
                    total_mineral_n_kg=budget_row[1] or 0,
                    total_n_kg=budget_row[2] or 0
                )
                
                # Apply distribution algorithm
                allocations = self.distribute_fertilizer_for_farm(farm_budget, field_list)
                all_allocations.extend(allocations)
        
        # Step 5: Create distributed fertilizer table
        self.log.info(f"Creating distributed fertilizer table with {len(all_allocations)} field allocations...")
        
        # Insert allocations into new table
        self.conn.execute("DROP TABLE IF EXISTS distributed_fertilizer_data")
        self.conn.execute("""
            CREATE TABLE distributed_fertilizer_data (
                field_id VARCHAR,
                cvr_number VARCHAR,
                year INTEGER,
                crop_name VARCHAR,
                crop_priority INTEGER,
                area_ha DECIMAL(10,4),
                n_quota_kg_ha DECIMAL(8,2),
                total_n_quota_kg DECIMAL(10,2),
                organic_n_allocated_kg DECIMAL(10,2),
                mineral_n_allocated_kg DECIMAL(10,2),
                total_n_allocated_kg DECIMAL(10,2),
                organic_n_rate_kg_ha DECIMAL(8,2),
                mineral_n_rate_kg_ha DECIMAL(8,2),
                organic_quota_fraction DECIMAL(5,3),
                allocation_method VARCHAR
            )
        """)
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(all_allocations), batch_size):
            batch = all_allocations[i:i + batch_size]
            values = []
            for alloc in batch:
                values.append((
                    alloc.field_id, alloc.cvr_number, alloc.year, alloc.crop_name,
                    alloc.crop_priority, alloc.area_ha, alloc.n_quota_kg_ha, alloc.total_n_quota_kg,
                    alloc.organic_n_allocated_kg, alloc.mineral_n_allocated_kg, alloc.total_n_allocated_kg,
                    alloc.organic_n_rate_kg_ha, alloc.mineral_n_rate_kg_ha, alloc.organic_quota_fraction,
                    alloc.allocation_method
                ))
            
            self.conn.executemany("""
                INSERT INTO distributed_fertilizer_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, values)
        
        # Step 6: Join distributed data back to fields
        result_table = f"{fields_table}_with_distributed_fertilizer"
        self.conn.execute(f"DROP TABLE IF EXISTS {result_table}")
        self.conn.execute(f"""
            CREATE TABLE {result_table} AS
            SELECT 
                f.*,
                COALESCE(d.organic_n_rate_kg_ha, 0.0) as organic_n_hus,
                COALESCE(d.mineral_n_rate_kg_ha * 0.4, 0.0) as mineral_n_foraar,  -- Assume 40% spring
                COALESCE(d.mineral_n_rate_kg_ha * 0.1, 0.0) as mineral_n_eft,     -- Assume 10% autumn  
                COALESCE(d.mineral_n_rate_kg_ha * 0.5, 0.0) as mineral_n_udb,     -- Assume 50% growing season
                COALESCE(d.total_n_allocated_kg / NULLIF(f.area_ha, 0) / 1000, 0.0) as tn_t_ha,  -- Convert to tonnes/ha
                COALESCE(d.allocation_method, 'no_fertilizer_data') as fertilizer_allocation_method,
                COALESCE(d.organic_quota_fraction, 0.0) as organic_quota_fraction,
                COALESCE(d.crop_priority, 7) as fertilizer_crop_priority
            FROM {fields_table} f
            LEFT JOIN distributed_fertilizer_data d ON f.field_id = d.field_id AND f.year = d.year
        """)
        
        # Clean up intermediate tables
        self.conn.execute("DROP TABLE IF EXISTS farm_fertilizer_budgets")
        self.conn.execute("DROP TABLE IF EXISTS distributed_fertilizer_data")
        
        distributed_count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
        with_fertilizer = self.conn.execute(f"""
            SELECT COUNT(*) FROM {result_table} 
            WHERE fertilizer_allocation_method != 'no_fertilizer_data'
        """).fetchone()[0]
        
        self.log.info(f"✅ NLES5 fertilizer distribution completed: {distributed_count:,} fields processed, "
                     f"{with_fertilizer:,} fields received fertilizer allocation")
        
        return result_table
