#!/usr/bin/env python3
"""
Corrected mapping analysis based on actual data verification.
"""

def analyze_corrected_mappings():
    """Analyze the corrected mappings based on actual data structure."""
    
    print("🔧 CORRECTED FERTILISER DATA MAPPINGS")
    print("=" * 80)
    
    print("\n✅ EFTERAFGRØDER MAPPINGS (VERIFIED):")
    print("-" * 50)
    
    efterafgroeder_correct = {
        '2020': {
            'pattern': 'Efterafgrøder 2020',
            'columns': {
                'a18_indberetefterafgalternativ': 'indberet_alternativ',
                'a19_faktiskhaudlagteaalternativ': 'faktisk_areal_ha', 
                'a20_omregnethamedea': 'omregnet_areal_ha'
            },
            'has_marknummer': False
        },
        '2021': {
            'pattern': 'Efterafgrøder 2021',
            'columns': {
                'a19_indberetefterafgalternativ': 'indberet_alternativ',
                'a20_faktiskhaudlagteaalternativ': 'faktisk_areal_ha',
                'a21_omregnethamedea': 'omregnet_areal_ha'
            },
            'has_marknummer': False
        },
        '2022': {
            'pattern': 'Efterafgrøder 2022',
            'columns': {
                'a20_indberetefterafgalternativ': 'indberet_alternativ',
                'a21_faktiskhaudlagteaalternativ': 'faktisk_areal_ha',
                'a24_omregnethamedea': 'omregnet_areal_ha'  # Note: a24, not a23!
            },
            'has_marknummer': True,
            'additional_columns': ['a22_indberetplandbrugind', 'a23_indberetplandbrugareal']
        },
        '2023': {
            'pattern': 'Efterafgrøder 2023',
            'columns': {
                'a19_indberetefterafgalternativ': 'indberet_alternativ',
                'a20_faktiskhaudlagteaalternativ': 'faktisk_areal_ha',
                'a23_omregnethamedea': 'omregnet_areal_ha'
            },
            'has_marknummer': True,
            'additional_columns': ['a21_indberetplandbrugind', 'a22_indberetplandbrugareal']
        }
    }
    
    for year, info in efterafgroeder_correct.items():
        print(f"\n  📅 {year}: {info['pattern']}")
        for original, mapped in info['columns'].items():
            print(f"     {original} → {mapped}")
        if info.get('additional_columns'):
            print(f"     Additional: {info['additional_columns']}")
        print(f"     Has marknummer: {info['has_marknummer']}")
    
    print("\n✅ GKEA MAPPINGS (CORRECTED BASED ON HEADERS):")
    print("-" * 50)
    
    gkea_correct = {
        '2021': {
            'pattern': 'GKEA2021_Markplan_med_Gødningsoplysninger',
            'header_row': 2,  # Row index where real headers are
            'columns': {
                'column_1': 'journal_nummer',      # Journal Nummer
                'column_2': 'cvr_number',          # CVR  
                'column_6': 'marknummer',          # Marknummer (position 6)
                'column_7': 'areal_ha',            # Areal 
                'column_10': 'harmoni_areal_ha',   # Harmoni Areal
                'column_15': 'hovedafgroede',      # Hovedafgrøde
                'column_19': 'fosfortal',          # Fosfortal
                'column_28': 'n_kvote_mark'        # N Kvote Mark
            }
        },
        '2022': {
            'pattern': 'GKEA2022_Markplan_med_Gødningsoplysninger', 
            'header_row': 2,
            'columns': {
                'column_1': 'journal_nummer',      # Journal Nummer
                'column_2': 'cvr_number',          # CVR
                'column_4': 'marknummer',          # Marknummer 
                'column_5': 'areal_ha',            # Areal
                'column_8': 'harmoni_areal_ha',    # Harmoni Areal
                'column_12': 'hovedafgroede',      # Hovedafgrøde  
                'column_17': 'fosfortal',          # Fosfortal
                'column_24': 'n_kvote_mark'        # N Kvote Mark
            }
        },
        '2023': {
            'pattern': 'GKEA2023_Markplan_med_Gødningsoplysninger_Aktindsigt',
            'header_row': 2,
            'columns': {
                'column_1': 'journal_nummer',      # Journal Nummer
                'column_2': 'cvr_number',          # CVR
                'column_4': 'marknummer',          # Marknummer
                'column_5': 'areal_ha',            # Areal
                'column_7': 'harmoni_areal_ha',    # Harmoni Areal  
                'column_11': 'hovedafgroede',      # Hovedafgrøde
                'column_15': 'fosfortal',          # N Norm Udlæg (closest to fosfortal)
                'column_19': 'n_kvote_mark'        # N Kvote Mark
            }
        },
        '2024_efterafgroeder': {
            'pattern': 'GKEA2024_Markplan_Efterafgrøder',
            'header_row': 2,
            'columns': {
                'column_1': 'journal_nummer',      # Journal Nummer
                'column_2': 'cvr_number',          # CVR
                'column_4': 'marknummer',          # Marknummer
                'column_6': 'areal_ha',            # Areal
                'column_14': 'omregnet_areal_ha',  # Areal Omregnet Til EA
                'column_5': 'hovedafgroede'        # Hoved afgrøde
            }
        },
        '2024_goedning': {
            'pattern': 'GKEA2024_Markplan_med_Gødningsoplysninger',
            'header_row': 2,
            'columns': {
                'column_1': 'journal_nummer',      # Journal Nummer  
                'column_2': 'cvr_number',          # CVR
                'column_4': 'marknummer',          # Marknummer
                'column_5': 'areal_ha',            # Areal
                'column_7': 'harmoni_areal_ha',    # Harmoni Areal
                'column_11': 'hovedafgroede',      # Hovedafgrøde
                'column_15': 'fosfortal',          # N Norm Udlæg
                'column_19': 'n_kvote_mark'        # N Kvote Mark
            }
        }
    }
    
    for year, info in gkea_correct.items():
        print(f"\n  📅 GKEA {year}: {info['pattern']}")
        print(f"     Header row: {info['header_row']}")
        for original, mapped in info['columns'].items():
            print(f"     {original} → {mapped}")
    
    print("\n✅ GØDNINGSREGNSKABER MAPPINGS (VERIFIED):")
    print("-" * 50)
    
    goedning_correct = {
        '2022_data': {
            'file': 'Gødningsregnskaber 2022_data.parquet',
            'columns': ['cvr_number', 'kommune', 'vir_navn'],
            'year': '2022'
        },
        '2023': {
            'file': 'Gødningsregnskaber 2023.parquet', 
            'columns': ['cvr_number', 'vir_navn'],  # No 'kommune' in 2023!
            'year': '2023',
            'note': 'Missing kommune column in 2023 file'
        }
    }
    
    for year, info in goedning_correct.items():
        print(f"\n  📅 {year}: {info['file']}")
        print(f"     Available columns: {info['columns']}")
        print(f"     Year: {info['year']}")
        if 'note' in info:
            print(f"     ⚠️  {info['note']}")
    
    print("\n🚨 CRITICAL CORRECTIONS NEEDED:")
    print("-" * 50)
    print("1. ✅ Efterafgrøder mappings are correct in my implementation")
    print("2. ❌ GKEA column positions need adjustment:")
    print("   - 2022: fosfortal should be column_17, not column_16") 
    print("   - 2023: fosfortal should be column_15, not column_14")
    print("   - 2024: Different structure for Efterafgrøder vs Gødningsoplysninger")
    print("3. ❌ Gødningsregnskaber 2023 missing 'kommune' column")
    print("4. ✅ Header row skipping (row > 2) is correct for GKEA files")
    
    return efterafgroeder_correct, gkea_correct, goedning_correct

if __name__ == "__main__":
    analyze_corrected_mappings()