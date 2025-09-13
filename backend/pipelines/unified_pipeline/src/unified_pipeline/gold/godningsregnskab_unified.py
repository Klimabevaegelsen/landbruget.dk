"""Gold pipeline for combining all godningsregnskab tables into a unified dataset."""

import os
import tempfile
import zipfile
from typing import Dict, List, Tuple

from ..base.gold_base import GoldBase


class GodningsregnskabUnified(GoldBase):
    """Gold pipeline for unified godningsregnskab analysis."""

    def __init__(self, db_path: str = ":memory:"):
        super().__init__("godningsregnskab_unified", db_path)
        self.zip_path = "data/godningsregnskab.zip"
        self.extracted_tables: Dict[str, str] = {}

    def create_analytics_table(self, silver_tables: List[str] = None, **kwargs) -> str:
        """
        Create unified analytics table from all godningsregnskab data.

        Args:
            silver_tables: Not used for this pipeline (we work directly with raw data)
            **kwargs: Additional processing parameters

        Returns:
            str: Name of created gold table
        """
        print("🚀 Starting godningsregnskab unified pipeline...")
        
        # Extract and process all data files
        self._extract_and_process_files()
        
        # Create unified table
        unified_table = self._create_unified_table()
        
        # Add metadata and enrichments
        self._add_metadata_columns(unified_table)
        
        # Create summary statistics
        self._create_summary_tables(unified_table)
        
        print(f"✅ Created unified table: {unified_table}")
        return unified_table

    def _extract_and_process_files(self):
        """Extract and process all files from the zip archive."""
        print("📦 Extracting and processing files from zip archive...")
        
        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            # Process different file types
            csv_files = [f for f in file_list if f.endswith('.csv')]
            xls_files = [f for f in file_list if f.endswith('.xls')]
            xlsx_files = [f for f in file_list if f.endswith('.xlsx')]
            
            print(f"Found {len(csv_files)} CSV files, {len(xls_files)} XLS files, {len(xlsx_files)} XLSX files")
            
            # Process CSV files first (most common)
            self._process_csv_files(zip_ref, csv_files)
            
            # Process Excel files
            self._process_excel_files(zip_ref, xls_files + xlsx_files)

    def _process_csv_files(self, zip_ref: zipfile.ZipFile, csv_files: List[str]):
        """Process CSV files from the zip archive."""
        print(f"📊 Processing {len(csv_files)} CSV files...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            for csv_file in csv_files:
                try:
                    # Extract file
                    zip_ref.extract(csv_file, temp_dir)
                    file_path = os.path.join(temp_dir, csv_file)
                    
                    # Determine table type and year
                    table_type, year = self._parse_filename(csv_file)
                    
                    # Create table name
                    table_name = f"{table_type}_{year}_{len(self.extracted_tables)}"
                    
                    # Load into DuckDB
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT * FROM read_csv('{file_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
                    """)
                    
                    # Store metadata
                    self.extracted_tables[table_name] = {
                        'file_path': csv_file,
                        'table_type': table_type,
                        'year': year,
                        'format': 'csv'
                    }
                    
                    print(f"  ✅ Loaded {table_name} from {csv_file}")
                    
                except Exception as e:
                    print(f"  ❌ Error processing {csv_file}: {e}")

    def _process_excel_files(self, zip_ref: zipfile.ZipFile, excel_files: List[str]):
        """Process Excel files from the zip archive."""
        print(f"📈 Processing {len(excel_files)} Excel files...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            for excel_file in excel_files:
                try:
                    # Extract file
                    zip_ref.extract(excel_file, temp_dir)
                    file_path = os.path.join(temp_dir, excel_file)
                    
                    # Determine table type and year
                    table_type, year = self._parse_filename(excel_file)
                    
                    # Create table name
                    table_name = f"{table_type}_{year}_{len(self.extracted_tables)}"
                    
                    # Load into DuckDB (DuckDB can read Excel files)
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT * FROM read_excel('{file_path}')
                    """)
                    
                    # Store metadata
                    self.extracted_tables[table_name] = {
                        'file_path': excel_file,
                        'table_type': table_type,
                        'year': year,
                        'format': 'excel'
                    }
                    
                    print(f"  ✅ Loaded {table_name} from {excel_file}")
                    
                except Exception as e:
                    print(f"  ❌ Error processing {excel_file}: {e}")

    def _parse_filename(self, filename: str) -> Tuple[str, str]:
        """Parse filename to extract table type and year."""
        # Extract year from path
        year = "unknown"
        for part in filename.split('/'):
            if part.startswith('GR ') and len(part) >= 6:
                year = part.split(' ')[1]
                break
        
        # Extract table type from filename
        table_type = "unknown"
        filename_lower = filename.lower()
        
        if 'goedrk' in filename_lower:
            table_type = "fertilizer_accounting"
        elif 'modrk' in filename_lower:
            table_type = "manure_accounting"
        elif 'dyrerk' in filename_lower:
            table_type = "animal_accounting"
        elif 'aftrk' in filename_lower:
            table_type = "general_accounting"
        elif 'forarbr' in filename_lower:
            table_type = "forest_agricultural_work"
        elif 'ovdrk' in filename_lower:
            table_type = "overhead_accounting"
        elif 'aoggoed' in filename_lower:
            table_type = "other_organic_fertilizer"
        elif 'biomasr' in filename_lower:
            table_type = "biomass_accounting"
        elif 'blandrk' in filename_lower:
            table_type = "plant_accounting"
        elif 'modhumr' in filename_lower:
            table_type = "human_manure"
        elif 'erklrk' in filename_lower:
            table_type = "agricultural_accounting"
        elif 'lg_company' in filename_lower:
            table_type = "company_data"
        elif 'feltdefinition' in filename_lower:
            table_type = "field_definition"
        elif 'modtaget' in filename_lower:
            table_type = "received_data"
        elif 'afsat' in filename_lower:
            table_type = "delivered_data"
        
        return table_type, year

    def _create_unified_table(self) -> str:
        """Create unified table combining all extracted tables."""
        print("🔗 Creating unified table from all extracted data...")
        
        unified_table = "godningsregnskab_unified"
        
        # Get all table names
        all_tables = list(self.extracted_tables.keys())
        
        if not all_tables:
            raise ValueError("No tables were successfully extracted")
        
        # Create unified table with common schema
        # We'll use UNION ALL to combine all tables, handling schema differences
        union_queries = []
        
        for table_name in all_tables:
            try:
                # Get table info to understand schema
                table_info = self.get_table_info(table_name)
                columns = [col[0] for col in table_info]
                
                # Create a standardized select with common columns
                select_cols = self._standardize_columns(columns, table_name)
                
                union_queries.append(f"""
                    SELECT 
                        '{table_name}' as source_table,
                        '{self.extracted_tables[table_name]["table_type"]}' as table_type,
                        '{self.extracted_tables[table_name]["year"]}' as data_year,
                        '{self.extracted_tables[table_name]["format"]}' as file_format,
                        {select_cols}
                    FROM {table_name}
                """)
                
            except Exception as e:
                print(f"  ⚠️  Skipping table {table_name}: {e}")
        
        if not union_queries:
            raise ValueError("No valid tables to combine")
        
        # Create the unified table
        union_query = " UNION ALL ".join(union_queries)
        
        self.conn.execute(f"""
            CREATE TABLE {unified_table} AS
            {union_query}
        """)
        
        # Get row count
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {unified_table}").fetchone()[0]
        print(f"  ✅ Unified table created with {row_count:,} rows from {len(union_queries)} tables")
        
        return unified_table

    def _standardize_columns(self, columns: List[str], table_name: str) -> str:
        """Standardize column names across different tables."""
        # Common column mappings
        column_mappings = {
            'cvr_nummer': 'cvr_number',
            'cvr': 'cvr_number',
            'cvrnr': 'cvr_number',
            'virksomhedsnavn': 'company_name',
            'navn': 'company_name',
            'virksomhed': 'company_name',
            'adresse': 'address',
            'postnummer': 'postal_code',
            'postnr': 'postal_code',
            'by': 'city',
            'kommune': 'municipality',
            'region': 'region',
            'feltnummer': 'field_number',
            'felt': 'field_number',
            'areal': 'area',
            'hektar': 'area_hectares',
            'afgroede': 'crop',
            'afgroede_kode': 'crop_code',
            'gødning': 'fertilizer',
            'gødningstype': 'fertilizer_type',
            'mængde': 'amount',
            'mængde_kg': 'amount_kg',
            'mængde_n': 'amount_n',
            'mængde_p': 'amount_p',
            'mængde_k': 'amount_k',
            'dato': 'date',
            'år': 'year',
            'måned': 'month',
            'uge': 'week'
        }
        
        # Create standardized select statement
        select_parts = []
        
        for col in columns:
            col_lower = col.lower().strip()
            
            # Map to standard column name
            if col_lower in column_mappings:
                standard_name = column_mappings[col_lower]
                select_parts.append(f"'{col}' as {standard_name}")
            else:
                # Keep original column name but clean it
                clean_name = col.replace(' ', '_').replace('-', '_').lower()
                select_parts.append(f"'{col}' as {clean_name}")
        
        return ",\n                        ".join(select_parts)

    def _add_metadata_columns(self, table_name: str):
        """Add metadata columns to the unified table."""
        print("📋 Adding metadata columns...")
        
        # Add processing metadata
        self.conn.execute(f"""
            ALTER TABLE {table_name} ADD COLUMN processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)
        
        self.conn.execute(f"""
            ALTER TABLE {table_name} ADD COLUMN pipeline_version VARCHAR DEFAULT '1.0'
        """)
        
        # Add data quality indicators
        self.conn.execute(f"""
            ALTER TABLE {table_name} ADD COLUMN has_cvr_number BOOLEAN DEFAULT FALSE
        """)
        
        self.conn.execute(f"""
            ALTER TABLE {table_name} ADD COLUMN has_field_data BOOLEAN DEFAULT FALSE
        """)
        
        # Update quality indicators
        self.conn.execute(f"""
            UPDATE {table_name} 
            SET has_cvr_number = CASE 
                WHEN cvr_number IS NOT NULL AND cvr_number != '' THEN TRUE 
                ELSE FALSE 
            END
        """)
        
        self.conn.execute(f"""
            UPDATE {table_name} 
            SET has_field_data = CASE 
                WHEN field_number IS NOT NULL AND field_number != '' THEN TRUE 
                ELSE FALSE 
            END
        """)

    def _create_summary_tables(self, unified_table: str):
        """Create summary and analysis tables."""
        print("📊 Creating summary tables...")
        
        # Company summary
        company_summary = f"{unified_table}_company_summary"
        self.conn.execute(f"""
            CREATE TABLE {company_summary} AS
            SELECT 
                cvr_number,
                company_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT data_year) as years_active,
                MIN(data_year) as first_year,
                MAX(data_year) as last_year,
                COUNT(DISTINCT table_type) as table_types_count
            FROM {unified_table}
            WHERE cvr_number IS NOT NULL AND cvr_number != ''
            GROUP BY cvr_number, company_name
            ORDER BY record_count DESC
        """)
        
        # Year summary
        year_summary = f"{unified_table}_year_summary"
        self.conn.execute(f"""
            CREATE TABLE {year_summary} AS
            SELECT 
                data_year,
                table_type,
                COUNT(*) as record_count,
                COUNT(DISTINCT cvr_number) as unique_companies,
                COUNT(DISTINCT field_number) as unique_fields
            FROM {unified_table}
            GROUP BY data_year, table_type
            ORDER BY data_year DESC, record_count DESC
        """)
        
        # Data quality summary
        quality_summary = f"{unified_table}_quality_summary"
        self.conn.execute(f"""
            CREATE TABLE {quality_summary} AS
            SELECT 
                data_year,
                table_type,
                COUNT(*) as total_records,
                SUM(CASE WHEN has_cvr_number THEN 1 ELSE 0 END) as records_with_cvr,
                SUM(CASE WHEN has_field_data THEN 1 ELSE 0 END) as records_with_field_data,
                ROUND(100.0 * SUM(CASE WHEN has_cvr_number THEN 1 ELSE 0 END) / COUNT(*), 2) as cvr_completeness_pct,
                ROUND(100.0 * SUM(CASE WHEN has_field_data THEN 1 ELSE 0 END) / COUNT(*), 2) as field_completeness_pct
            FROM {unified_table}
            GROUP BY data_year, table_type
            ORDER BY data_year DESC, total_records DESC
        """)
        
        print(f"  ✅ Created summary tables: {company_summary}, {year_summary}, {quality_summary}")

    def get_data_summary(self, table_name: str = "godningsregnskab_unified") -> Dict:
        """Get comprehensive data summary."""
        summary = {}
        
        # Basic counts
        summary['total_records'] = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        summary['unique_companies'] = self.conn.execute(f"SELECT COUNT(DISTINCT cvr_number) FROM {table_name} WHERE cvr_number IS NOT NULL").fetchone()[0]
        summary['unique_years'] = self.conn.execute(f"SELECT COUNT(DISTINCT data_year) FROM {table_name}").fetchone()[0]
        summary['table_types'] = self.conn.execute(f"SELECT COUNT(DISTINCT table_type) FROM {table_name}").fetchone()[0]
        
        # Year distribution
        year_dist = self.conn.execute(f"""
            SELECT data_year, COUNT(*) as count 
            FROM {table_name} 
            GROUP BY data_year 
            ORDER BY data_year DESC
        """).fetchall()
        summary['year_distribution'] = dict(year_dist)
        
        # Table type distribution
        table_dist = self.conn.execute(f"""
            SELECT table_type, COUNT(*) as count 
            FROM {table_name} 
            GROUP BY table_type 
            ORDER BY count DESC
        """).fetchall()
        summary['table_type_distribution'] = dict(table_dist)
        
        return summary

    def export_to_gcs(self, table_name: str, gcs_path: str):
        """Export unified table to GCS."""
        print(f"☁️  Exporting {table_name} to {gcs_path}...")
        
        self.save_table_to_gcs_parquet(table_name, gcs_path, compression="zstd")
        print(f"  ✅ Exported to {gcs_path}")


def main():
    """Main execution function."""
    # Initialize pipeline
    pipeline = GodningsregnskabUnified()
    
    try:
        # Create unified analytics table
        unified_table = pipeline.create_analytics_table()
        
        # Get summary
        summary = pipeline.get_data_summary(unified_table)
        
        print("\n📈 DATA SUMMARY:")
        print(f"Total records: {summary['total_records']:,}")
        print(f"Unique companies: {summary['unique_companies']:,}")
        print(f"Years covered: {summary['unique_years']}")
        print(f"Table types: {summary['table_types']}")
        
        print("\n📅 Year Distribution:")
        for year, count in summary['year_distribution'].items():
            print(f"  {year}: {count:,} records")
        
        print("\n📊 Table Type Distribution:")
        for table_type, count in summary['table_type_distribution'].items():
            print(f"  {table_type}: {count:,} records")
        
        # Export to GCS if credentials are available
        gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
        gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")
        
        if gcs_access_key and gcs_secret_key:
            pipeline.setup_gcs_secret(gcs_access_key, gcs_secret_key)
            gcs_path = "gs://landbrugsdata-raw-data/gold/godningsregnskab_unified.parquet"
            pipeline.export_to_gcs(unified_table, gcs_path)
        
        return unified_table
        
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        raise
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()