import ibis
import os
import re
import logging
import uuid
import shutil
import tempfile
from datetime import datetime


class SilverPipeline:
    """
    A class to handle the silver layer transformations for arbejdstilsynet_inspections data.
    Transforms raw CSV data into cleaned and structured parquet format.
    """
    def __init__(self):
        """Initialize the Silver Pipeline with paths, constants, and logging setup."""
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('SilverPipeline')
        
        # Constants and paths
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.pipeline_root = os.path.dirname(os.path.abspath(__file__))
        self.data_root = os.path.join(self.pipeline_root, "..", "bronze", "data")
        self.silver_dir = os.path.join(self.pipeline_root, "..", "silver", "data")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = os.path.join(self.silver_dir, self.timestamp)
        self.output_parquet = os.path.join(self.output_dir, "processed_data.parquet")
        self.temp_dir = None
        
        # Column renaming and conventions
        self.column_rename = {
            "Dato": "date",
            "Antal": "case_count",
            "Afgørelse": "decision",
            "Arbejdsmiljøproblem (emne)": "work_env_issue",
            "Påklaget": "appealed",
            "Efterkommet": "complied",
            "Produktionsenhed": "company_name",
            "P-nummer": "company_id",
            "Branche": "industry",
            "Produktionenhedens adresse": "company_address"
        }
        
        # DuckDB connection via Ibis
        self.con = None
        self.raw = None
        self.df = None
        self.input_csv = None

    def setup_output_directories(self):
        """Create output directories if they don't exist."""
        try:
            os.makedirs(self.silver_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            # Create a temporary directory for processing
            self.temp_dir = tempfile.mkdtemp(prefix="silver_transform_")
            self.logger.info(f"Created temporary directory: {self.temp_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Error creating output directories: {str(e)}")
            return False

    def find_latest_bronze_data(self):
        """Find the latest bronze directory and CSV file."""
        try:
            self.logger.info(f"Looking for bronze data in: {self.data_root}")
            bronze_dirs = [d for d in os.listdir(self.data_root) 
                          if os.path.isdir(os.path.join(self.data_root, d))]
            
            if not bronze_dirs:
                self.logger.error(f"No bronze data directories found in {self.data_root}")
                return False
                
            latest_bronze_dir = max(bronze_dirs)
            self.input_csv = os.path.join(self.data_root, latest_bronze_dir, "data_merged.csv")
            
            if not os.path.exists(self.input_csv):
                self.logger.error(f"Input CSV file not found: {self.input_csv}")
                return False
                
            self.logger.info(f"Found latest bronze data: {self.input_csv}")
            return True
        except Exception as e:
            self.logger.error(f"Error finding latest bronze data: {str(e)}")
            return False

    def connect_database(self):
        """Connect to DuckDB via Ibis."""
        try:
            self.con = ibis.connect("duckdb://data.ddb")
            self.logger.info("Connected to DuckDB via Ibis")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to DuckDB: {str(e)}")
            return False

    def load_data(self):
        """Load CSV data using Ibis."""
        try:
            self.raw = self.con.read_csv(self.input_csv)
            self.logger.info(f"Loaded data with {len(self.raw.columns)} columns")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data from CSV: {str(e)}")
            return False

    def rename_columns(self):
        """Rename columns according to conventions."""
        try:
            # Stepwise renaming to avoid IbisTypeError
            for old, new in self.column_rename.items():
                if old in self.raw.columns and new not in self.raw.columns:
                    # Use Ibis rename with new_name=old_name as kwargs
                    self.raw = self.raw.rename(**{new: old})
            
            self.logger.info(f"Columns after rename: {self.raw.columns}")
            return True
        except Exception as e:
            self.logger.error(f"Error renaming columns: {str(e)}")
            return False

    def validate_column_names(self):
        """Validate column names against conventions."""
        try:
            for name in self.raw.columns:
                if len(name.split("_")) > 5:
                    self.logger.error(f"Column name '{name}' exceeds 5-word limit.")
                    return False
                if not name.islower():
                    self.logger.error(f"Column name '{name}' must be lowercase.")
                    return False
            
            self.logger.info("All column names validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error validating column names: {str(e)}")
            return False

    def deduplicate(self):
        """Remove duplicate rows."""
        try:
            original_rows = self.raw.count().execute()
            self.raw = self.raw.distinct()
            new_rows = self.raw.count().execute()
            
            if original_rows > new_rows:
                self.logger.info(f"Removed {original_rows - new_rows} duplicate rows")
            else:
                self.logger.info("No duplicate rows found")
            
            return True
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            return False

    def normalize_enum_ibis(self, col):
        """Normalize a column's values by replacing Danish characters and standardizing case."""
        return (
            col.cast("string")
            .lower()
            .replace("æ", "ae")
            .replace("ø", "oe")
            .replace("å", "aa")
            .strip()
        )

    def normalize_enums(self):
        """Normalize enums and special characters."""
        try:
            for field in ["decision", "work_env_issue", "appealed", "complied", "industry"]:
                if field in self.raw.columns:
                    self.raw = self.raw.mutate(**{field: self.normalize_enum_ibis(self.raw[field])})
            
            self.logger.info("Normalized enum fields")
            return True
        except Exception as e:
            self.logger.error(f"Error normalizing enums: {str(e)}")
            return False

    def handle_null_values(self):
        """Convert empty strings to null values."""
        try:
            for field in self.raw.columns:
                if str(self.raw[field].type()) == 'string':
                    self.raw = self.raw.mutate(**{field: ibis.cases(
                        (self.raw[field].isnull() | (self.raw[field].cast('string') == ''), None),
                        else_=self.raw[field]
                    )})
            
            self.logger.info("Handled null values in string columns")
            return True
        except Exception as e:
            self.logger.error(f"Error handling null values: {str(e)}")
            return False

    def cast_types(self):
        """Cast columns to appropriate types."""
        try:
            mutate_dict = {}
            
            if "date" in self.raw.columns:
                mutate_dict["date"] = self.raw.date.cast("date")
            
            if "case_count" in self.raw.columns:
                mutate_dict["case_count"] = self.raw.case_count.cast("int64")
            
            if "company_id" in self.raw.columns:
                mutate_dict["company_id"] = self.raw.company_id.cast("int64")
            
            if "appealed" in self.raw.columns:
                mutate_dict["appealed"] = (self.raw.appealed == "paaklaget").ifelse(1, 0)
            
            if "complied" in self.raw.columns:
                mutate_dict["complied"] = (self.raw.complied == "efterkommet").ifelse(1, 0)
            
            if mutate_dict:
                self.raw = self.raw.mutate(**mutate_dict)
                self.logger.info("Cast columns to appropriate types")
            
            return True
        except Exception as e:
            self.logger.error(f"Error casting types: {str(e)}")
            return False

    def check_for_pii(self):
        """Check for potential PII data (like CPR numbers) and replace if found."""
        try:
            # Execute Ibis expression to get a pandas DataFrame
            self.df = self.raw.execute()
            
            pii_found = False
            for col in self.df.columns:
                if self.df[col].dtype == object:  # Check string columns
                    if self.df[col].astype(str).str.contains(r"\b\d{10}\b").any():
                        self.logger.warning(f"⚠️ Potential PII detected in column: {col}")
                        pii_found = True
                        # Replace with UUIDv4 if found
                        self.df[col] = self.df[col].astype(str).apply(
                            lambda v: str(uuid.uuid4()) if re.match(r"\b\d{10}\b", str(v)) else v
                        )
            
            if not pii_found:
                self.logger.info("No potential PII detected")
            
            return True
        except Exception as e:
            self.logger.error(f"Error checking for PII: {str(e)}")
            return False

    def save_output(self):
        """Save the transformed data to parquet."""
        try:
            # Save to temp location first
            temp_output = os.path.join(self.temp_dir, "processed_data.parquet")
            self.df.to_parquet(temp_output, index=False)
            
            # Move to final location
            os.makedirs(os.path.dirname(self.output_parquet), exist_ok=True)
            shutil.move(temp_output, self.output_parquet)
            
            self.logger.info(f"✅ Silver layer saved to: {self.output_parquet}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving output: {str(e)}")
            return False

    def cleanup(self):
        """Clean up temporary resources."""
        try:
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            return False

    def run(self):
        """Run the entire silver pipeline process."""
        try:
            steps = [
                self.setup_output_directories,
                self.find_latest_bronze_data,
                self.connect_database,
                self.load_data,
                self.rename_columns,
                self.validate_column_names,
                self.deduplicate,
                self.normalize_enums,
                self.handle_null_values,
                self.cast_types,
                self.check_for_pii,
                self.save_output
            ]
            
            for step in steps:
                step_name = step.__name__
                self.logger.info(f"Starting step: {step_name}")
                if not step():
                    self.logger.error(f"Pipeline failed at step: {step_name}")
                    return False
            
            self.logger.info("Silver pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Unexpected error in silver pipeline: {str(e)}")
            return False
        finally:
            self.cleanup()


def main():
    """Main function to run the silver pipeline."""
    pipeline = SilverPipeline()
    success = pipeline.run()
    
    if success:
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)