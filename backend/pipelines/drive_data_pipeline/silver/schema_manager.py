"""Schema management for Silver layer."""

import json
from pathlib import Path

from ..utils.logging import get_logger

# Get logger
logger = get_logger()


class SchemaManager:
    """Manager for data schemas in the Silver layer."""

    def __init__(self, schema_dir: Path | None = None):
        """Initialize the schema manager.

        Args:
            schema_dir: Directory containing schema definitions (optional)
        """
        self.schema_dir = schema_dir
        self.schemas = {}
        
        # Load schemas if directory is provided
        if schema_dir and schema_dir.exists():
            self._load_schemas()
            
        logger.info(f"Initialized schema manager with {len(self.schemas)} schemas")

    def _load_schemas(self):
        """Load schemas from the schema directory."""
        try:
            # Look for JSON schema files
            schema_files = list(self.schema_dir.glob("*.json"))
            
            for schema_file in schema_files:
                try:
                    with open(schema_file, encoding="utf-8") as f:
                        schema = json.load(f)
                    
                    # Schema name is the filename without extension
                    schema_name = schema_file.stem
                    self.schemas[schema_name] = schema
                    
                    logger.debug(f"Loaded schema: {schema_name}")
                
                except Exception as e:
                    logger.warning(f"Failed to load schema {schema_file}: {str(e)}")
            
            logger.info(f"Loaded {len(self.schemas)} schemas from {self.schema_dir}")
        
        except Exception as e:
            logger.error(f"Failed to load schemas: {str(e)}")

    def get_schema(self, schema_name: str) -> dict | None:
        """Get a schema by name.

        Args:
            schema_name: Name of the schema

        Returns:
            Schema dictionary or None if not found
        """
        return self.schemas.get(schema_name)

    def create_schema(
        self, schema_name: str, schema: dict, save_to_file: bool = True
    ) -> bool:
        """Create a new schema.

        Args:
            schema_name: Name of the schema
            schema: Schema dictionary
            save_to_file: Whether to save the schema to a file

        Returns:
            True if the schema was created successfully, False otherwise
        """
        try:
            # Add schema to the manager
            self.schemas[schema_name] = schema
            
            # Save to file if requested
            if save_to_file and self.schema_dir:
                schema_file = self.schema_dir / f"{schema_name}.json"
                
                # Ensure directory exists
                self.schema_dir.mkdir(parents=True, exist_ok=True)
                
                # Save schema as JSON
                with open(schema_file, "w", encoding="utf-8") as f:
                    json.dump(schema, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved schema to {schema_file}")
            
            logger.info(f"Created schema: {schema_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {str(e)}")
            return False

    def update_schema(
        self, schema_name: str, schema: dict, save_to_file: bool = True
    ) -> bool:
        """Update an existing schema.

        Args:
            schema_name: Name of the schema
            schema: New schema dictionary
            save_to_file: Whether to save the schema to a file

        Returns:
            True if the schema was updated successfully, False otherwise
        """
        # Check if schema exists
        if schema_name not in self.schemas:
            logger.warning(f"Schema {schema_name} does not exist")
            return False
        
        # Update the schema
        return self.create_schema(schema_name, schema, save_to_file)

    def delete_schema(self, schema_name: str, delete_file: bool = True) -> bool:
        """Delete a schema.

        Args:
            schema_name: Name of the schema
            delete_file: Whether to delete the schema file

        Returns:
            True if the schema was deleted successfully, False otherwise
        """
        try:
            # Check if schema exists
            if schema_name not in self.schemas:
                logger.warning(f"Schema {schema_name} does not exist")
                return False
            
            # Remove from memory
            del self.schemas[schema_name]
            
            # Delete file if requested
            if delete_file and self.schema_dir:
                schema_file = self.schema_dir / f"{schema_name}.json"
                if schema_file.exists():
                    schema_file.unlink()
                    logger.info(f"Deleted schema file: {schema_file}")
            
            logger.info(f"Deleted schema: {schema_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to delete schema {schema_name}: {str(e)}")
            return False

    def get_schema_by_subfolder(self, subfolder: str) -> dict | None:
        """Get a schema based on the subfolder name.

        This method tries to find a schema matching the subfolder name or a prefix.

        Args:
            subfolder: Subfolder name

        Returns:
            Schema dictionary or None if not found
        """
        # Try exact match
        if subfolder in self.schemas:
            return self.schemas[subfolder]
        
        # Try normalized name (lowercase, underscores)
        normalized = subfolder.lower().replace(" ", "_")
        if normalized in self.schemas:
            return self.schemas[normalized]
        
        # Try prefix match
        for name, schema in self.schemas.items():
            if subfolder.startswith(name) or name.startswith(subfolder):
                return schema
        
        logger.warning(f"No schema found for subfolder: {subfolder}")
        return None

    def list_schemas(self) -> list[str]:
        """List all available schemas.

        Returns:
            List of schema names
        """
        return list(self.schemas.keys()) 