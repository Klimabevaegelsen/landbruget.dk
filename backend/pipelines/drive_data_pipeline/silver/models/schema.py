"""Schema models for Silver layer data standardization."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DataType(str, Enum):
    """Data types supported in Silver layer schemas."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    GEOMETRY = "geometry"


class ColumnSchema(BaseModel):
    """Schema definition for a single column."""

    name: str = Field(..., description="Column name")
    data_type: DataType = Field(..., description="Data type")
    nullable: bool = Field(True, description="Whether the column can be null")
    description: str | None = Field(None, description="Column description")
    
    # Validation constraints
    min_value: int | float | None = Field(
        None, description="Minimum value (for numeric types)"
    )
    max_value: int | float | None = Field(
        None, description="Maximum value (for numeric types)"
    )
    unique: bool = Field(False, description="Whether values must be unique")
    pattern: str | None = Field(
        None, description="Regex pattern for validation (for string type)"
    )
    
    # Transformations
    transform: str | None = Field(
        None, description="Transformation expression to apply"
    )
    source_column: str | None = Field(
        None, description="Source column name if different"
    )
    default_value: Any | None = Field(
        None, description="Default value for missing data"
    )
    
    # Geospatial properties
    srid: int | None = Field(
        None, description="Spatial reference ID (for geometry type)"
    )
    geometry_type: str | None = Field(
        None, description="Geometry type (point, line, polygon, etc.)"
    )


class TableSchema(BaseModel):
    """Schema definition for a table."""

    name: str = Field(..., description="Table name")
    description: str | None = Field(None, description="Table description")
    columns: list[ColumnSchema] = Field(..., description="Column schemas")
    partition_by: list[str] | None = Field(
        None, description="List of columns to partition by"
    )
    primary_key: str | list[str] | None = Field(
        None, description="Primary key column(s)"
    )
    source_type: str | None = Field(
        None, description="Source file type (PDF, Excel, etc.)"
    )
    
    @property
    def column_dict(self) -> dict[str, ColumnSchema]:
        """Get columns as a dictionary keyed by name.
        
        Returns:
            Dictionary mapping column names to schemas
        """
        return {col.name: col for col in self.columns}


class DatasetSchema(BaseModel):
    """Schema definition for a complete dataset."""

    name: str = Field(..., description="Dataset name")
    description: str | None = Field(None, description="Dataset description")
    version: str = Field("1.0", description="Schema version")
    tables: list[TableSchema] = Field(..., description="Table schemas")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @property
    def table_dict(self) -> dict[str, TableSchema]:
        """Get tables as a dictionary keyed by name.
        
        Returns:
            Dictionary mapping table names to schemas
        """
        return {table.name: table for table in self.tables}
    
    def to_dict(self) -> dict[str, Any]:
        """Convert the schema to a dictionary.
        
        Returns:
            Dictionary representation of the schema
        """
        return self.dict(by_alias=True, exclude_none=True)
    
    def to_json(self) -> str:
        """Convert the schema to a JSON string.
        
        Returns:
            JSON string representation of the schema
        """
        return self.json(by_alias=True, exclude_none=True, indent=2)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DatasetSchema":
        """Create a schema from a dictionary.
        
        Args:
            data: Dictionary representation of the schema
            
        Returns:
            DatasetSchema instance
        """
        return cls.parse_obj(data)


# Example schemas for common data types

def create_animal_welfare_schema() -> DatasetSchema:
    """Create a standard schema for animal welfare data.
    
    Returns:
        DatasetSchema for animal welfare data
    """
    return DatasetSchema(
        name="animal_welfare",
        description="Danish animal welfare inspection data",
        tables=[
            TableSchema(
                name="inspections",
                description="Animal welfare inspections",
                columns=[
                    ColumnSchema(
                        name="inspection_id",
                        data_type=DataType.STRING,
                        nullable=False,
                        description="Unique identifier for the inspection",
                        unique=True,
                    ),
                    ColumnSchema(
                        name="inspection_date",
                        data_type=DataType.DATE,
                        description="Date of inspection",
                    ),
                    ColumnSchema(
                        name="farm_id",
                        data_type=DataType.STRING,
                        description="Identifier for the inspected farm",
                    ),
                    ColumnSchema(
                        name="animal_type",
                        data_type=DataType.STRING,
                        description="Type of animals inspected",
                    ),
                    ColumnSchema(
                        name="violation_count",
                        data_type=DataType.INTEGER,
                        description="Number of violations found",
                        min_value=0,
                    ),
                    ColumnSchema(
                        name="location",
                        data_type=DataType.GEOMETRY,
                        description="Geographic location of the farm",
                        srid=4326,
                        geometry_type="point",
                    ),
                ],
                primary_key="inspection_id",
            )
        ],
    )


def create_farm_schema() -> DatasetSchema:
    """Create a standard schema for farm data.
    
    Returns:
        DatasetSchema for farm data
    """
    return DatasetSchema(
        name="farm_data",
        description="Danish farm and agricultural land data",
        tables=[
            TableSchema(
                name="farms",
                description="Farm information",
                columns=[
                    ColumnSchema(
                        name="farm_id",
                        data_type=DataType.STRING,
                        nullable=False,
                        description="Unique identifier for the farm",
                        unique=True,
                    ),
                    ColumnSchema(
                        name="farm_name",
                        data_type=DataType.STRING,
                        description="Name of the farm",
                    ),
                    ColumnSchema(
                        name="farm_type",
                        data_type=DataType.STRING,
                        description="Type of farm (e.g., dairy, pig, crop)",
                    ),
                    ColumnSchema(
                        name="municipality",
                        data_type=DataType.STRING,
                        description="Municipality where the farm is located",
                    ),
                    ColumnSchema(
                        name="total_area_ha",
                        data_type=DataType.FLOAT,
                        description="Total area in hectares",
                        min_value=0,
                    ),
                    ColumnSchema(
                        name="organic",
                        data_type=DataType.BOOLEAN,
                        description="Whether the farm is organic",
                    ),
                    ColumnSchema(
                        name="location",
                        data_type=DataType.GEOMETRY,
                        description="Geographic location of the farm",
                        srid=4326,
                        geometry_type="point",
                    ),
                    ColumnSchema(
                        name="boundary",
                        data_type=DataType.GEOMETRY,
                        description="Farm boundary",
                        srid=4326,
                        geometry_type="polygon",
                    ),
                ],
                primary_key="farm_id",
            ),
            TableSchema(
                name="fields",
                description="Agricultural fields",
                columns=[
                    ColumnSchema(
                        name="field_id",
                        data_type=DataType.STRING,
                        nullable=False,
                        description="Unique identifier for the field",
                        unique=True,
                    ),
                    ColumnSchema(
                        name="farm_id",
                        data_type=DataType.STRING,
                        description="Identifier for the farm this field belongs to",
                    ),
                    ColumnSchema(
                        name="crop_type",
                        data_type=DataType.STRING,
                        description="Type of crop grown in the field",
                    ),
                    ColumnSchema(
                        name="area_ha",
                        data_type=DataType.FLOAT,
                        description="Area in hectares",
                        min_value=0,
                    ),
                    ColumnSchema(
                        name="boundary",
                        data_type=DataType.GEOMETRY,
                        description="Field boundary",
                        srid=4326,
                        geometry_type="polygon",
                    ),
                ],
                primary_key="field_id",
            ),
        ],
    ) 