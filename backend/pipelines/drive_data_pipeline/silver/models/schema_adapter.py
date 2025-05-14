"""Schema adapter for applying schemas to data."""


import ibis
import pandas as pd

from ...utils.logging import get_logger
from ..duckdb_helper import DuckDBHelper
from .schema import ColumnSchema, DataType, TableSchema

# Get logger
logger = get_logger()


class SchemaAdapter:
    """Adapter for applying schema definitions to data."""

    def __init__(self, duckdb_helper: DuckDBHelper | None = None):
        """Initialize the schema adapter.

        Args:
            duckdb_helper: Optional DuckDBHelper instance
        """
        self.duckdb_helper = duckdb_helper or DuckDBHelper()
        logger.info("Initialized SchemaAdapter")

    def apply_schema(
        self,
        df: pd.DataFrame,
        table_schema: TableSchema,
        infer_types: bool = True,
    ) -> pd.DataFrame:
        """Apply a table schema to a DataFrame.

        Args:
            df: DataFrame to apply schema to
            table_schema: Schema to apply
            infer_types: Whether to infer types for columns not in schema

        Returns:
            DataFrame with schema applied
        """
        logger.info(f"Applying schema '{table_schema.name}' to DataFrame")

        # Create an Ibis table from the DataFrame
        table_name = table_schema.name
        ibis_table = self.duckdb_helper.dataframe_to_ibis(df, table_name)

        # Get the schema as a dictionary
        schema_dict = table_schema.column_dict

        # Apply transformations for each column in the schema
        for col_schema in table_schema.columns:
            col_name = col_schema.name

            # If the column comes from a different source column, rename it
            if col_schema.source_column and col_schema.source_column in df.columns:
                source_col = col_schema.source_column
                if col_name not in df.columns:
                    # Rename the source column to the target column name
                    ibis_table = ibis_table.mutate(
                        **{col_name: ibis_table[source_col]}
                    )

            # Skip if column doesn't exist and no transformation is specified
            if col_name not in ibis_table.columns:
                logger.warning(f"Column '{col_name}' not found in data")
                continue

            # Apply type conversions
            ibis_table = self._apply_type_conversion(
                ibis_table, col_name, col_schema
            )

            # Apply transformations if specified
            if col_schema.transform:
                ibis_table = self._apply_transformation(
                    ibis_table, col_name, col_schema.transform
                )

            # Apply default values for nulls if specified
            if col_schema.default_value is not None:
                ibis_table = ibis_table.mutate(
                    **{
                        col_name: ibis_table[col_name].fillna(
                            col_schema.default_value
                        )
                    }
                )

        # Select only the columns in the schema
        schema_columns = list(schema_dict.keys())
        
        # Add any columns not in the schema if infer_types is True
        if infer_types:
            extra_columns = [
                col for col in ibis_table.columns if col not in schema_columns
            ]
            if extra_columns:
                logger.info(
                    f"Including {len(extra_columns)} columns not in schema"
                )
                all_columns = schema_columns + extra_columns
                ibis_table = ibis_table[all_columns]
        else:
            # Only include columns in the schema
            ibis_table = ibis_table[schema_columns]

        # Convert back to DataFrame
        result_df = self.duckdb_helper.ibis_to_dataframe(ibis_table)
        
        logger.info(f"Successfully applied schema to DataFrame with {len(result_df)} rows")
        return result_df

    def _apply_type_conversion(
        self, table: ibis.expr.types.Table, col_name: str, col_schema: ColumnSchema
    ) -> ibis.expr.types.Table:
        """Apply type conversion to a column.

        Args:
            table: Ibis table
            col_name: Column name
            col_schema: Column schema

        Returns:
            Ibis table with type conversion applied
        """
        target_type = col_schema.data_type
        
        # Map schema types to DuckDB types
        type_map = {
            DataType.STRING: "string",
            DataType.INTEGER: "int64",
            DataType.FLOAT: "double",
            DataType.BOOLEAN: "boolean",
            DataType.DATE: "date",
            DataType.TIMESTAMP: "timestamp",
            # Geometry type requires special handling
            DataType.GEOMETRY: "string",  # Store as WKT string initially
        }
        
        if target_type in type_map:
            duckdb_type = type_map[target_type]
            try:
                # Special handling for date/timestamp
                if target_type == DataType.DATE:
                    # Try to parse as date
                    table = table.mutate(
                        **{col_name: table[col_name].cast('date')}
                    )
                elif target_type == DataType.TIMESTAMP:
                    # Try to parse as timestamp
                    table = table.mutate(
                        **{col_name: table[col_name].cast('timestamp')}
                    )
                else:
                    # Standard type cast
                    table = table.mutate(
                        **{col_name: table[col_name].cast(duckdb_type)}
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to convert column '{col_name}' to {target_type}: {str(e)}"
                )
        
        return table

    def _apply_transformation(
        self, table: ibis.expr.types.Table, col_name: str, transform: str
    ) -> ibis.expr.types.Table:
        """Apply a transformation to a column.

        Args:
            table: Ibis table
            col_name: Column name
            transform: Transformation expression

        Returns:
            Ibis table with transformation applied
        """
        try:
            # This is a simple implementation - in a real system, you would
            # need a more sophisticated expression parser/evaluator
            
            # Try to parse the transform as a SQL expression
            # For example, "UPPER(field_name)" or "field1 + field2"
            sql = f"SELECT *, {transform} AS {col_name}_new FROM data"
            
            # Execute the SQL to transform the column
            conn = self.duckdb_helper.conn
            conn.register("data", table.compile().compile())
            result = conn.query(sql).to_arrow_table()
            
            # Convert back to Ibis table
            new_table = ibis.backends.duckdb.from_pyarrow(
                result, self.duckdb_helper.ibis_conn
            )
            
            # Replace the original column with the transformed column
            table = new_table.mutate(
                **{col_name: new_table[f"{col_name}_new"]}
            )
            
            # Drop the temporary column
            columns = [c for c in table.columns if c != f"{col_name}_new"]
            table = table[columns]
            
            logger.debug(f"Applied transformation to column '{col_name}'")
            
        except Exception as e:
            logger.warning(
                f"Failed to apply transformation to column '{col_name}': {str(e)}"
            )
        
        return table

    def validate_data_against_schema(
        self, df: pd.DataFrame, table_schema: TableSchema
    ) -> dict[str, list[str]]:
        """Validate data against a schema.

        Args:
            df: DataFrame to validate
            table_schema: Schema to validate against

        Returns:
            Dictionary of validation errors by column
        """
        validation_errors = {}
        
        # Get the schema as a dictionary
        schema_dict = table_schema.column_dict
        
        # Check for required columns
        for col_name, col_schema in schema_dict.items():
            if not col_schema.nullable and col_name not in df.columns:
                if col_name not in validation_errors:
                    validation_errors[col_name] = []
                validation_errors[col_name].append(
                    f"Required column '{col_name}' is missing"
                )
        
        # Validate each column
        for col_name, col_schema in schema_dict.items():
            if col_name not in df.columns:
                continue
            
            col_errors = []
            
            # Check non-null constraint
            if not col_schema.nullable and df[col_name].isna().any():
                col_errors.append("Contains NULL values but is defined as non-nullable")
            
            # Check numeric constraints
            if col_schema.data_type in (DataType.INTEGER, DataType.FLOAT):
                # Check minimum value
                if col_schema.min_value is not None:
                    min_val = df[col_name].min()
                    if not pd.isna(min_val) and min_val < col_schema.min_value:
                        col_errors.append(
                            f"Contains values less than minimum ({col_schema.min_value})"
                        )
                
                # Check maximum value
                if col_schema.max_value is not None:
                    max_val = df[col_name].max()
                    if not pd.isna(max_val) and max_val > col_schema.max_value:
                        col_errors.append(
                            f"Contains values greater than maximum ({col_schema.max_value})"
                        )
            
            # Check uniqueness
            if col_schema.unique and not df[col_name].is_unique:
                col_errors.append("Contains duplicate values but should be unique")
            
            # Check pattern constraint for strings
            if (
                col_schema.pattern
                and col_schema.data_type == DataType.STRING
                and not df[col_name].str.contains(col_schema.pattern).all()
            ):
                col_errors.append(
                    f"Contains values that don't match pattern '{col_schema.pattern}'"
                )
            
            # Add errors to the result
            if col_errors:
                validation_errors[col_name] = col_errors
        
        return validation_errors

    def get_table_schema_sql(self, table_schema: TableSchema) -> str:
        """Generate SQL CREATE TABLE statement from a schema.

        Args:
            table_schema: Schema to generate SQL for

        Returns:
            SQL CREATE TABLE statement
        """
        # Map schema types to SQL types
        type_map = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "INTEGER",
            DataType.FLOAT: "DOUBLE",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.GEOMETRY: "VARCHAR",  # Store as WKT string
        }
        
        # Build column definitions
        columns = []
        for col in table_schema.columns:
            # Get SQL type
            sql_type = type_map.get(col.data_type, "VARCHAR")
            
            # Build column definition
            col_def = f'"{col.name}" {sql_type}'
            
            # Add NOT NULL constraint
            if not col.nullable:
                col_def += " NOT NULL"
            
            # Add UNIQUE constraint
            if col.unique:
                col_def += " UNIQUE"
            
            columns.append(col_def)
        
        # Add primary key
        if table_schema.primary_key:
            if isinstance(table_schema.primary_key, str):
                columns.append(f'PRIMARY KEY ("{table_schema.primary_key}")')
            else:
                pk_cols = '", "'.join(table_schema.primary_key)
                columns.append(f'PRIMARY KEY ("{pk_cols}")')
        
        # Build CREATE TABLE statement
        sql = f'CREATE TABLE "{table_schema.name}" (\n'
        sql += ',\n'.join(f'  {col}' for col in columns)
        sql += '\n)'
        
        return sql 