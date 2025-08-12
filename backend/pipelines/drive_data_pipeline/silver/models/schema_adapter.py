"""Schema adapter for applying schemas to data using DuckDB."""

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger():
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
from .schema import ColumnSchema, DataType, TableSchema

# Get logger
logger = get_logger()


class SchemaAdapter(DuckDBProcessor):
    """Adapter for applying schema definitions to data using DuckDB."""

    def __init__(self) -> None:
        """Initialize the schema adapter."""
        super().__init__()
        logger.info("Initialized SchemaAdapter with DuckDB")

    def apply_schema(
        self,
        table_name_or_data: any,
        table_schema: TableSchema,
        infer_types: bool = True,
    ) -> str:
        """Apply a table schema to data using DuckDB.

        Args:
            table_name_or_data: DuckDB table name (str) or data to apply schema to
            table_schema: Schema to apply
            infer_types: Whether to infer types for columns not in schema

        Returns:
            DuckDB table name with schema applied
        """
        logger.info(f"Applying schema '{table_schema.name}' using DuckDB")

        # Handle different input types
        if isinstance(table_name_or_data, str):
            source_table = table_name_or_data
        else:
            # Register data as a table
            source_table = f"schema_source_{table_schema.name}"
            self.register_table(table_name_or_data, source_table)

        # Create result table name
        result_table = f"{table_schema.name}_with_schema"

        # Get the schema as a dictionary

        # Get source table columns
        columns_info = self.conn.execute(f"DESCRIBE {source_table}").fetchall()
        source_columns = [col[0] for col in columns_info]

        # Build the transformation query
        select_parts = []

        # Apply transformations for each column in the schema
        for col_schema in table_schema.columns:
            col_name = col_schema.name
            source_col = col_schema.source_column or col_name

            # Skip if source column doesn't exist
            if source_col not in source_columns:
                if col_schema.default_value is not None:
                    # Use default value
                    default_val = self._format_default_value(
                        col_schema.default_value, col_schema.data_type
                    )
                    select_parts.append(f"{default_val} AS {col_name}")
                else:
                    logger.warning(
                        f"Source column '{source_col}' not found for schema column '{col_name}'"
                    )
                continue

            # Build the column transformation
            column_expr = self._build_column_transformation(source_col, col_schema)
            select_parts.append(f"{column_expr} AS {col_name}")

        # Add any columns not in the schema if infer_types is True
        if infer_types:
            schema_source_columns = {
                col_schema.source_column or col_schema.name for col_schema in table_schema.columns
            }
            extra_columns = [col for col in source_columns if col not in schema_source_columns]
            if extra_columns:
                logger.info(f"Including {len(extra_columns)} columns not in schema")
                select_parts.extend(extra_columns)

        # Create the result table
        if select_parts:
            self.conn.execute(f"""
                CREATE TABLE {result_table} AS
                SELECT {", ".join(select_parts)}
                FROM {source_table}
            """)
        else:
            logger.error("No columns to select after schema application")
            return source_table

        logger.info(f"Successfully applied schema to create table {result_table}")
        return result_table

    def _build_column_transformation(self, source_col: str, col_schema: ColumnSchema) -> str:
        """Build the SQL expression for column transformation.

        Args:
            source_col: Source column name
            col_schema: Column schema definition

        Returns:
            SQL expression for the column transformation
        """
        expr = source_col

        # Apply custom transformation if specified
        if col_schema.transform:
            # Replace placeholder with actual column name
            expr = col_schema.transform.replace("${column}", source_col)

        # Apply type conversion
        expr = self._apply_type_conversion(expr, col_schema.data_type)

        # Apply default value for nulls if specified
        if col_schema.default_value is not None:
            default_val = self._format_default_value(col_schema.default_value, col_schema.data_type)
            expr = f"COALESCE({expr}, {default_val})"

        return expr

    def _apply_type_conversion(self, expr: str, target_type: DataType) -> str:
        """Apply type conversion to an expression.

        Args:
            expr: SQL expression
            target_type: Target data type

        Returns:
            SQL expression with type conversion
        """
        # Map schema types to DuckDB types
        type_map = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "BIGINT",
            DataType.FLOAT: "DOUBLE",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.GEOMETRY: "VARCHAR",  # Store as WKT string
        }

        if target_type in type_map:
            duckdb_type = type_map[target_type]

            # Special handling for different types
            if target_type == DataType.DATE:
                return f"TRY_CAST({expr} AS DATE)"
            elif target_type == DataType.TIMESTAMP:
                return f"TRY_CAST({expr} AS TIMESTAMP)"
            elif target_type == DataType.BOOLEAN:
                # Handle various boolean representations
                return f"""
                    CASE 
                        WHEN LOWER(CAST({expr} AS VARCHAR)) IN ('true', '1', 'yes', 'ja', 't', 'y') THEN true
                        WHEN LOWER(CAST({expr} AS VARCHAR)) IN ('false', '0', 'no', 'nej', 'f', 'n') THEN false
                        ELSE TRY_CAST({expr} AS BOOLEAN)
                    END
                """
            else:
                return f"TRY_CAST({expr} AS {duckdb_type})"

        return expr

    def _format_default_value(self, default_value: any, data_type: DataType) -> str:
        """Format a default value for SQL.

        Args:
            default_value: The default value
            data_type: The target data type

        Returns:
            SQL-formatted default value
        """
        if default_value is None:
            return "NULL"

        if data_type in [DataType.STRING, DataType.GEOMETRY]:
            return f"'{default_value}'"
        elif data_type == DataType.DATE:
            return f"DATE '{default_value}'"
        elif data_type == DataType.TIMESTAMP:
            return f"TIMESTAMP '{default_value}'"
        elif data_type == DataType.BOOLEAN:
            return "true" if default_value else "false"
        else:
            return str(default_value)

    def validate_data_against_schema(
        self, table_name_or_data: any, table_schema: TableSchema
    ) -> dict[str, list[str]]:
        """Validate data against a schema using DuckDB.

        Args:
            table_name_or_data: DuckDB table name (str) or data to validate
            table_schema: Schema to validate against

        Returns:
            Dictionary of validation errors by column
        """
        validation_errors = {}

        try:
            # Handle different input types
            if isinstance(table_name_or_data, str):
                table_name = table_name_or_data
            else:
                # Register data as a table
                table_name = f"validation_{table_schema.name}"
                self.register_table(table_name_or_data, table_name)

            # Get table information
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            available_columns = [col[0] for col in columns_info]

            # Check each column in the schema
            for col_schema in table_schema.columns:
                col_name = col_schema.name
                source_col = col_schema.source_column or col_name

                column_errors = []

                # Check if required column exists
                if source_col not in available_columns:
                    if col_schema.default_value is None:
                        column_errors.append(f"Required column '{source_col}' is missing")
                    continue

                # Validate data type compatibility
                try:
                    # Try to convert a sample of the data
                    sample_query = f"""
                        SELECT COUNT(*) as total_count,
                               COUNT(CASE WHEN TRY_CAST({source_col} AS {self._get_duckdb_type(col_schema.data_type)}) IS NOT NULL THEN 1 END) as valid_count
                        FROM {table_name}
                        WHERE {source_col} IS NOT NULL
                        LIMIT 1000
                    """

                    result = self.conn.execute(sample_query).fetchone()
                    total_count, valid_count = result

                    if (
                        total_count > 0 and valid_count < total_count * 0.95
                    ):  # Allow 5% conversion failures
                        invalid_count = total_count - valid_count
                        column_errors.append(
                            f"Data type validation failed: {invalid_count}/{total_count} values cannot be converted to {col_schema.data_type.value}"
                        )

                except Exception as e:
                    column_errors.append(f"Type validation error: {str(e)}")

                # Check for null values if column is required
                if not col_schema.nullable:
                    try:
                        null_count = self.conn.execute(f"""
                            SELECT COUNT(*) FROM {table_name} 
                            WHERE {source_col} IS NULL
                        """).fetchone()[0]

                        if null_count > 0:
                            column_errors.append(
                                f"Found {null_count} null values in non-nullable column"
                            )

                    except Exception as e:
                        column_errors.append(f"Null check error: {str(e)}")

                if column_errors:
                    validation_errors[col_name] = column_errors

        except Exception as e:
            validation_errors["_general"] = [f"Schema validation failed: {str(e)}"]

        return validation_errors

    def _get_duckdb_type(self, data_type: DataType) -> str:
        """Get DuckDB type string for a DataType.

        Args:
            data_type: Schema data type

        Returns:
            DuckDB type string
        """
        type_map = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "BIGINT",
            DataType.FLOAT: "DOUBLE",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.GEOMETRY: "VARCHAR",
        }
        return type_map.get(data_type, "VARCHAR")

    def get_table_schema_sql(self, table_schema: TableSchema) -> str:
        """Generate CREATE TABLE SQL for a schema.

        Args:
            table_schema: Schema to generate SQL for

        Returns:
            CREATE TABLE SQL statement
        """
        columns = []
        for col_schema in table_schema.columns:
            col_def = f"{col_schema.name} {self._get_duckdb_type(col_schema.data_type)}"

            if not col_schema.nullable:
                col_def += " NOT NULL"

            if col_schema.default_value is not None:
                default_val = self._format_default_value(
                    col_schema.default_value, col_schema.data_type
                )
                col_def += f" DEFAULT {default_val}"

            columns.append(col_def)

        columns_str = ",\n    ".join(columns)
        return f"CREATE TABLE {table_schema.name} (\n    {columns_str}\n)"
