"""Base class for gold layer processing."""

from typing import List

from .duckdb_processor import DuckDBProcessor


class GoldBase(DuckDBProcessor):
    """Base class for gold layer analytics and aggregations."""

    def __init__(self, dataset_name: str, db_path: str = ":memory:"):
        super().__init__(db_path, dataset_name)

    def create_analytics_table(self, silver_tables: List[str], **kwargs) -> str:
        """
        Create analytics table from silver layer data.

        Args:
            silver_tables: List of silver table names to process
            **kwargs: Additional processing parameters

        Returns:
            str: Name of created gold table
        """
        raise NotImplementedError("Subclasses must implement create_analytics_table method")

    def aggregate_data(self, table_name: str, group_by: List[str], metrics: List[str]) -> str:
        """
        Create aggregated data table.

        Args:
            table_name: Source table name
            group_by: List of columns to group by
            metrics: List of metrics to calculate

        Returns:
            str: Name of aggregated table
        """
        agg_table = f"agg_{table_name}_{int(time.time())}"

        group_clause = ", ".join(group_by)
        metric_clause = ", ".join(metrics)

        self.conn.execute(f"""
            CREATE TABLE {agg_table} AS
            SELECT 
                {group_clause},
                {metric_clause}
            FROM {table_name}
            GROUP BY {group_clause}
        """)

        return agg_table

    def create_summary_statistics(self, table_name: str) -> str:
        """
        Create summary statistics table.

        Args:
            table_name: Source table name

        Returns:
            str: Name of summary statistics table
        """
        stats_table = f"stats_{table_name}_{int(time.time())}"

        # Get numeric columns
        columns_info = self.get_table_info(table_name)
        numeric_columns = [
            col[0] for col in columns_info if col[1] in ["DOUBLE", "INTEGER", "BIGINT"]
        ]

        if numeric_columns:
            stats_queries = []
            for col in numeric_columns:
                stats_queries.append(f"""
                    SELECT 
                        '{col}' as column_name,
                        COUNT({col}) as count,
                        AVG({col}) as mean,
                        MIN({col}) as min_val,
                        MAX({col}) as max_val,
                        STDDEV({col}) as std_dev
                    FROM {table_name}
                """)

            union_query = " UNION ALL ".join(stats_queries)

            self.conn.execute(f"""
                CREATE TABLE {stats_table} AS
                {union_query}
            """)

        return stats_table
