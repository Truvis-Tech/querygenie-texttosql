import sqlglot
from sqlglot import exp
from typing import Dict, List, Set
import re
import logging

logger = logging.getLogger(__name__)

class BigQueryTableColumnExtractor:
    def __init__(self, default_project: str = None, default_dataset: str = None):
        self.default_project = default_project
        self.default_dataset = default_dataset
        
    def extract_tables_and_columns(self, sql_query: str) -> Dict[str, List[str]]:
        """
        Extracts tables and their associated columns from a BigQuery SQL query.
        Returns a dictionary mapping fully qualified table names to a list of their columns.
        """
        try:
            parsed = sqlglot.parse(sql_query, read='bigquery')
            if not parsed:
                logger.warning("Could not parse SQL query.")
                return {}

            table_columns = {}
            for statement in parsed:
                self._extract_from_statement(statement, table_columns)
            
            # Convert sets to lists for the final output
            return {table: sorted(list(cols)) for table, cols in table_columns.items()}

        except Exception as e:
            logger.error(f"Error parsing SQL: {e}")
            return {}

    def _extract_from_statement(self, statement: exp.Expression, table_columns: Dict[str, Set[str]]):
        alias_to_table = {}
        
        # First pass: find all tables and their aliases
        for table_node in statement.find_all(exp.Table):
            full_table_name = self._get_full_table_name(table_node)
            if full_table_name not in table_columns:
                table_columns[full_table_name] = set()

            alias = table_node.alias_or_name
            if alias and alias != full_table_name:
                alias_to_table[alias] = full_table_name

        # Second pass: associate columns with their tables
        for column_node in statement.find_all(exp.Column):
            column_name = column_node.name
            table_ref = column_node.table

            if table_ref:
                resolved_table = alias_to_table.get(table_ref, table_ref)
                # Find the full table name that matches the resolved alias/name
                for fq_name in table_columns:
                    if fq_name.endswith(f".{resolved_table}") or fq_name == resolved_table:
                        table_columns[fq_name].add(column_name)
                        break
            else:
                # If column is not qualified, it could belong to any table in the FROM clause.
                # This is an ambiguous case. For simplicity, we can add it to all tables, 
                # or decide on a more sophisticated resolving strategy.
                # Here, we'll add it to all tables found in the query.
                for table_name in table_columns:
                    table_columns[table_name].add(column_name)

    def _get_full_table_name(self, table_node: exp.Table) -> str:
        """Constructs a fully qualified table name."""
        parts = [p.name for p in table_node.parts]
        
        # Reconstruct from parts, assuming [project, dataset, table]
        if len(parts) == 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        if len(parts) == 2 and self.default_project:
            return f"{self.default_project}.{parts[0]}.{parts[1]}"
        if len(parts) == 1 and self.default_project and self.default_dataset:
            return f"{self.default_project}.{self.default_dataset}.{parts[0]}"
        
        return table_node.sql(dialect='bigquery')

def extract_table_columns_from_query(sql_query: str, 
                                   default_project: str = None, 
                                   default_dataset: str = None) -> Dict[str, List[str]]:
    """Main function to extract tables and their columns from a BigQuery SQL query."""
    extractor = BigQueryTableColumnExtractor(default_project, default_dataset)
    return extractor.extract_tables_and_columns(sql_query)
