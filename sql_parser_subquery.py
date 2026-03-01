import sqlglot
from sqlglot import exp
from typing import Dict, List, Set, Optional
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
        Only returns actual base tables, not CTEs or subqueries.
        """
        try:
            parsed = sqlglot.parse(sql_query, read='bigquery')
            if not parsed:
                logger.warning("Could not parse SQL query.")
                return {}
            
            table_columns = {}
            for statement in parsed:
                self._extract_from_statement(statement, table_columns)
            
            # Filter out CTEs and subqueries, keep only base tables
            base_tables = {
                table: cols for table, cols in table_columns.items()
                if not table.startswith('__cte__.') and not table.startswith('__subquery__.')
            }
            
            # Convert sets to lists for the final output
            return {table: sorted(list(cols)) for table, cols in base_tables.items()}
        except Exception as e:
            logger.error(f"Error parsing SQL: {e}")
            return {}
    
    def _extract_from_statement(self, statement: exp.Expression, table_columns: Dict[str, Set[str]]):
        """Extract tables and columns from a SQL statement."""
        alias_to_table = {}
        table_to_alias = {}
        
        # First pass: find all tables and their aliases (including CTEs and subqueries)
        self._collect_table_references(statement, table_columns, alias_to_table, table_to_alias)
        
        # Second pass: associate columns with their tables
        self._associate_columns_with_tables(statement, table_columns, alias_to_table)
    
    def _collect_table_references(self, node: exp.Expression, 
                                  table_columns: Dict[str, Set[str]],
                                  alias_to_table: Dict[str, str],
                                  table_to_alias: Dict[str, str]):
        """Collect all table references including CTEs and subqueries."""
        # Handle CTEs (WITH clauses)
        for cte in node.find_all(exp.CTE):
            cte_alias = cte.alias_or_name
            if cte_alias:
                alias_to_table[cte_alias] = f"__cte__.{cte_alias}"
                table_columns[f"__cte__.{cte_alias}"] = set()
                
                # RECURSIVELY process the CTE's query to find base tables
                cte_query = cte.this
                if cte_query:
                    self._collect_table_references(cte_query, table_columns, alias_to_table, table_to_alias)
        
        # Handle regular tables
        for table_node in node.find_all(exp.Table):
            full_table_name = self._get_full_table_name(table_node)
            if full_table_name not in table_columns:
                table_columns[full_table_name] = set()
            
            alias = table_node.alias_or_name
            if alias:
                alias_to_table[alias] = full_table_name
                table_to_alias[full_table_name] = alias
        
        # Handle subqueries with aliases - RECURSIVELY process them
        for subquery in node.find_all(exp.Subquery):
            alias = subquery.alias_or_name
            if alias:
                # Track the subquery as a virtual table
                alias_to_table[alias] = f"__subquery__.{alias}"
                table_columns[f"__subquery__.{alias}"] = set()
                
                # RECURSIVELY process the subquery to find base tables inside it
                subquery_query = subquery.this
                if subquery_query:
                    # Create a temporary mapping for tables inside the subquery
                    temp_alias_to_table = {}
                    temp_table_to_alias = {}
                    self._collect_table_references(subquery_query, table_columns, 
                                                  temp_alias_to_table, temp_table_to_alias)
    
    def _associate_columns_with_tables(self, statement: exp.Expression,
                                      table_columns: Dict[str, Set[str]],
                                      alias_to_table: Dict[str, str]):
        """Associate columns with their source tables."""
        # Track which columns are selected/used
        for column_node in statement.find_all(exp.Column):
            column_name = column_node.name
            
            # Skip if this is not a real column (e.g., part of a function call context)
            if not column_name or column_name == '*':
                continue
            
            table_ref = column_node.table
            
            if table_ref:
                # Column is qualified (e.g., table.column)
                resolved_table = self._resolve_table_reference(table_ref, alias_to_table, table_columns)
                if resolved_table:
                    table_columns[resolved_table].add(column_name)
            else:
                # Unqualified column - try to be smart about assignment
                self._handle_unqualified_column(column_node, column_name, table_columns, statement)
    
    def _resolve_table_reference(self, table_ref: str, 
                                alias_to_table: Dict[str, str],
                                table_columns: Dict[str, Set[str]]) -> Optional[str]:
        """Resolve a table reference to its fully qualified name."""
        # Direct alias match
        if table_ref in alias_to_table:
            return alias_to_table[table_ref]
        
        # Check if it's already a full table name
        if table_ref in table_columns:
            return table_ref
        
        # Try to find partial matches
        for fq_name in table_columns:
            if fq_name.endswith(f".{table_ref}"):
                return fq_name
            # Handle case where table_ref might be dataset.table
            if '.' in table_ref and fq_name.endswith(table_ref):
                return fq_name
        
        return None
    
    def _handle_unqualified_column(self, column_node: exp.Column, 
                                   column_name: str,
                                   table_columns: Dict[str, Set[str]],
                                   statement: exp.Expression):
        """Handle unqualified columns with improved logic."""
        # Check if column is in SELECT clause (output column)
        parent = column_node.parent
        
        # If it's in a SELECT, it likely comes from the FROM clause
        if self._is_in_select_clause(column_node):
            # Add to all base tables (not CTEs/subqueries) as we can't be certain
            base_tables = [t for t in table_columns.keys() 
                          if not t.startswith('__cte__.') and not t.startswith('__subquery__.')]
            
            if len(base_tables) == 1:
                # Only one table, so it must come from there
                table_columns[base_tables[0]].add(column_name)
            else:
                # Multiple tables - we can't determine which one without schema info
                # Add to all tables (conservative approach)
                for table_name in base_tables:
                    table_columns[table_name].add(column_name)
        else:
            # Column appears elsewhere (WHERE, JOIN, etc.) - apply same logic
            base_tables = [t for t in table_columns.keys() 
                          if not t.startswith('__cte__.') and not t.startswith('__subquery__.')]
            for table_name in base_tables:
                table_columns[table_name].add(column_name)
    
    def _is_in_select_clause(self, column_node: exp.Column) -> bool:
        """Check if a column is in the SELECT clause."""
        parent = column_node.parent
        while parent:
            if isinstance(parent, exp.Select):
                # Check if this column is in the expressions (SELECT list)
                if column_node in parent.expressions or any(
                    column_node in expr.find_all(exp.Column) 
                    for expr in parent.expressions
                ):
                    return True
                return False
            parent = parent.parent
        return False
    
    def _get_full_table_name(self, table_node: exp.Table) -> str:
        """Constructs a fully qualified table name WITHOUT alias."""
        # Use sqlglot's built-in properties to get table name components
        catalog = table_node.catalog if hasattr(table_node, 'catalog') and table_node.catalog else None
        db = table_node.db if hasattr(table_node, 'db') and table_node.db else None
        table_name = table_node.this.name if table_node.this else None
        
        # Build the fully qualified name
        parts = []
        if catalog:
            parts.append(catalog)
        if db:
            parts.append(db)
        if table_name:
            parts.append(table_name)
        
        # Apply defaults if needed
        if len(parts) == 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        elif len(parts) == 2:
            if self.default_project:
                return f"{self.default_project}.{parts[0]}.{parts[1]}"
            return f"{parts[0]}.{parts[1]}"
        elif len(parts) == 1:
            if self.default_project and self.default_dataset:
                return f"{self.default_project}.{self.default_dataset}.{parts[0]}"
            elif self.default_dataset:
                return f"{self.default_dataset}.{parts[0]}"
            return parts[0]
        
        # Fallback - should not reach here
        return table_name or "unknown_table"


def extract_table_columns_from_query(sql_query: str, 
                                     default_project: str = None, 
                                     default_dataset: str = None) -> Dict[str, List[str]]:
    """Main function to extract tables and their columns from a BigQuery SQL query."""
    extractor = BigQueryTableColumnExtractor(default_project, default_dataset)
    return extractor.extract_tables_and_columns(sql_query)


# EXAMPLE USAGE
if __name__ == "__main__":
    # Example 1: Simple subquery
    query1 = """
    SELECT a.customer_id, b.total_amount
    FROM customers a
    JOIN (
        SELECT customer_id, SUM(amount) as total_amount
        FROM orders
        WHERE status = 'completed'
        GROUP BY customer_id
    ) b ON a.customer_id = b.customer_id
    """
    
    print("Example 1: Simple subquery")
    print("Query:", query1)
    result1 = extract_table_columns_from_query(query1)
    print("Result:", result1)
    print("\n" + "="*80 + "\n")
    
    # Example 2: Nested subqueries
    query2 = """
    SELECT 
        main.product_id,
        main.product_name,
        sales.total_sales
    FROM products main
    JOIN (
        SELECT 
            o.product_id,
            SUM(o.quantity) as total_sales
        FROM (
            SELECT product_id, quantity, customer_id
            FROM order_items
            WHERE created_date >= '2024-01-01'
        ) o
        JOIN customers c ON o.customer_id = c.id
        WHERE c.country = 'USA'
        GROUP BY o.product_id
    ) sales ON main.product_id = sales.product_id
    """
    
    print("Example 2: Nested subqueries")
    print("Query:", query2)
    result2 = extract_table_columns_from_query(query2)
    print("Result:", result2)
    print("\n" + "="*80 + "\n")
    
    # Example 3: CTE with subquery
    query3 = """
    WITH active_customers AS (
        SELECT customer_id, name, email
        FROM customers
        WHERE status = 'active'
    )
    SELECT 
        ac.name,
        recent_orders.order_count
    FROM active_customers ac
    JOIN (
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        WHERE order_date >= '2024-01-01'
        GROUP BY customer_id
    ) recent_orders ON ac.customer_id = recent_orders.customer_id
    """
    
    print("Example 3: CTE with subquery")
    print("Query:", query3)
    result3 = extract_table_columns_from_query(query3)
    print("Result:", result3)
    print("\n" + "="*80 + "\n")
