import json
import psycopg2
from typing import Dict, List, Optional
from datetime import datetime

class PostgresMetadataReader:
    """
    PostgreSQL-backed metadata reader that implements the same interface as StaticMetadataReader.
    Reads table metadata from public.bq_metadata table instead of static.json file.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize with database configuration.
        
        Args:
            db_config: Dictionary with keys: host, port, user, password, database
        """
        self.db_config = db_config
        self._validate_connection()
        
    def _validate_connection(self):
        """Test database connection and show metadata summary"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Get count and latest extraction time
            cur.execute("""
                SELECT COUNT(*), MAX(extraction_time) 
                FROM public.bq_metadata
            """)
            count, latest_extraction = cur.fetchone()
            
            if count == 0:
                raise ValueError("❌ No metadata found in bq_metadata table. Run upload_data.py first.")
            
            # Calculate age
            if latest_extraction:
                age_days = (datetime.now(latest_extraction.tzinfo) - latest_extraction).days
                print(f"📚 Connected to PostgreSQL metadata store")
                print(f" - Latest extraction: {latest_extraction.strftime('%Y-%m-%d %H:%M')}")
                print(f" - Age: {age_days} days")
                print(f" - Tables: {count}")
            
            cur.close()
            conn.close()
            
        except psycopg2.Error as e:
            raise ConnectionError(f"❌ Database connection failed: {e}")
        except Exception as e:
            raise RuntimeError(f"❌ Metadata validation failed: {e}")

    def get_metadata_for_tables(self, 
                               table_names: List[str], 
                               table_columns: Dict[str, List[str]] = None) -> Dict:
        """
        Get metadata for specified tables and optionally filter to specific columns.
        This method signature matches StaticMetadataReader exactly.
        
        Args:
            table_names: List of table names to retrieve metadata for
            table_columns: Optional dict of table_name -> [column_names] for filtering
            
        Returns:
            Dict mapping table names to their metadata (same format as StaticMetadataReader)
        """
        result = {}
        
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            for table_name in table_names:
                # Resolve table name to full qualified name
                full_table_name = self._resolve_table_name(cur, table_name)
                
                if not full_table_name:
                    print(f"⚠️ Table not found in bq_metadata: {table_name}")
                    result[table_name] = {
                        'table_name': table_name,
                        'schema': [],
                        'error': 'Table not found in PostgreSQL metadata'
                    }
                    continue
                
                # Retrieve metadata from database
                cur.execute("""
                    SELECT metadata FROM public.bq_metadata 
                    WHERE table_name = %s
                """, (full_table_name,))
                
                row = cur.fetchone()
                if not row:
                    result[table_name] = {
                        'table_name': table_name,
                        'schema': [],
                        'error': 'Metadata not found'
                    }
                    continue
                
                # Parse metadata JSON
                table_meta = row[0]  # JSONB is automatically parsed by psycopg2
                
                # Apply column filtering if requested (reuse StaticMetadataReader logic)
                if table_columns and table_name in table_columns:
                    requested_cols = table_columns[table_name]
                    table_meta = self._filter_table_metadata(table_meta, requested_cols)
                
                result[table_name] = table_meta
            
            cur.close()
            
        except psycopg2.Error as e:
            print(f"❌ Database error retrieving metadata: {e}")
            # Return error entries for all requested tables
            for table_name in table_names:
                if table_name not in result:
                    result[table_name] = {
                        'table_name': table_name,
                        'schema': [],
                        'error': f'Database error: {str(e)}'
                    }
        finally:
            if conn:
                conn.close()
                
        return result

    def _resolve_table_name(self, cur, table_name: str) -> Optional[str]:
        """
        Resolve partial table names to full names using database lookup.
        Handles cases like 'table_name' vs 'project.dataset.table_name'
        """
        # First try exact match
        cur.execute("""
            SELECT table_name FROM public.bq_metadata 
            WHERE table_name = %s
        """, (table_name,))
        
        if cur.fetchone():
            return table_name
        
        # Try suffix match (table_id lookup)  
        cur.execute("""
            SELECT table_name FROM public.bq_metadata 
            WHERE table_name LIKE %s OR table_name LIKE %s
        """, (f'%.{table_name}', f'%.%.{table_name}'))
        
        row = cur.fetchone()
        if row:
            return row[0]
        
        return None

    def _filter_table_metadata(self, table_meta: Dict, requested_cols: List[str]) -> Dict:
        """
        Filter metadata to only include requested columns and their parent structures.
        Reuses the exact same logic as StaticMetadataReader.
        """
        filtered_meta = table_meta.copy()
        
        # Filter schema to requested columns
        filtered_schema = []
        for field in table_meta.get('schema', []):
            filtered_field = self._filter_schema_field(field, requested_cols)
            if filtered_field:
                filtered_schema.append(filtered_field)
        
        filtered_meta['schema'] = filtered_schema
        filtered_meta['requested_columns'] = requested_cols
        
        # Update column paths to only include requested ones
        filtered_meta['column_paths'] = [
            path for path in table_meta.get('column_paths', [])
            if any(path == col or path.startswith(col + '.') or col.startswith(path + '.')
                   for col in requested_cols)
        ]
        
        return filtered_meta

    def _filter_schema_field(self, field: Dict, requested_cols: List[str]) -> Optional[Dict]:
        """
        Recursively filter schema field to include only if it or its children are requested.
        Identical logic to StaticMetadataReader._filter_schema_field.
        """
        field_path = field['column_name']
        
        # Check if this field or any child is needed
        is_needed = False
        for col in requested_cols:
            if col == field_path or col.startswith(field_path + '.') or field_path.startswith(col + '.'):
                is_needed = True
                break
        
        if not is_needed:
            return None
        
        # Copy field
        filtered_field = field.copy()
        
        # If has nested fields, filter them recursively
        if 'nested_fields' in field:
            filtered_nested = []
            for nested in field['nested_fields']:
                filtered_nested_field = self._filter_schema_field(nested, requested_cols)
                if filtered_nested_field:
                    filtered_nested.append(filtered_nested_field)
            
            if filtered_nested:
                filtered_field['nested_fields'] = filtered_nested
            else:
                # Remove nested_fields key if empty after filtering
                filtered_field.pop('nested_fields', None)
        
        return filtered_field

    def get_all_tables_metadata(self) -> Dict:
        """Get metadata for all tables"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            cur.execute("SELECT table_name, metadata FROM public.bq_metadata")
            rows = cur.fetchall()
            
            result = {}
            for table_name, metadata in rows:
                result[table_name] = metadata
            
            cur.close()
            return result
            
        except psycopg2.Error as e:
            print(f"❌ Database error: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def get_statistics(self) -> Dict:
        """Get extraction statistics from database"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total_tables,
                    MAX(extraction_time) as latest_extraction,
                    SUM((metadata->>'column_count')::int) as total_columns,
                    SUM((metadata->>'size_mb')::float) as total_size_mb
                FROM public.bq_metadata
                WHERE metadata->>'column_count' IS NOT NULL
            """)
            
            row = cur.fetchone()
            if row:
                total_tables, latest_extraction, total_columns, total_size_mb = row
                return {
                    'total_tables': total_tables or 0,
                    'total_columns': total_columns or 0,
                    'total_size_mb': total_size_mb or 0.0,
                    'latest_extraction': latest_extraction.isoformat() if latest_extraction else None
                }
            
            cur.close()
            
        except psycopg2.Error as e:
            print(f"❌ Database error: {e}")
        finally:
            if conn:
                conn.close()
        
        return {}
