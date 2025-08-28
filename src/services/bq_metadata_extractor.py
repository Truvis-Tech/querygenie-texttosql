import logging
from typing import List, Dict, Any
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

logger = logging.getLogger(__name__)

class BigQueryMetadataExtractor:
    """
    Extracts detailed metadata from tables in a BigQuery dataset.
    """
    
    def __init__(self, client: bigquery.Client):
        """
        Initialize the metadata extractor.
        
        Args:
            client: A configured BigQuery client instance.
        """
        self.client = client
        self.project_id = client.project

    def _get_table_list(self, dataset_id: str) -> List[bigquery.table.TableListItem]:
        """Get list of all tables in the dataset."""
        dataset_ref = f"{self.project_id}.{dataset_id}"
        try:
            return list(self.client.list_tables(dataset_ref))
        except GoogleAPICallError as e:
            logger.error(f"Failed to list tables for {dataset_ref}: {e}")
            raise

    def _get_table_metadata(self, table_ref: bigquery.TableReference, columns: List[str] = None) -> Dict[str, Any]:
        """Get schema and metadata for a specific table.
        
        Args:
            table_ref: Reference to the BigQuery table
            columns: Optional list of column names to include in the schema. If None, all columns are included.
        """
        try:
            table_obj = self.client.get_table(table_ref)
            
            schema = []
            for field in table_obj.schema:
                if columns is None or field.name in columns:
                    schema.append({
                        'column_name': field.name,
                        'data_type': field.field_type,
                        'mode': field.mode,
                        'description': field.description or "",
                    })
            
            return {
                'table_name': str(table_ref),
                'table_description': table_obj.description or "No description available",
                'schema': schema,
                'num_rows': table_obj.num_rows,
                'size_bytes': table_obj.num_bytes,
                'last_modified': str(table_obj.modified),
                'created': str(table_obj.created),
                'partitioning_info': {
                    'type': table_obj.partitioning_type,
                    'field': table_obj.time_partitioning.field if table_obj.time_partitioning else None,
                },
                'clustering_fields': table_obj.clustering_fields or [],
            }
        except GoogleAPICallError as e:
            logger.error(f"Failed to get metadata for table {table_ref}: {e}")
            raise

    def get_metadata_for_tables(self, table_names: List[str], table_columns: Dict[str, List[str]] = None) -> List[Dict[str, Any]]:
        """
        Extracts metadata for a specific list of fully qualified table names.

        Args:
            table_names: A list of fully qualified table IDs (e.g., ['project.dataset.table1']).
            table_columns: Optional dictionary mapping table names to lists of column names to include.
                         If provided, only the specified columns will be included in the schema.
                         Example: {'project.dataset.table1': ['col1', 'col2']}

        Returns:
            A list of dictionaries, each containing the metadata for a table.
        """
        all_metadata = []
        for fq_table_name in table_names:
            try:
                table_ref = bigquery.TableReference.from_string(fq_table_name)
                columns = table_columns.get(fq_table_name) if table_columns else None
                metadata = self._get_table_metadata(table_ref, columns)
                all_metadata.append(metadata)
            except ValueError as e:
                logger.warning(f"Invalid table name format '{fq_table_name}': {e}")
            except Exception as e:
                logger.error(f"Could not extract metadata for '{fq_table_name}': {e}")
        
        return all_metadata
        
    def get_all_tables_metadata(self, dataset_id: str) -> Dict[str, Any]:
        """
        Extracts metadata for all tables in the specified dataset.
        
        Args:
            dataset_id: The dataset ID from which to extract metadata.
            
        Returns:
            A dictionary containing:
            - 'dataset': Dataset information
            - 'tables': List of table metadata dictionaries
            - 'summary': Summary statistics about the dataset
        """
        try:
            # Get dataset information
            dataset_ref = self.client.get_dataset(f"{self.project_id}.{dataset_id}")
            dataset_info = {
                'dataset_id': dataset_ref.dataset_id,
                'project_id': dataset_ref.project,
                'description': dataset_ref.description or "No description available",
                'created': str(dataset_ref.created),
                'modified': str(dataset_ref.modified),
                'default_table_expiration_ms': dataset_ref.default_table_expiration_ms,
                'location': dataset_ref.location,
                'labels': dict(dataset_ref.labels) if dataset_ref.labels else {}
            }
            
            # Get all tables in the dataset
            tables = self._get_table_list(dataset_id)
            tables_metadata = []
            total_rows = 0
            total_size_bytes = 0
            
            for table_item in tables:
                try:
                    table_ref = table_item.reference
                    table_metadata = self._get_table_metadata(table_ref)
                    tables_metadata.append(table_metadata)
                    
                    # Update summary statistics
                    if table_metadata['num_rows']:
                        total_rows += table_metadata['num_rows']
                    if table_metadata['size_bytes']:
                        total_size_bytes += table_metadata['size_bytes']
                        
                except Exception as e:
                    logger.error(f"Error processing table {table_item.table_id}: {e}")
            
            # Prepare summary
            summary = {
                'total_tables': len(tables_metadata),
                'total_columns': sum(len(table['schema']) for table in tables_metadata),
                'total_rows': total_rows,
                'total_size_gb': round(total_size_bytes / (1024 ** 3), 2)  # Convert to GB
            }
            
            return {
                'dataset': dataset_info,
                'tables': tables_metadata,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"Failed to extract metadata for dataset {dataset_id}: {e}")
            raise
