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

    def _get_table_metadata(self, table_ref: bigquery.TableReference) -> Dict[str, Any]:
        """Get schema and metadata for a specific table."""
        try:
            table_obj = self.client.get_table(table_ref)
            
            schema = []
            for field in table_obj.schema:
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

    def get_metadata_for_tables(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """
        Extracts metadata for a specific list of fully qualified table names.

        Args:
            table_names: A list of fully qualified table IDs (e.g., ['project.dataset.table1']).

        Returns:
            A list of dictionaries, each containing the metadata for a table.
        """
        all_metadata = []
        for fq_table_name in table_names:
            try:
                table_ref = bigquery.TableReference.from_string(fq_table_name)
                metadata = self._get_table_metadata(table_ref)
                all_metadata.append(metadata)
            except ValueError as e:
                logger.warning(f"Invalid table name format '{fq_table_name}': {e}")
            except Exception as e:
                logger.error(f"Could not extract metadata for '{fq_table_name}': {e}")
        
        return all_metadata
