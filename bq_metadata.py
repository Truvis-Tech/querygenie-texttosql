"""
BigQuery Metadata Extraction Service
Handles extraction of metadata from BigQuery datasets and tables
"""
from google.cloud import bigquery
from datetime import datetime
from typing import Dict, List, Any
from src.utils.logger import logger


def extract_nested_schema(field, parent_path="", level=0):
    """Recursively extract schema including nested RECORD fields"""
    field_path = f"{parent_path}.{field.name}" if parent_path else field.name
    
    field_info = {
        'column_name': field_path,
        'field_name': field.name,
        'data_type': field.field_type,
        'mode': field.mode,
        'description': field.description or "",
        'level': level
    }
    
    if field.field_type == 'RECORD' and field.fields:
        field_info['nested_fields'] = []
        for nested_field in field.fields:
            nested_info = extract_nested_schema(nested_field, field_path, level + 1)
            field_info['nested_fields'].append(nested_info)
    
    if field.mode == 'REPEATED':
        field_info['is_array'] = True
    
    return field_info


def get_all_column_paths(schema_field):
    """Extract all queryable dot-notation paths"""
    paths = [schema_field['column_name']]
    
    if 'nested_fields' in schema_field:
        for nested in schema_field['nested_fields']:
            paths.extend(get_all_column_paths(nested))
    
    return paths


def extract_partitioning_info(table):
    """Extract partitioning information"""
    if not table.partitioning_type:
        return None
    
    partitioning_info = {
        'type': table.partitioning_type,
        'is_partitioned': True,
    }
    
    if hasattr(table, 'time_partitioning') and table.time_partitioning:
        partitioning_info.update({
            'field': table.time_partitioning.field,
            'granularity': table.time_partitioning.type_,
            'expiration_ms': table.time_partitioning.expiration_ms,
            'require_partition_filter': table.time_partitioning.require_partition_filter,
        })
    
    if hasattr(table, 'range_partitioning') and table.range_partitioning:
        partitioning_info.update({
            'field': table.range_partitioning.field,
            'range_start': table.range_partitioning.range_.start,
            'range_end': table.range_partitioning.range_.end,
            'range_interval': table.range_partitioning.range_.interval,
        })
    
    return partitioning_info


def extract_clustering_info(table):
    """Extract clustering information"""
    if not table.clustering_fields:
        return None
    
    return {
        'fields': list(table.clustering_fields),
        'field_count': len(table.clustering_fields),
        'is_optimized_for_filtering': True,
        'is_optimized_for_aggregation': True,
    }


def categorize_table_size(table):
    """Categorize table size"""
    if not table.num_rows:
        return 'unknown'
    elif table.num_rows < 1000:
        return 'tiny'
    elif table.num_rows < 100000:
        return 'small' 
    elif table.num_rows < 10000000:
        return 'medium'
    elif table.num_rows < 1000000000:
        return 'large'
    else:
        return 'very_large'


def extract_complete_table_metadata(client, table_ref):
    """Extract comprehensive metadata for a single table"""
    table = client.get_table(table_ref)
    
    schema_fields = []
    all_column_paths = []
    
    for field in table.schema:
        field_info = extract_nested_schema(field)
        schema_fields.append(field_info)
        all_column_paths.extend(get_all_column_paths(field_info))
    
    metadata = {
        'table_name': table_ref,
        'table_id': table.table_id,
        'dataset_id': table.dataset_id,
        'project_id': table.project,
        'schema': schema_fields,
        'column_paths': sorted(list(set(all_column_paths))),
        'column_count': len(all_column_paths),
        'top_level_columns': len(table.schema),
        'num_rows': table.num_rows,
        'num_bytes': table.num_bytes,
        'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0,
        'created': table.created.isoformat() if table.created else None,
        'modified': table.modified.isoformat() if table.modified else None,
        'description': table.description or "",
        'performance_characteristics': {
            'partitioning': extract_partitioning_info(table),
            'clustering': extract_clustering_info(table),
            'size_category': categorize_table_size(table),
        }
    }
    
    return metadata


class BigQueryMetadata:
    """Service for extracting BigQuery metadata"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    def extract_all_metadata(self) -> List[Dict[str, Any]]:
        """Extract metadata from all datasets in the project"""
        logger.info("Extracting metadata for project: %s", self.project_id)
        
        try:
            datasets = list(self.client.list_datasets(self.project_id))
            logger.info("Found %d datasets", len(datasets))
        except Exception as e:
            logger.error("Error listing datasets: %s", str(e))
            raise
        
        all_datasets_metadata = []
        overall_start_time = datetime.now()
        
        for dataset_idx, dataset_item in enumerate(datasets, 1):
            dataset_metadata = self._extract_dataset_metadata(dataset_item, dataset_idx, len(datasets))
            all_datasets_metadata.append(dataset_metadata)
        
        overall_duration = (datetime.now() - overall_start_time).total_seconds()
        logger.info("Metadata extraction complete for %d datasets in %.2fs", 
                   len(all_datasets_metadata), overall_duration)
        
        return all_datasets_metadata
    
    def _extract_dataset_metadata(self, dataset_item, dataset_idx: int, total_datasets: int) -> Dict[str, Any]:
        """Extract metadata for a single dataset"""
        current_dataset_id = dataset_item.dataset_id
        
        logger.info("Extracting dataset [%d/%d]: %s.%s", 
                   dataset_idx, total_datasets, self.project_id, current_dataset_id)
        
        dataset = self.client.get_dataset(dataset_item.reference)
        
        metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'project_id': self.project_id,
            'dataset_id': current_dataset_id,
            'location': dataset.location,
            'tables': {},
            'statistics': {
                'total_tables': 0,
                'total_columns': 0,
                'total_size_mb': 0,
                'tables_with_nested_fields': 0,
                'partitioned_tables': 0,
                'clustered_tables': 0,
                'extraction_duration_seconds': 0
            }
        }
        
        start_time = datetime.now()
        
        try:
            tables = list(self.client.list_tables(current_dataset_id))
            metadata['statistics']['total_tables'] = len(tables)
            
            logger.info("Found %d tables in dataset %s", len(tables), current_dataset_id)
            
            for i, table_item in enumerate(tables, 1):
                table_ref = f"{self.project_id}.{current_dataset_id}.{table_item.table_id}"
                
                try:
                    table_metadata = extract_complete_table_metadata(self.client, table_ref)
                    
                    # Update statistics
                    has_nested = any('nested_fields' in field for field in table_metadata['schema'])
                    if has_nested:
                        metadata['statistics']['tables_with_nested_fields'] += 1
                    
                    perf_chars = table_metadata.get('performance_characteristics', {})
                    if perf_chars.get('partitioning'):
                        metadata['statistics']['partitioned_tables'] += 1
                    if perf_chars.get('clustering'):
                        metadata['statistics']['clustered_tables'] += 1
                    
                    metadata['tables'][table_ref] = table_metadata
                    metadata['statistics']['total_columns'] += table_metadata['column_count']
                    metadata['statistics']['total_size_mb'] += table_metadata['size_mb']
                    
                    if i % 10 == 0:
                        logger.info("Processed %d/%d tables in dataset %s", i, len(tables), current_dataset_id)
                    
                except Exception as e:
                    logger.error("Error processing table %s: %s", table_item.table_id, str(e))
                    metadata['tables'][table_ref] = {
                        'table_name': table_ref,
                        'error': str(e)
                    }
            
        except Exception as e:
            logger.error("Error processing dataset %s: %s", current_dataset_id, str(e))
        
        duration = (datetime.now() - start_time).total_seconds()
        metadata['statistics']['extraction_duration_seconds'] = round(duration, 2)
        
        logger.info("Dataset complete - Tables: %d, Columns: %d, Duration: %.2fs",
                   metadata['statistics']['total_tables'],
                   metadata['statistics']['total_columns'],
                   duration)
        
        return metadata
