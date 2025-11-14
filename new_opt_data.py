import json
from typing import Dict, List, Any
from google.cloud import bigquery
from datetime import datetime
from src.services.bq_client import get_bigquery_client

def extract_nested_schema(field, parent_path="", level=0):
    """
    Recursively extract schema including all nested RECORD fields.
    Returns both hierarchical structure and flattened paths.
    """
    field_path = f"{parent_path}.{field.name}" if parent_path else field.name
    
    field_info = {
        'column_name': field_path,
        'field_name': field.name,  # Keep original name too
        'data_type': field.field_type,
        'mode': field.mode,
        'description': field.description or "",
        'level': level  # Track nesting depth
    }
    
    # For RECORD/STRUCT types, recursively process nested fields
    if field.field_type == 'RECORD' and field.fields:
        field_info['nested_fields'] = []
        for nested_field in field.fields:
            nested_info = extract_nested_schema(nested_field, field_path, level + 1)
            field_info['nested_fields'].append(nested_info)
    
    # For REPEATED fields, mark as array
    if field.mode == 'REPEATED':
        field_info['is_array'] = True
    
    return field_info

def get_all_column_paths(schema_field):
    """Extract all queryable dot-notation paths from a schema field"""
    paths = [schema_field['column_name']]
    
    if 'nested_fields' in schema_field:
        for nested in schema_field['nested_fields']:
            paths.extend(get_all_column_paths(nested))
    
    return paths

def extract_partitioning_info(table):
    """Extract comprehensive partitioning information"""
    if not table.partitioning_type:
        return None
    
    partitioning_info = {
        'type': table.partitioning_type,  # TIME, RANGE, INTEGER
        'is_partitioned': True,
    }
    
    # Time partitioning details
    if hasattr(table, 'time_partitioning') and table.time_partitioning:
        partitioning_info.update({
            'field': table.time_partitioning.field,
            'granularity': table.time_partitioning.type_,  # DAY, HOUR, MONTH, YEAR
            'expiration_ms': table.time_partitioning.expiration_ms,
            'require_partition_filter': table.time_partitioning.require_partition_filter,
        })
    
    # Range partitioning details  
    if hasattr(table, 'range_partitioning') and table.range_partitioning:
        partitioning_info.update({
            'field': table.range_partitioning.field,
            'range_start': table.range_partitioning.range_.start,
            'range_end': table.range_partitioning.range_.end,
            'range_interval': table.range_partitioning.range_.interval,
        })
    
    return partitioning_info

def extract_clustering_info(table):
    """Extract clustering information with performance context"""
    if not table.clustering_fields:
        return None
    
    return {
        'fields': list(table.clustering_fields),
        'field_count': len(table.clustering_fields),
        'is_optimized_for_filtering': True,  # Clustering helps with WHERE clauses
        'is_optimized_for_aggregation': True,  # Clustering helps with GROUP BY
    }

def extract_optimization_settings(table):
    """Extract table optimization settings"""
    return {
        'table_expiration': table.expires.isoformat() if table.expires else None,
        'default_table_expiration': getattr(table, 'default_table_expiration_ms', None),
        'labels': dict(table.labels) if table.labels else {},
        'table_type': table.table_type,  # TABLE, VIEW, EXTERNAL, MATERIALIZED_VIEW
        'require_partition_filter': getattr(table, 'require_partition_filter', False),
        'max_staleness': getattr(table, 'max_staleness', None),  # For materialized views
    }

def extract_access_patterns(table):
    """Extract information relevant to query performance patterns"""
    
    # Analyze schema for common access patterns
    has_timestamp_fields = any(
        field.field_type in ['TIMESTAMP', 'DATETIME', 'DATE'] 
        for field in table.schema
    )
    
    has_id_fields = any(
        'id' in field.name.lower() 
        for field in table.schema
    )
    
    return {
        'has_timestamp_fields': has_timestamp_fields,
        'has_id_fields': has_id_fields,
        'wide_table': len(table.schema) > 50,  # Many columns = wide table
        'large_table': table.num_rows > 1000000 if table.num_rows else False,
        'nested_structure': any(field.field_type == 'RECORD' for field in table.schema),
        'repeated_fields': any(field.mode == 'REPEATED' for field in table.schema),
    }

def extract_materialized_view_info(table):
    """Extract materialized view specific information"""
    if table.table_type != 'MATERIALIZED_VIEW':
        return None
    
    mv_info = {
        'is_materialized_view': True,
        'view_query': table.view_query if hasattr(table, 'view_query') else None,
        'last_refresh_time': table.modified.isoformat() if table.modified else None,
        'refresh_interval_minutes': getattr(table, 'refresh_interval_minutes', None),
    }
    
    # Add base tables information if available
    if hasattr(table, 'mview') and table.mview:
        mv_info.update({
            'enable_refresh': table.mview.enable_refresh,
            'refresh_interval': table.mview.refresh_interval_minutes,
            'last_refresh': table.mview.last_refresh_time.isoformat() if table.mview.last_refresh_time else None,
        })
    
    return mv_info

def extract_external_table_info(table):
    """Extract external table configuration"""
    if not hasattr(table, 'external_data_configuration') or not table.external_data_configuration:
        return None
    
    external_config = table.external_data_configuration
    
    return {
        'is_external_table': True,
        'source_format': external_config.source_format,
        'source_uris': list(external_config.source_uris) if external_config.source_uris else [],
        'compression': external_config.compression,
        'max_bad_records': external_config.max_bad_records,
        'autodetect': external_config.autodetect,
        'ignore_unknown_values': external_config.ignore_unknown_values,
    }

def categorize_table_size(table):
    """Categorize table size for optimization purposes"""
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

def categorize_table_complexity(table):
    """Categorize schema complexity"""
    has_nested = any(field.field_type == 'RECORD' for field in table.schema)
    has_repeated = any(field.mode == 'REPEATED' for field in table.schema)
    column_count = len(table.schema)
    
    if has_nested and has_repeated and column_count > 20:
        return 'very_complex'
    elif has_nested or has_repeated or column_count > 50:
        return 'complex'
    elif column_count > 20:
        return 'moderate'
    else:
        return 'simple'

def generate_performance_recommendations(table):
    """Generate LLM-friendly performance recommendations"""
    recommendations = []
    
    # Partitioning recommendations
    if not table.partitioning_type:
        has_timestamp = any(
            field.field_type in ['TIMESTAMP', 'DATETIME', 'DATE'] 
            for field in table.schema
        )
        if has_timestamp and table.num_rows and table.num_rows > 100000:
            recommendations.append({
                'type': 'partitioning',
                'priority': 'high',
                'suggestion': 'Consider time-based partitioning for large tables with timestamp fields',
                'benefit': 'Reduces query cost and improves performance'
            })
    
    # Clustering recommendations
    if not table.clustering_fields and table.partitioning_type:
        recommendations.append({
            'type': 'clustering',
            'priority': 'medium', 
            'suggestion': 'Consider clustering on frequently filtered columns',
            'benefit': 'Improves query performance for WHERE and JOIN operations'
        })
    
    # Large table recommendations
    if table.num_rows and table.num_rows > 10000000:  # 10M+ rows
        recommendations.append({
            'type': 'query_optimization',
            'priority': 'high',
            'suggestion': 'Use partition pruning and column selection for large tables',
            'benefit': 'Significantly reduces query costs and execution time'
        })
    
    # Wide table recommendations
    if len(table.schema) > 50:
        recommendations.append({
            'type': 'query_optimization',
            'priority': 'medium',
            'suggestion': 'Use SELECT specific columns instead of SELECT * for wide tables',
            'benefit': 'Reduces data transfer and query cost'
        })
    
    # Nested structure recommendations
    has_nested = any(field.field_type == 'RECORD' for field in table.schema)
    if has_nested:
        recommendations.append({
            'type': 'query_optimization',
            'priority': 'medium',
            'suggestion': 'Use specific field paths when querying nested structures',
            'benefit': 'Avoids unnecessary data processing and improves performance'
        })
    
    return recommendations

def extract_query_optimization_context(table):
    """Extract context useful for LLM query optimization"""
    return {
        'estimated_scan_cost_per_tb': 5.0,  # BigQuery pricing
        'partition_pruning_available': bool(table.partitioning_type),
        'clustering_available': bool(table.clustering_fields),
        'recommended_where_fields': list(table.clustering_fields) if table.clustering_fields else [],
        'size_category': categorize_table_size(table),
        'complexity_category': categorize_table_complexity(table),
        'partition_field': (
            table.time_partitioning.field if hasattr(table, 'time_partitioning') and table.time_partitioning 
            else table.range_partitioning.field if hasattr(table, 'range_partitioning') and table.range_partitioning 
            else None
        )
    }

def extract_complete_table_metadata(client, table_ref):
    """Extract comprehensive metadata including ALL performance parameters for LLM optimization"""
    table = client.get_table(table_ref)
    
    # Process schema with nested field support
    schema_fields = []
    all_column_paths = []  # All queryable paths (e.g., id.payload.schema.lifecycle_id)
    
    for field in table.schema:
        field_info = extract_nested_schema(field)
        schema_fields.append(field_info)
        all_column_paths.extend(get_all_column_paths(field_info))
    
    # Build complete metadata with enhanced performance parameters
    metadata = {
        'table_name': table_ref,
        'table_id': table.table_id,
        'dataset_id': table.dataset_id,
        'project_id': table.project,
        'schema': schema_fields,
        'column_paths': sorted(list(set(all_column_paths))),  # All unique queryable paths
        'column_count': len(all_column_paths),
        'top_level_columns': len(table.schema),
        'num_rows': table.num_rows,
        'num_bytes': table.num_bytes,
        'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0,
        'created': table.created.isoformat() if table.created else None,
        'modified': table.modified.isoformat() if table.modified else None,
        'description': table.description or "",
        
        # ENHANCED PERFORMANCE PARAMETERS FOR LLM
        'performance_characteristics': {
            # 1. PARTITIONING (Enhanced)
            'partitioning': extract_partitioning_info(table),
            
            # 2. CLUSTERING 
            'clustering': extract_clustering_info(table),
            
            # 3. TABLE OPTIMIZATION SETTINGS
            'optimization_settings': extract_optimization_settings(table),
            
            # 4. ACCESS PATTERNS & CONSTRAINTS
            'access_patterns': extract_access_patterns(table),
            
            # 5. MATERIALIZED VIEW INFO
            'materialized_view': extract_materialized_view_info(table),
            
            # 6. EXTERNAL TABLE INFO
            'external_table': extract_external_table_info(table),
            
            # 7. PERFORMANCE RECOMMENDATIONS
            'recommendations': generate_performance_recommendations(table),
            
            # 8. QUERY OPTIMIZATION CONTEXT
            'query_optimization_context': extract_query_optimization_context(table),
        }
    }
    
    return metadata

def main():
    """Generate comprehensive static.json with all metadata including performance parameters for ALL datasets"""
    print("🚀 Starting enhanced metadata extraction with performance parameters for ALL datasets...")
    
    # Initialize BigQuery client
    market = 'linen-striker-454116-c9'  # Your market identifier
    client, project_id, dataset_id, location = get_bigquery_client(market)
    
    print(f"🔍 Discovering all datasets in project: {project_id}")
    
    # Get all datasets in the project
    try:
        datasets = list(client.list_datasets(project_id))
        print(f"📦 Found {len(datasets)} datasets in project")
    except Exception as e:
        print(f"✗ Error listing datasets: {e}")
        print(f"Falling back to single dataset: {dataset_id}")
        datasets = [type('obj', (object,), {'dataset_id': dataset_id})]
    
    # List to store metadata for all datasets
    all_datasets_metadata = []
    
    overall_start_time = datetime.now()
    
    # Process each dataset
    for dataset_idx, dataset_item in enumerate(datasets, 1):
        current_dataset_id = dataset_item.dataset_id
        
        print(f"\n{'='*80}")
        print(f"📊 Extracting from dataset [{dataset_idx}/{len(datasets)}]: {project_id}.{current_dataset_id}")
        print(f"{'='*80}")
        
        # Initialize metadata structure for this dataset (KEEPING ORIGINAL STRUCTURE)
        metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'project_id': project_id,
            'dataset_id': current_dataset_id,
            'location': location,
            'tables': {},
            'statistics': {
                'total_tables': 0,
                'total_columns': 0,
                'total_size_mb': 0,
                'tables_with_nested_fields': 0,
                'partitioned_tables': 0,
                'clustered_tables': 0,
                'materialized_views': 0,
                'external_tables': 0,
                'extraction_duration_seconds': 0
            }
        }
        
        start_time = datetime.now()
        
        try:
            # Get all tables in dataset
            tables = list(client.list_tables(current_dataset_id))
            metadata['statistics']['total_tables'] = len(tables)
            
            print(f"📋 Found {len(tables)} tables to process")
            
            # Process each table
            for i, table_item in enumerate(tables, 1):
                table_ref = f"{project_id}.{current_dataset_id}.{table_item.table_id}"
                print(f"  [{i}/{len(tables)}] Processing: {table_item.table_id}")
                
                try:
                    table_metadata = extract_complete_table_metadata(client, table_ref)
                    
                    # Check if table has nested fields
                    has_nested = any(
                        'nested_fields' in field 
                        for field in table_metadata['schema']
                    )
                    
                    if has_nested:
                        metadata['statistics']['tables_with_nested_fields'] += 1
                        print(f"    ✓ Found nested fields - {table_metadata['column_count']} total paths")
                    
                    # Update performance statistics
                    perf_chars = table_metadata.get('performance_characteristics', {})
                    
                    if perf_chars.get('partitioning'):
                        metadata['statistics']['partitioned_tables'] += 1
                        print(f"    ✓ Partitioned table: {perf_chars['partitioning']['type']}")
                    
                    if perf_chars.get('clustering'):
                        metadata['statistics']['clustered_tables'] += 1
                        print(f"    ✓ Clustered on: {', '.join(perf_chars['clustering']['fields'])}")
                    
                    if perf_chars.get('materialized_view'):
                        metadata['statistics']['materialized_views'] += 1
                        print(f"    ✓ Materialized view")
                    
                    if perf_chars.get('external_table'):
                        metadata['statistics']['external_tables'] += 1
                        print(f"    ✓ External table")
                    
                    # Show performance recommendations count
                    recommendations = perf_chars.get('recommendations', [])
                    if recommendations:
                        print(f"    💡 {len(recommendations)} performance recommendations generated")
                    
                    metadata['tables'][table_ref] = table_metadata
                    metadata['statistics']['total_columns'] += table_metadata['column_count']
                    metadata['statistics']['total_size_mb'] += table_metadata['size_mb']
                    
                except Exception as e:
                    print(f"    ✗ Error processing {table_item.table_id}: {e}")
                    metadata['tables'][table_ref] = {
                        'table_name': table_ref,
                        'error': str(e)
                    }
            
        except Exception as e:
            print(f"✗ Error processing dataset {current_dataset_id}: {e}")
        
        # Calculate extraction duration
        duration = (datetime.now() - start_time).total_seconds()
        metadata['statistics']['extraction_duration_seconds'] = round(duration, 2)
        
        # Add this dataset's metadata to the list
        all_datasets_metadata.append(metadata)
        
        print(f"\n✅ Dataset extraction complete!")
        print(f"📊 Statistics:")
        print(f"   - Tables: {metadata['statistics']['total_tables']}")
        print(f"   - Total columns/paths: {metadata['statistics']['total_columns']}")
        print(f"   - Tables with nested fields: {metadata['statistics']['tables_with_nested_fields']}")
        print(f"   - Partitioned tables: {metadata['statistics']['partitioned_tables']}")
        print(f"   - Clustered tables: {metadata['statistics']['clustered_tables']}")
        print(f"   - Materialized views: {metadata['statistics']['materialized_views']}")
        print(f"   - External tables: {metadata['statistics']['external_tables']}")
        print(f"   - Total size: {metadata['statistics']['total_size_mb']:.2f} MB")
        print(f"   - Extraction time: {duration:.2f} seconds")
    
    # Calculate overall extraction duration
    overall_duration = (datetime.now() - overall_start_time).total_seconds()
    
    # Save to static.json - OUTPUT AS LIST OF DATASET JSON OBJECTS
    output_file = 'static_new.json'
    with open(output_file, 'w') as f:
        json.dump(all_datasets_metadata, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"✅ Enhanced metadata extraction complete for ALL {len(all_datasets_metadata)} datasets!")
    print(f"{'='*80}")
    print(f"📁 Saved to: {output_file}")
    print(f"📊 Overall Statistics:")
    print(f"   - Total Datasets: {len(all_datasets_metadata)}")
    print(f"   - Total Tables: {sum(d['statistics']['total_tables'] for d in all_datasets_metadata)}")
    print(f"   - Total Columns/Paths: {sum(d['statistics']['total_columns'] for d in all_datasets_metadata)}")
    print(f"   - Total Partitioned Tables: {sum(d['statistics']['partitioned_tables'] for d in all_datasets_metadata)}")
    print(f"   - Total Clustered Tables: {sum(d['statistics']['clustered_tables'] for d in all_datasets_metadata)}")
    print(f"   - Total Materialized Views: {sum(d['statistics']['materialized_views'] for d in all_datasets_metadata)}")
    print(f"   - Total External Tables: {sum(d['statistics']['external_tables'] for d in all_datasets_metadata)}")
    print(f"   - Total Size: {sum(d['statistics']['total_size_mb'] for d in all_datasets_metadata):.2f} MB")
    print(f"   - Overall Extraction Time: {overall_duration:.2f} seconds")
    print(f"\n🎯 Enhanced with comprehensive performance metadata for LLM optimization!")

if __name__ == "__main__":
    main()
