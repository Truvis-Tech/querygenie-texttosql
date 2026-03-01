import json
import psycopg2
from psycopg2.extras import Json

# Database connection configuration
# Update these values according to your environment
DB_CONFIG = {
    "host": "",
    "port": 5432,
    "user": "postgres",
    "password": "",
    "database": "postgres"
}

def main():
    """
    Load static.json (list of datasets) and insert table metadata into PostgreSQL.
    Creates public.bq_metadata table and upserts each table's metadata from all datasets.
    """
    print("🚀 Starting upload to PostgreSQL...")
    
    # Load static.json
    try:
        with open('static_new.json', 'r') as f:
            datasets_metadata = json.load(f)
    except FileNotFoundError:
        print("❌ Error: 'static_new.json' not found. Please generate it first by running the extraction script.")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in static_new.json: {e}")
        return
    
    # Validate that we have a list
    if not isinstance(datasets_metadata, list):
        print("❌ Error: Expected static_new.json to contain a list of datasets")
        return
    
    print(f"📦 Found {len(datasets_metadata)} datasets to process")
    
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print(f"✅ Connected to PostgreSQL database: {DB_CONFIG['database']}")
        
        # Create the bq_metadata table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.bq_metadata (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) UNIQUE NOT NULL,
                project_id VARCHAR(255),
                dataset_id VARCHAR(255), 
                metadata JSONB NOT NULL,
                extraction_time TIMESTAMPTZ
            );
        ''')
        
        # Create indexes for faster lookups
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_bq_metadata_table_name 
            ON public.bq_metadata(table_name);
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_bq_metadata_dataset_id 
            ON public.bq_metadata(dataset_id);
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_bq_metadata_project_id 
            ON public.bq_metadata(project_id);
        ''')
        
        conn.commit()
        print("📋 Created/verified bq_metadata table structure with indexes")
        
        # Process each dataset
        total_tables = 0
        for dataset_idx, dataset_metadata in enumerate(datasets_metadata, 1):
            extraction_ts = dataset_metadata.get('extraction_timestamp')
            project_id = dataset_metadata.get('project_id')
            dataset_id = dataset_metadata.get('dataset_id')
            
            print(f"\n📊 Processing dataset [{dataset_idx}/{len(datasets_metadata)}]: {dataset_id}")
            
            # Insert each table's metadata from this dataset
            tables = dataset_metadata.get('tables', {})
            
            if not tables:
                print(f"   ⚠️  No tables found in dataset {dataset_id}")
                continue
            
            dataset_table_count = 0
            for table_name, metadata in tables.items():
                # Skip tables with errors
                if 'error' in metadata and len(metadata) <= 2:
                    print(f"   ⚠️  Skipping table {table_name} (error during extraction)")
                    continue
                
                cur.execute('''
                    INSERT INTO public.bq_metadata (
                        table_name, project_id, dataset_id, metadata, extraction_time
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (table_name) DO UPDATE SET
                        project_id = EXCLUDED.project_id,
                        dataset_id = EXCLUDED.dataset_id,
                        metadata = EXCLUDED.metadata,
                        extraction_time = EXCLUDED.extraction_time;
                ''', (table_name, project_id, dataset_id, Json(metadata), extraction_ts))
                
                dataset_table_count += 1
                total_tables += 1
            
            conn.commit()
            print(f"   ✅ Inserted/updated {dataset_table_count} tables from dataset {dataset_id}")
        
        print(f"\n✅ Successfully processed {len(datasets_metadata)} datasets")
        print(f"✅ Total tables inserted/updated: {total_tables}")
        
        # Show summary by dataset
        print("\n📊 Summary by dataset:")
        cur.execute('''
            SELECT dataset_id, COUNT(*) as table_count 
            FROM public.bq_metadata 
            GROUP BY dataset_id 
            ORDER BY dataset_id;
        ''')
        
        for row in cur.fetchall():
            print(f"   - {row[0]}: {row[1]} tables")
        
        # Show overall summary
        cur.execute("SELECT COUNT(*) FROM public.bq_metadata;")
        total_count = cur.fetchone()[0]
        print(f"\n📊 Total records in bq_metadata: {total_count}")
        
        # Show statistics about table types
        cur.execute('''
            SELECT 
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'partitioning' IS NOT NULL) as partitioned_tables,
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'clustering' IS NOT NULL) as clustered_tables,
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'materialized_view' IS NOT NULL) as materialized_views,
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'external_table' IS NOT NULL) as external_tables
            FROM public.bq_metadata;
        ''')
        
        stats = cur.fetchone()
        print(f"\n📈 Table characteristics:")
        print(f"   - Partitioned tables: {stats[0]}")
        print(f"   - Clustered tables: {stats[1]}")
        print(f"   - Materialized views: {stats[2]}")
        print(f"   - External tables: {stats[3]}")
        
        cur.close()
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")

if __name__ == '__main__':
    main()
