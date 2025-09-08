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
    Load static.json and insert table metadata into PostgreSQL.
    Creates public.bq_metadata table and upserts each table's metadata.
    """
    print("🚀 Starting upload to PostgreSQL...")
    
    # Load static.json
    try:
        with open('static.json', 'r') as f:
            static_metadata = json.load(f)
    except FileNotFoundError:
        print("❌ Error: 'static.json' not found. Please generate it first by running 'python static.py'.")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in static.json: {e}")
        return

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
        
        # Create index for faster lookups
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_bq_metadata_table_name 
            ON public.bq_metadata(table_name);
        ''')
        
        conn.commit()
        print("📋 Created/verified bq_metadata table structure")

        # Extract global metadata
        extraction_ts = static_metadata.get('extraction_timestamp')
        project_id = static_metadata.get('project_id')
        dataset_id = static_metadata.get('dataset_id')

        # Insert each table's metadata
        tables = static_metadata.get('tables', {})
        for table_name, metadata in tables.items():
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

        conn.commit()
        print(f"✅ Successfully inserted/updated {len(tables)} table records into bq_metadata")
        
        # Show summary
        cur.execute("SELECT COUNT(*) FROM public.bq_metadata;")
        total_count = cur.fetchone()[0]
        print(f"📊 Total records in bq_metadata: {total_count}")

        cur.close()

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed")

if __name__ == '__main__':
    main()
