"""
BigQuery Metadata Uploader Service
Handles uploading extracted metadata to PostgreSQL
"""
import psycopg2
from psycopg2.extras import Json
from typing import Dict, List, Any
from src.utils.logger import logger


class BigQueryMetadataUploader:
    """Service for uploading BigQuery metadata to PostgreSQL"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
    
    def upload(self, datasets_metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Upload metadata to PostgreSQL"""
        logger.info("Starting upload to PostgreSQL")
        
        if not isinstance(datasets_metadata, list):
            raise ValueError("Expected datasets_metadata to be a list")
        
        try:
            self._connect()
            self._create_schema()
            stats = self._insert_metadata(datasets_metadata)
            db_stats = self._get_database_stats()
            
            return {
                'total_datasets': len(datasets_metadata),
                'total_tables': stats['total_tables'],
                'database_total_records': db_stats['total_records'],
                'partitioned_tables': db_stats['partitioned_tables'],
                'clustered_tables': db_stats['clustered_tables']
            }
            
        except psycopg2.Error as e:
            logger.error("Database error: %s", str(e))
            if self.conn:
                self.conn.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error: %s", str(e))
            raise
        finally:
            self._disconnect()
    
    def _connect(self):
        """Establish database connection"""
        self.conn = psycopg2.connect(**self.db_config)
        logger.info("Connected to PostgreSQL: %s", self.db_config['database'])
    
    def _disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def _create_schema(self):
        """Create table and indexes if they don't exist"""
        cur = self.conn.cursor()
        
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
        
        self.conn.commit()
        cur.close()
        logger.info("Schema verified/created")
    
    def _insert_metadata(self, datasets_metadata: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert metadata records"""
        cur = self.conn.cursor()
        total_tables = 0
        
        for dataset_idx, dataset_metadata in enumerate(datasets_metadata, 1):
            extraction_ts = dataset_metadata.get('extraction_timestamp')
            project_id = dataset_metadata.get('project_id')
            dataset_id = dataset_metadata.get('dataset_id')
            tables = dataset_metadata.get('tables', {})
            
            logger.info("Processing dataset [%d/%d]: %s", dataset_idx, len(datasets_metadata), dataset_id)
            
            if not tables:
                logger.warning("No tables in dataset %s", dataset_id)
                continue
            
            dataset_table_count = 0
            for table_name, metadata in tables.items():
                if 'error' in metadata and len(metadata) <= 2:
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
            
            self.conn.commit()
            logger.info("Inserted/updated %d tables from dataset %s", dataset_table_count, dataset_id)
        
        cur.close()
        logger.info("Upload complete - %d total tables", total_tables)
        
        return {'total_tables': total_tables}
    
    def _get_database_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        cur = self.conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM public.bq_metadata;")
        total_records = cur.fetchone()[0]
        
        cur.execute('''
            SELECT 
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'partitioning' IS NOT NULL) as partitioned,
                COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'clustering' IS NOT NULL) as clustered
            FROM public.bq_metadata;
        ''')
        stats = cur.fetchone()
        
        cur.close()
        
        return {
            'total_records': total_records,
            'partitioned_tables': stats[0],
            'clustered_tables': stats[1]
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed database statistics"""
        try:
            self._connect()
            cur = self.conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM public.bq_metadata;")
            total_count = cur.fetchone()[0]
            
            cur.execute('''
                SELECT dataset_id, COUNT(*) as table_count 
                FROM public.bq_metadata 
                GROUP BY dataset_id 
                ORDER BY dataset_id;
            ''')
            dataset_stats = [{'dataset_id': row[0], 'table_count': row[1]} for row in cur.fetchall()]
            
            cur.execute('''
                SELECT 
                    COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'partitioning' IS NOT NULL) as partitioned,
                    COUNT(*) FILTER (WHERE metadata->'performance_characteristics'->'clustering' IS NOT NULL) as clustered
                FROM public.bq_metadata;
            ''')
            stats = cur.fetchone()
            
            cur.close()
            
            return {
                'success': True,
                'total_tables': total_count,
                'datasets': dataset_stats,
                'table_characteristics': {
                    'partitioned_tables': stats[0],
                    'clustered_tables': stats[1]
                }
            }
            
        finally:
            self._disconnect()
