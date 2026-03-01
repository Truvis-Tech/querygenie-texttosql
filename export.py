import json
import psycopg2
import logging
from typing import List, Dict, Any, Optional
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgreSQLVectorImporter:
    """
    A class to handle importing JSON data with vector embeddings into PostgreSQL
    with vector extension support.
    """
    
    def __init__(self, host: str, database: str, user: str, password: str, port: str = "5432"):
        """
        Initialize the database connection parameters.
        
        Args:
            host: PostgreSQL host (CloudSQL public IP)
            database: Database name
            user: Username
            password: Password
            port: Port number (default: 5432)
        """
        self.connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        
    def get_connection(self):
        """Create and return a database connection."""
        try:
            conn = psycopg2.connect(**self.connection_params)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def setup_database(self, table_name: str, vector_dimension: int = 1536):
        """
        Create the vector extension and table if they don't exist.
        
        Args:
            table_name: Name of the table to create
            vector_dimension: Dimension of the vector embeddings (default: 1536)
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            logger.info("Creating vector extension...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            logger.info(f"Creating table {table_name}...")
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id_key       VARCHAR(255) PRIMARY KEY,
                    embedding    VECTOR({vector_dimension}),
                    raw_text     TEXT
                );
            """
            cursor.execute(create_table_query)
            
            # Create index for better vector similarity search performance
            index_query = f"""
                CREATE INDEX IF NOT EXISTS {table_name}_embedding_idx 
                ON {table_name} USING ivfflat (embedding vector_cosine_ops) 
                WITH (lists = 100);
            """
            cursor.execute(index_query)
            
            conn.commit()
            logger.info(f"Database setup completed successfully for table: {table_name}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during database setup: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def validate_and_fix_embedding(self, embedding: List[float], expected_dim: int = 1536, record_id: str = "unknown") -> List[float]:
        """
        Validate embedding dimensions and fix if necessary.
        
        Args:
            embedding: List of float values representing the embedding
            expected_dim: Expected dimension (default: 1536)
            record_id: ID of the record for logging purposes
            
        Returns:
            Fixed embedding with correct dimensions
        """
        current_dim = len(embedding)
        
        if current_dim == expected_dim:
            return embedding
        elif current_dim < expected_dim:
            # Pad with zeros
            logger.warning(f"Record {record_id}: Padding embedding from {current_dim} to {expected_dim} dimensions")
            return embedding + [0.0] * (expected_dim - current_dim)
        else:
            # Truncate
            logger.warning(f"Record {record_id}: Truncating embedding from {current_dim} to {expected_dim} dimensions")
            return embedding[:expected_dim]
    
    def convert_list_to_vector_string(self, embedding_list: List[float]) -> str:
        """
        Convert Python list to PostgreSQL vector string format.
        
        Args:
            embedding_list: List of float values
            
        Returns:
            Vector string in format '[1.0,2.0,3.0,...]'
        """
        # Ensure all values are properly formatted floats
        formatted_values = []
        for val in embedding_list:
            if val is None:
                formatted_values.append('0.0')
            else:
                formatted_values.append(str(float(val)))
        
        return '[' + ','.join(formatted_values) + ']'
    
    def validate_json_data(self, json_data: List[Dict[str, Any]]) -> bool:
        """
        Validate the structure of JSON data.
        
        Args:
            json_data: List of dictionaries containing the data
            
        Returns:
            True if valid, raises exception if invalid
        """
        if not isinstance(json_data, list):
            raise ValueError("JSON data must be a list of dictionaries")
        
        if not json_data:
            raise ValueError("JSON data is empty")
        
        # Check first record structure
        sample_record = json_data[0]
        required_fields = ['id_key', 'embedding', 'raw_text']
        
        for field in required_fields:
            if field not in sample_record:
                raise ValueError(f"Missing required field: {field}")
        
        if not isinstance(sample_record['embedding'], list):
            raise ValueError("Embedding must be a list of numbers")
        
        logger.info(f"JSON data validation passed. Found {len(json_data)} records")
        return True
    
    def insert_data_from_json(self, json_file_path: str, table_name: str, batch_size: int = 100) -> Dict[str, int]:
        """
        Insert data from JSON file into PostgreSQL table.
        
        Args:
            json_file_path: Path to the JSON file
            table_name: Name of the target table
            batch_size: Number of records to process in each batch
            
        Returns:
            Dictionary with success and failure counts
        """
        # Load JSON data
        logger.info(f"Loading JSON data from: {json_file_path}")
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file: {e}")
            raise
        
        # Validate JSON structure
        self.validate_json_data(json_data)
        
        # Initialize counters
        stats = {
            'total_records': len(json_data),
            'successful_inserts': 0,
            'failed_inserts': 0,
            'updated_records': 0
        }
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Prepare insert query with upsert functionality
            insert_query = f"""
                INSERT INTO {table_name} (id_key, embedding, raw_text)
                VALUES (%s, %s::vector, %s)
                ON CONFLICT (id_key) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    raw_text  = EXCLUDED.raw_text;
            """
            
            logger.info(f"Starting data insertion in batches of {batch_size}...")
            
            # Process data in batches
            for i in range(0, len(json_data), batch_size):
                batch = json_data[i:i + batch_size]
                batch_number = (i // batch_size) + 1
                total_batches = (len(json_data) + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches}")
                
                batch_data = []
                batch_failed = 0
                
                for record in batch:
                    try:
                        # Extract and validate data
                        id_key = record['id_key']
                        raw_text = record['raw_text']
                        embedding_list = record['embedding']
                        
                        # Validate and fix embedding
                        validated_embedding = self.validate_and_fix_embedding(
                            embedding_list, 
                            expected_dim=1536, 
                            record_id=id_key
                        )
                        
                        # Convert to vector string format
                        vector_string = self.convert_list_to_vector_string(validated_embedding)
                        
                        # Add to batch
                        batch_data.append((id_key, vector_string, raw_text))
                        
                    except Exception as e:
                        logger.error(f"Error processing record {record.get('id_key', 'unknown')}: {e}")
                        batch_failed += 1
                        continue
                
                # Execute batch insertion
                if batch_data:
                    try:
                        cursor.executemany(insert_query, batch_data)
                        stats['successful_inserts'] += len(batch_data)
                        logger.info(f"Batch {batch_number} completed: {len(batch_data)} records inserted")
                    except Exception as e:
                        logger.error(f"Error inserting batch {batch_number}: {e}")
                        stats['failed_inserts'] += len(batch_data)
                        # Continue with next batch instead of failing completely
                        continue
                
                stats['failed_inserts'] += batch_failed
            
            # Commit all changes
            conn.commit()
            logger.info("All batches processed successfully. Committing transaction...")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during data insertion, rolling back: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        return stats
    
    def verify_insertion(self, table_name: str) -> Dict[str, Any]:
        """
        Verify the data insertion and return statistics.
        
        Args:
            table_name: Name of the table to verify
            
        Returns:
            Dictionary with verification results
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        verification_results = {}
        
        try:
            # Count total records
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            verification_results['total_records'] = total_count
            
            # Check vector dimensions
            cursor.execute(f"""
                SELECT 
                    MIN(array_length(embedding::real[], 1)) as min_dim,
                    MAX(array_length(embedding::real[], 1)) as max_dim,
                    AVG(array_length(embedding::real[], 1)) as avg_dim
                FROM {table_name}
            """)
            dim_stats = cursor.fetchone()
            verification_results['dimension_stats'] = {
                'min': dim_stats[0],
                'max': dim_stats[1],
                'avg': float(dim_stats[2]) if dim_stats[2] else 0
            }
            
            # Get sample records
            cursor.execute(f"""
                SELECT 
                    id_key, 
                    array_length(embedding::real[], 1) as vector_dimension,
                    length(raw_text) as text_length
                FROM {table_name} 
                LIMIT 5
            """)
            samples = cursor.fetchall()
            verification_results['sample_records'] = [
                {
                    'id_key': sample[0],
                    'vector_dimension': sample[1],
                    'text_length': sample[2]
                }
                for sample in samples
            ]
            
            logger.info(f"Verification completed for table {table_name}")
            logger.info(f"Total records: {total_count}")
            logger.info(f"Vector dimensions - Min: {dim_stats[0]}, Max: {dim_stats[1]}, Avg: {dim_stats[2]:.2f}")
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        return verification_results

def main():
    """
    Main function to demonstrate usage of the PostgreSQLVectorImporter.
    """
    # Database configuration
    DB_CONFIG = {
        'host': 'your_cloudsql_host',  # Replace with your CloudSQL public IP
        'database': 'your_database_name',  # Replace with your database name
        'user': 'your_username',  # Replace with your username
        'password': 'your_password',  # Replace with your password
        'port': '5432'
    }
    
    # Configuration
    JSON_FILE_PATH = 'export.json'  # Path to your JSON file
    TABLE_NAME = 'your_table_name'  # Replace with your desired table name
    BATCH_SIZE = 100  # Adjust based on your needs
    
    try:
        # Initialize the importer
        logger.info("Initializing PostgreSQL Vector Importer...")
        importer = PostgreSQLVectorImporter(**DB_CONFIG)
        
        # Setup database (create extension and table)
        logger.info("Setting up database...")
        importer.setup_database(TABLE_NAME)
        
        # Import data from JSON
        logger.info("Starting data import...")
        start_time = datetime.now()
        
        stats = importer.insert_data_from_json(
            json_file_path=JSON_FILE_PATH,
            table_name=TABLE_NAME,
            batch_size=BATCH_SIZE
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print results
        logger.info("=" * 50)
        logger.info("IMPORT COMPLETED")
        logger.info("=" * 50)
        logger.info(f"Total records processed: {stats['total_records']}")
        logger.info(f"Successful insertions: {stats['successful_inserts']}")
        logger.info(f"Failed insertions: {stats['failed_inserts']}")
        logger.info(f"Duration: {duration}")
        logger.info("=" * 50)
        
        # Verify the insertion
        logger.info("Verifying data insertion...")
        verification_results = importer.verify_insertion(TABLE_NAME)
        
        logger.info("=" * 50)
        logger.info("VERIFICATION RESULTS")
        logger.info("=" * 50)
        logger.info(f"Records in database: {verification_results['total_records']}")
        logger.info(f"Vector dimensions: {verification_results['dimension_stats']}")
        logger.info("Sample records:")
        for sample in verification_results['sample_records']:
            logger.info(f"  ID: {sample['id_key']}, Dim: {sample['vector_dimension']}, Text len: {sample['text_length']}")
        
        if stats['successful_inserts'] == stats['total_records']:
            logger.info("✅ All records imported successfully!")
        else:
            logger.warning(f"⚠️  {stats['failed_inserts']} records failed to import")
        
    except Exception as e:
        logger.error(f"Import process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
