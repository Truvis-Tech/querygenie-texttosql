from src.database.db_config import GoogleCloudSqlUtility
import json
from datetime import datetime
from src.utils.logger import logger

def validate_required_fields(metadata, required_fields, object_type="record"):
    logger.debug("Validating required fields for %s: %s", object_type, required_fields)
    for field in required_fields:
        if field not in metadata:
            logger.error("VALIDATION_ERROR - Missing field '%s' in %s", field, object_type)
            raise ValueError(f"Missing required field '{field}' in {object_type}")
        if metadata[field] is None:
            logger.error("VALIDATION_ERROR - Field '%s' is None in %s", field, object_type)
            raise ValueError(f"Required field '{field}' cannot be None in {object_type}")
    logger.debug("Validation successful for %s", object_type)

def generate_id_key(data_source_id, data_namespace, table_name_details):
    namespace = data_namespace if data_namespace and data_namespace.strip() else "NA"
    id_key = f"{data_source_id}~{namespace}~{table_name_details}"
    logger.debug("Generated ID key: %s", id_key)
    return id_key

def generate_column_id_key(data_source_id, data_namespace, table_name_details, column_name_details):
    namespace = data_namespace if data_namespace and data_namespace.strip() else "NA"
    id_key = f"{data_source_id}~{namespace}~{table_name_details}~{column_name_details}"
    logger.debug("Generated column ID key: %s", id_key)
    return id_key

class PostgresLoader:
    def __init__(self):
        self.db_util = GoogleCloudSqlUtility()
        logger.info("PostgresLoader initialized using GoogleCloudSqlUtility")

    def _get_connection(self):
        conn, connector = self.db_util.get_db_connection()
        return conn, connector

    def create_table_config(self):
        logger.info("Creating table_config table")
        create_table_config_sql = """
        CREATE TABLE IF NOT EXISTS table_config (
        id_key VARCHAR(200) PRIMARY KEY,
        data_source_id VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        display_name VARCHAR(100) NOT NULL,
        data_namespace VARCHAR(100),
        description TEXT,
        filter_columns TEXT[],
        aggregate_columns TEXT[],
        sort_columns TEXT[],
        key_columns TEXT[],
        join_tables JSONB,  
        related_business_terms TEXT[],
        sample_usage JSONB,
        tags TEXT[],
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        created_by VARCHAR(100),
        updated_by VARCHAR(100),
        CONSTRAINT table_config_table_name_key UNIQUE (data_source_id, table_name)
        );
        """
        conn, connector = None, None
        try:
            conn, connector = self._get_connection()
            cur = conn.cursor()
            cur.execute(create_table_config_sql)
            conn.commit()
            cur.close()
            logger.info("table_config table created successfully")
        except Exception as e:
            logger.exception("CREATE_TABLE_CONFIG_ERROR - Error: %s", str(e))
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            if connector:
                connector.close()

    def create_column_config(self):
        logger.info("Creating column_config table")
        create_column_config_sql = """
        CREATE TABLE IF NOT EXISTS column_config (
        id_key VARCHAR(200) PRIMARY KEY,
        data_source_id VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        column_name VARCHAR(100) NOT NULL,
        data_namespace VARCHAR(100),
        description TEXT,
        data_type VARCHAR(20) NOT NULL,
        is_filterable BOOLEAN DEFAULT FALSE,
        is_aggregatable BOOLEAN DEFAULT FALSE,
        sample_values TEXT[],
        related_business_terms TEXT[],
        sample_usage JSONB,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        created_by VARCHAR(100),
        updated_by VARCHAR(100),
        CONSTRAINT unique_table_column UNIQUE (data_source_id, table_name, column_name)
        );
        """

        conn, connector = None, None
        try:
            conn, connector = self._get_connection()
            cur = conn.cursor()
            cur.execute(create_column_config_sql)
            conn.commit()
            cur.close()
            logger.info("column_config table created successfully")
        except Exception as e:
            logger.exception("CREATE_COLUMN_CONFIG_ERROR - Error: %s", str(e))
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            if connector:
                connector.close()
    
    def create_audit_logs(self):
        logger.info("Creating audit_logs table")
        create_audit = """
        CREATE TABLE IF NOT EXISTS audit_logs (
        id BIGSERIAL PRIMARY KEY,
        
        user_id TEXT,
        user_name TEXT,
        
        action TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        
        event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        
        old_data JSONB, 
        new_data JSONB, 
        
        metadata JSONB
        );
        """

        conn, connector = None, None
        try:
            conn, connector = self._get_connection()
            cur = conn.cursor()
            cur.execute(create_audit)
            conn.commit()
            cur.close()
            logger.info("audit_logs table created successfully")
        except Exception as e:
            logger.exception("CREATE_AUDIT_LOGS_ERROR - Error: %s", str(e))
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            if connector:
                connector.close()

    def insert_table_metadata(self, metadata, table_name="table_config"):
        table_name_val = metadata.get('table_name', 'unknown')
        data_source_id = metadata.get('data_source_id', 'unknown')
        logger.info("INSERT_TABLE_METADATA_START - Table: %s, DataSource: %s", table_name_val, data_source_id)
        
        conn, connector = None, None
        try:
            required_fields = ['data_source_id', 'table_name', 'display_name']
            validate_required_fields(metadata, required_fields, object_type="table metadata")
            
            conn, connector = self._get_connection()
            cur = conn.cursor()

            created_at = datetime.utcnow()
            updated_at = datetime.utcnow()
            
            id_key = generate_id_key(metadata.get("data_source_id"), metadata.get('data_namespace', ''), metadata['table_name'])
            logger.debug(f"Generated id_key: {id_key}")
            
            join_tables_data = json.dumps(metadata.get('join_tables', []))
            sample_usage_data = json.dumps(metadata.get('sample_usage', []))
            
            logger.debug("Inserting table metadata with ID key: %s", id_key)
            
            insert_sql = """
                INSERT INTO table_config (
                    id_key, data_source_id, table_name, display_name, data_namespace, description,
                    filter_columns, aggregate_columns, sort_columns, key_columns, join_tables,
                    related_business_terms, sample_usage, tags, created_at, updated_at,
                    created_by, updated_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s)
                ON CONFLICT (id_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    filter_columns = EXCLUDED.filter_columns,
                    aggregate_columns = EXCLUDED.aggregate_columns,
                    sort_columns = EXCLUDED.sort_columns,
                    key_columns = EXCLUDED.key_columns,
                    join_tables = EXCLUDED.join_tables,
                    related_business_terms = EXCLUDED.related_business_terms,
                    sample_usage = EXCLUDED.sample_usage,
                    tags = EXCLUDED.tags,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                RETURNING id_key;
            """

            logger.debug("Executing INSERT/UPDATE query")
            cur.execute(insert_sql, (
                id_key,
                metadata.get("data_source_id"),
                metadata['table_name'],
                metadata['display_name'],
                metadata.get('data_namespace', None),
                metadata.get('description', None),
                metadata.get('filter_columns', []),
                metadata.get('aggregate_columns', []),
                metadata.get('sort_columns', []),
                metadata.get('key_columns', []),
                join_tables_data,
                metadata.get('related_business_terms', []),
                sample_usage_data,
                metadata.get('tags', []),
                created_at,
                updated_at,
                metadata.get('created_by', 'admin'),
                metadata.get('updated_by', 'admin')
            ))

            inserted_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            
            logger.info("INSERT_TABLE_METADATA_SUCCESS - ID: %s, Table: %s", inserted_id, table_name_val)
            return inserted_id

        except Exception as e:
            logger.exception("INSERT_TABLE_METADATA_ERROR - Table: %s, Error: %s", table_name_val, str(e))
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            if connector:
                connector.close()

    def insert_column_metadata(self, metadata, table_name="column_config"):
        table_name_val = metadata.get('table_name', 'unknown')
        column_name_val = metadata.get('column_name', 'unknown')
        logger.info("INSERT_COLUMN_METADATA_START - Table: %s, Column: %s", table_name_val, column_name_val)
        
        conn, connector = None, None
        try:
            required_fields = ['data_source_id', 'table_name', 'column_name', 'data_type']
            validate_required_fields(metadata, required_fields, object_type="column metadata")
            
            conn, connector = self._get_connection()
            cur = conn.cursor()

            created_at = datetime.utcnow()
            updated_at = datetime.utcnow()
            
            id_key = generate_column_id_key(
                metadata.get("data_source_id"),
                metadata.get('data_namespace', ''),
                metadata['table_name'],
                metadata['column_name']
            )
            logger.debug(f"Generated id_key: {id_key}")
            
            sample_usage_data = json.dumps(metadata.get('sample_usage', []))

            logger.debug("Inserting column metadata with ID key: %s", id_key)

            insert_sql = """
                INSERT INTO column_config (id_key, data_source_id, table_name, column_name, data_namespace,
                                    description, data_type, is_filterable, is_aggregatable, sample_values, related_business_terms,
                                    sample_usage, created_at, updated_at, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT (id_key) DO UPDATE SET
                description = EXCLUDED.description,
                data_type = EXCLUDED.data_type,
                is_filterable = EXCLUDED.is_filterable,
                is_aggregatable = EXCLUDED.is_aggregatable,
                sample_values = EXCLUDED.sample_values,
                related_business_terms = EXCLUDED.related_business_terms,
                sample_usage = EXCLUDED.sample_usage,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
                RETURNING id_key;
            """

            logger.debug("Executing INSERT/UPDATE query")
            cur.execute(insert_sql, (
                id_key,
                metadata.get("data_source_id"),
                metadata['table_name'],
                metadata['column_name'],
                metadata.get('data_namespace', None),
                metadata.get('description', None),
                metadata['data_type'],
                metadata.get('is_filterable', False),
                metadata.get('is_aggregatable', False),
                metadata.get('sample_values', []),
                metadata.get('related_business_terms', []),
                sample_usage_data,
                created_at,
                updated_at,
                metadata.get('created_by', 'admin'),
                metadata.get('updated_by', 'admin')
            ))

            inserted_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            
            logger.info("INSERT_COLUMN_METADATA_SUCCESS - ID: %s, Table: %s, Column: %s", 
                       inserted_id, table_name_val, column_name_val)
            return inserted_id

        except Exception as e:
            logger.exception("INSERT_COLUMN_METADATA_ERROR - Table: %s, Column: %s, Error: %s", 
                           table_name_val, column_name_val, str(e))
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
            if connector:
                connector.close()

    def insert_audit_log(self, cur, user_id: str, user_name: str, 
                       action: str, entity_name: str, entity_id: str,
                       old_data: dict, new_data: dict, metadata: dict):
        """Universal audit log insertion with enhanced logging"""
        logger.debug("INSERT_AUDIT_LOG - User: %s, Action: %s, Entity: %s, ID: %s", 
                    user_name, action, entity_name, entity_id)
        
        try:
            audit_sql = """
                INSERT INTO audit_logs (
                    user_id, user_name, action, entity_name, entity_id,
                    event_time, old_data, new_data, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(audit_sql, (
                user_id,
                user_name,
                action,
                entity_name,
                entity_id,
                datetime.utcnow(),
                json.dumps(old_data) if old_data else None,
                json.dumps(new_data) if new_data else None,
                json.dumps(metadata)
            ))
            logger.debug("Audit log inserted successfully")
        except Exception as e:
            logger.exception("INSERT_AUDIT_LOG_ERROR - User: %s, Action: %s, Error: %s", 
                           user_name, action, str(e))
            raise