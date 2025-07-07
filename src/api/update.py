from fastapi import HTTPException, APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from typing import Union, List, Dict, Any
from datetime import datetime
import json
import os

from src.utils.config_reader import load_config
from src.database.db_connector import get_postgres_connection_params
from src.database.postgres_loader import PostgresLoader, generate_id_key, generate_column_id_key
from src.database.audit_loader import get_table_audit_data, get_column_audit_data, handle_generic_audit
from src.utils.mock_ldap import verify_token
from src.utils.logger import logger
from src.database.postgres_vector_loader import PostgresVectorLoader

update_router = APIRouter(tags=["context update"])

class TableUpdateRequest(BaseModel):
    market: str
    table_name: str
    obj: Union[Dict[str, Any], List[Dict[str, Any]]]

@update_router.post("/update-table")
def update_table(payload: TableUpdateRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("UPDATE_TABLE started - User: %s, Market: %s, Table: %s", 
                username, payload.market, payload.table_name)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
        
    conn = None
    cur = None
    try:
        config = load_config(payload.market)
        host, port, database, user_db, password = get_postgres_connection_params(payload.market)
        project_id = config.get('Database', 'bigquery_project')
        dataset_id = config.get('Database', 'bigquery_dataset')

        conn = psycopg2.connect(host=host, port=port, dbname=database, user=user_db, password=password)
        cur = conn.cursor()

        rows_to_update = payload.obj if isinstance(payload.obj, list) else [payload.obj]
        pg = PostgresLoader(payload.market)
        postgres_vector_loader = PostgresVectorLoader(payload.market)

        for update_data in rows_to_update:
            id_key = generate_id_key(
                project_id,
                dataset_id,
                payload.table_name
            )
            
            old_data = get_table_audit_data(cur, id_key)
            
            update_data['id_key'] = id_key
            update_data['data_source_id'] = project_id
            update_data['table_name'] = payload.table_name
            update_data.setdefault('display_name', payload.table_name)
            update_data.setdefault('data_namespace', dataset_id)
            
            current_time = datetime.utcnow()
            update_data['updated_at'] = current_time
            update_data['updated_by'] = username

            processed_data = {}
            for key, value in update_data.items():
                if key in ['filter_columns', 'aggregate_columns', 'sort_columns', 'key_columns', 'tags', 'related_business_terms']:
                    processed_data[key] = value if value is not None else []
                elif key in ['sample_usage', 'join_tables']:
                    processed_data[key] = json.dumps(value) if value is not None else '[]'
                else:
                    processed_data[key] = value
            
            keys = list(processed_data.keys())
            values = [processed_data[k] for k in keys]

            update_fields = [k for k in keys if k != "id_key"]
            
            query = sql.SQL("""
                INSERT INTO table_config ({fields})
                VALUES ({placeholders})
                ON CONFLICT (id_key) DO UPDATE SET
                {updates}
            """).format(
                fields=sql.SQL(', ').join(map(sql.Identifier, keys)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(keys)),
                updates=sql.SQL(', ').join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(k)) for k in update_fields
                )
            )
            cur.execute(query, tuple(values))

            postgres_vector_loader.insert_table_context(update_data, "table_context")
            
            new_data = get_table_audit_data(cur, id_key)

            handle_generic_audit(
                pg=pg,
                cur=cur,
                user_id=user.get('uid') or 'unknown',
                user_name=user.get('username', 'unknown'),
                old_data=old_data,
                new_data=new_data,
                entity_name=payload.table_name,
                entity_id=id_key
            )

        conn.commit()
        logger.info("UPDATE_TABLE completed successfully - User: %s, Market: %s, Table: %s, Rows updated: %d", 
                    username, payload.market, payload.table_name, len(rows_to_update))
        return JSONResponse(status_code=200, content={"message": "Row(s) update successful."})

    except Exception as e:
        logger.exception("UPDATE_TABLE_ERROR - User: %s, Market: %s, Table: %s, Error: %s", 
                        username, payload.market, payload.table_name, str(e))
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

class ColumnUpdateRequest(BaseModel):
    market: str
    table_name: str
    column_name: str
    obj: Union[Dict[str, Any], List[Dict[str, Any]]]

@update_router.post("/update-columns")
def update_columns(payload: ColumnUpdateRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("UPDATE_COLUMNS started - User: %s, Market: %s, Table: %s, Column: %s", 
                username, payload.market, payload.table_name, payload.column_name)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
        
    conn = None
    cur = None
    try:
        config = load_config(payload.market)
        host, port, database, user_db, password = get_postgres_connection_params(payload.market)
        project_id = config.get('Database', 'bigquery_project')
        dataset_id = config.get('Database', 'bigquery_dataset')

        conn = psycopg2.connect(host=host, port=port, dbname=database, user=user_db, password=password)
        cur = conn.cursor()

        rows_to_update = payload.obj if isinstance(payload.obj, list) else [payload.obj]
        pg = PostgresLoader(payload.market)
        postgres_vector_loader = PostgresVectorLoader(payload.market)

        for update_data in rows_to_update:
            id_key = generate_column_id_key(
                project_id,
                dataset_id,
                payload.table_name,
                payload.column_name
            )

            old_data = get_column_audit_data(cur, id_key)
            update_data['id_key'] = id_key
            update_data['data_source_id'] = project_id
            update_data['table_name'] = payload.table_name
            update_data['column_name'] = payload.column_name
            update_data.setdefault('data_namespace', dataset_id)

            if 'data_type' not in update_data:
                if old_data and 'data_type' in old_data:
                    update_data['data_type'] = old_data['data_type']
                else:
                    logger.error("UPDATE_COLUMNS_ERROR - 'data_type' is missing for a new column entry. Payload: %s", update_data)
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing required field 'data_type' for creating metadata for new column '{payload.column_name}'."
                    )
            
            current_time = datetime.now()
            update_data['updated_at'] = current_time
            update_data['updated_by'] = username
            
            processed_data = {}
            for key, value in update_data.items():
                if key in ['sample_values', 'related_business_terms']:
                    processed_data[key] = value if value is not None else []
                elif key in ['sample_usage']:
                    processed_data[key] = json.dumps(value) if value is not None else '[]'
                else:
                    processed_data[key] = value

            keys = list(processed_data.keys())
            values = [processed_data[k] for k in keys]

            update_fields = [k for k in keys if k != "id_key"]

            query = sql.SQL("""
                INSERT INTO column_config ({fields})
                VALUES ({placeholders})
                ON CONFLICT (id_key) DO UPDATE SET
                {updates}
            """).format(
                fields=sql.SQL(', ').join(map(sql.Identifier, keys)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(keys)),
                updates=sql.SQL(', ').join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(k)) for k in update_fields
                )
            )
            cur.execute(query, tuple(values))

            postgres_vector_loader.insert_column_context(update_data, "column_context")

            new_data = get_column_audit_data(cur, id_key)

            handle_generic_audit(
                pg=pg,
                cur=cur,
                user_id=user.get('uid') or 'unknown',
                user_name=user.get('username', 'unknown'),
                old_data=old_data,
                new_data=new_data,
                entity_name=payload.table_name + '.' + payload.column_name,
                entity_id=id_key
            )

        conn.commit()
        logger.info("UPDATE_COLUMNS completed successfully - User: %s, Market: %s, Table: %s, Column: %s, Rows updated: %d", 
                    username, payload.market, payload.table_name, payload.column_name, len(rows_to_update))
        return JSONResponse(status_code=200, content={"message": "Column row(s) update successful."})

    except HTTPException:
        raise
    except Exception as e:
        logger.error("UPDATE_COLUMNS failed - %s", str(e))
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    finally:
        if cur: cur.close()
        if conn: conn.close()