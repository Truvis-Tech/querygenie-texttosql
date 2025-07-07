from fastapi import HTTPException, APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
from src.utils.mock_ldap import verify_token

from src.services.bq_client import get_bigquery_client
from src.utils.logger import logger
from src.database.db_config import GoogleCloudSqlUtility

bq_router = APIRouter(tags=["bigquery"])

class TableRequest(BaseModel):
    market: str

@bq_router.post("/get-tables")
def get_tables(payload: TableRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("GET_TABLES started - User: %s, Market: %s", username, payload.market)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
    
    try:
        if payload.market not in user["markets"]:
            logger.warning("ACCESS_DENIED - User %s attempted to access unauthorized market: %s", 
                          username, payload.market)
            raise HTTPException(status_code=403, detail="Market access denied")
        logger.debug("Getting BigQuery client for market: %s", payload.market)
        client, project_id, dataset_id, location = get_bigquery_client(payload.market)
        
        query = f"""
            SELECT table_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        logger.debug("Executing BigQuery query: %s", query.strip())
        
        result = client.query(query).result()
        tables = [row["table_name"] for row in result]
        
        logger.info("GET_TABLES completed - User: %s, Market: %s, Tables found: %d", 
                   username, payload.market, len(tables))

        return JSONResponse(
            status_code=200,
            content={'result': tables, 'metadata': "", 'sql_query': "", 'textual_summary': [], 'followup_prompts': [], "x-axis": "", "typeOFgraph": ""}
        )
        
    except FileNotFoundError as e:
        logger.error("CREDENTIALS_ERROR - User: %s, Market: %s, File not found: %s", 
                    username, payload.market, str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("GET_TABLES_ERROR - User: %s, Market: %s, Error: %s", 
                        username, payload.market, str(e))
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
class ColumnRequest(BaseModel):
    market: str
    table_name: str
    
@bq_router.post("/get-columns")
def get_columns(payload: ColumnRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("GET_COLUMNS started - User: %s, Market: %s, Table: %s", 
                username, payload.market, payload.table_name)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
    
    try:
        if payload.market not in user["markets"]:
            logger.warning("ACCESS_DENIED - User %s attempted to access unauthorized market: %s", 
                          username, payload.market)
            raise HTTPException(status_code=403, detail="Market access denied")
        logger.debug("Getting BigQuery client for market: %s", payload.market)
        client, project_id, dataset_id, location = get_bigquery_client(payload.market)
        
        query = f"""
            SELECT column_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{payload.table_name}'
            ORDER BY ordinal_position
        """
        logger.debug("Executing BigQuery query: %s", query.strip())
        
        result = client.query(query).result()
        columns = [row["column_name"] for row in result]
        
        logger.info("GET_COLUMNS completed - User: %s, Table: %s, Columns found: %d", 
                   username, payload.table_name, len(columns))

        return JSONResponse(
            status_code=200,
            content={'result': columns, 'metadata': "", 'sql_query': "", 'textual_summary': [], 'followup_prompts': [], "x-axis": "", "typeOFgraph": ""}
        )
        
    except FileNotFoundError as e:
        logger.error("CREDENTIALS_ERROR - User: %s, Market: %s, Table: %s, File not found: %s", 
                    username, payload.market, payload.table_name, str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("GET_COLUMNS_ERROR - User: %s, Market: %s, Table: %s, Error: %s", 
                        username, payload.market, payload.table_name, str(e))
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
class SimpleDataRequest(BaseModel):
    market: str

@bq_router.post("/fetch-data/")
def fetch_data(payload: SimpleDataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("FETCH_DATA started - User: %s, Market: %s", username, payload.market)
    client, project_id, dataset_id, location = get_bigquery_client(payload.market)
    query = f"""
            SELECT * FROM `{project_id}.{dataset_id}.rulemaster` AS RM
                              INNER JOIN `{project_id}.{dataset_id}.recommendation` AS RE
                                         ON RM.rule_id = RE.rule_id \
            """
    query_job = client.query(query)
    results = query_job.result()

    data = [dict(row.items()) for row in results]
    return {"data": data, "project_id": project_id}

@bq_router.post("/fetch-count/schema")
def fetch_count_schema(payload: SimpleDataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("FETCH_COUNT_SCHEMA started - User: %s, Market: %s", username, payload.market)
    db_util = GoogleCloudSqlUtility()
    conn, connector = db_util.get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SUM(no_of_schemas) AS total_schemas FROM recommendation_table")
    row = cur.fetchone()
    cur.close(); conn.close(); connector.close()
    return {"data": row}

@bq_router.post("/fetch-count/queries")
def fetch_count_queries(payload: SimpleDataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("FETCH_COUNT_QUERIES started - User: %s, Market: %s", username, payload.market)
    db_util = GoogleCloudSqlUtility()
    conn, connector = db_util.get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SUM(no_of_queries) AS total_schemas FROM recommendation_table")
    row = cur.fetchone()
    cur.close(); conn.close(); connector.close()
    return {"data": row}

@bq_router.post("/fetch-count/total_scanned")
def fetch_count_total(payload: SimpleDataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("FETCH_COUNT_TOTAL started - User: %s, Market: %s", username, payload.market)
    db_util = GoogleCloudSqlUtility()
    conn, connector = db_util.get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT total_query_scanned AS total_query_scanned FROM recommendation_table LIMIT 1")
    row = cur.fetchone()
    cur.close(); conn.close(); connector.close()
    return {"data": row}