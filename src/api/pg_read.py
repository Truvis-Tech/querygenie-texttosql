from fastapi import HTTPException, APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg2
from fastapi.encoders import jsonable_encoder
import os
from src.utils.mock_ldap import verify_token
from typing import Optional

from src.database.db_connector import get_postgres_connection_params
from src.utils.config_reader import load_config
from src.database.postgres_loader import generate_id_key, generate_column_id_key
from src.utils.logger import logger
from src.database.db_config import GoogleCloudSqlUtility

pg_router = APIRouter(tags=["metadata"])

class TableMetadataRequest(BaseModel):
    market: str
    table_name: str

@pg_router.post("/get-table-metadata")
def get_table_metadata(payload: TableMetadataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("GET_TABLE_METADATA started - User: %s, Market: %s, Table: %s", 
                username, payload.market, payload.table_name)
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
    market = payload.market
    table = payload.table_name
    conn = None
    table_meta = None
    try:
        if market not in user["markets"]:
            logger.warning("ACCESS_DENIED - User %s attempted to access unauthorized market: %s", username, market)
            raise HTTPException(status_code=403, detail="Market access denied")
        
        logger.debug("Loading database connection params for market: %s", market)
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        config = load_config(market)
        project_id = config.get('Database', 'bigquery_project')
        dataset_id = config.get('Database', 'bigquery_dataset')
        
        id_key = generate_id_key(project_id, dataset_id, table)
        logger.debug("Generated ID key: %s", id_key)
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing metadata query for ID key: %s", id_key)
        cur.execute("SELECT * FROM table_config WHERE id_key = %s", (id_key,))
        row = cur.fetchone()
        
        if row:
            colnames = [desc[0] for desc in cur.description]
            table_meta = jsonable_encoder(dict(zip(colnames, row)))
            logger.info("GET_TABLE_METADATA completed - User: %s, Table: %s, Metadata found", username, table)
        else:
            logger.warning("GET_TABLE_METADATA - No metadata found for User: %s, Table: %s, ID: %s", 
                          username, table, id_key)
            raise HTTPException(status_code=404, detail="Table metadata not found.")

        return JSONResponse(
            status_code=200,
            content={'result': table_meta, 'metadata': "", 'sql_query': "", 'textual_summary': [], 'followup_prompts': [], "x-axis": "", "typeOFgraph": ""}
        )
        
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - User: %s, Table: %s, PostgreSQL Error: %s", username, table, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_TABLE_METADATA_ERROR - User: %s, Market: %s, Table: %s, Error: %s", 
                        username, market, table, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for user: %s", username)

class ColumnMetadataRequest(BaseModel):
    market: str
    table_name: str
    column_name: str

@pg_router.post("/get-column-metadata")
def get_column_metadata(payload: ColumnMetadataRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("GET_COLUMN_METADATA started - User: %s, Market: %s, Table: %s, Column: %s", 
                username, payload.market, payload.table_name, payload.column_name)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock response")
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
    
    market = payload.market
    table = payload.table_name
    column = payload.column_name
    conn = None
    
    try:
        if market not in user["markets"]:
            logger.warning("ACCESS_DENIED - User %s attempted to access unauthorized market: %s", username, market)
            raise HTTPException(status_code=403, detail="Market access denied")
        
        logger.debug("Loading database connection params for market: %s", market)
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        config = load_config(market)
        project_id = config.get('Database', 'bigquery_project')
        dataset_id = config.get('Database', 'bigquery_dataset')
        
        id_key = generate_column_id_key(project_id, dataset_id, table, column)
        logger.debug("Generated column ID key: %s", id_key)
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing column metadata query for ID key: %s", id_key)
        cur.execute("SELECT * FROM column_config WHERE id_key = %s", (id_key,))
        row = cur.fetchone()
        
        if row:
            colnames = [desc[0] for desc in cur.description]
            column_meta = jsonable_encoder(dict(zip(colnames, row)))
            logger.info("GET_COLUMN_METADATA completed - User: %s, Column: %s.%s, Metadata found", 
                       username, table, column)
        else:
            logger.warning("GET_COLUMN_METADATA - No metadata found for User: %s, Column: %s.%s, ID: %s", 
                          username, table, column, id_key)
            raise HTTPException(status_code=404, detail="Column metadata not found.")

        return JSONResponse(
            status_code=200,
            content={'result': column_meta, 'metadata': "", 'sql_query': "", 'textual_summary': [], 'followup_prompts': [], "x-axis": "", "typeOFgraph": ""}
        )
        
    except HTTPException:
        raise
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - User: %s, Market: %s, Table: %s, Column: %s, PostgreSQL Error: %s", 
                    username, market, table, column, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_COLUMN_METADATA_ERROR - User: %s, Market: %s, Table: %s, Column: %s, Error: %s", 
                        username, market, table, column, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)

class AuditTableRequest(BaseModel):
    market: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None

class RecommendationTableRequest(BaseModel):
    market: str

@pg_router.post("/get-recommendations")
def get_recommendations(payload: RecommendationTableRequest):
    username = "admin"
    # username = user.get('username', 'unknown')
    # logger.info("GET_RECOMMENDATIONS started - User: %s, Market: %s",
    #             username, payload.market)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock recommendations")
        mock_recommendations = [
            {"rule_id": 1, "no_of_queries": 100, "total_queries": 1000, 
             "sample_query": "SELECT * FROM example_table", "no_of_schemas": 2, 
             "recommendation": "Consider adding an index to improve query performance"},
            {"rule_id": 2, "no_of_queries": 50, "total_queries": 500, 
             "sample_query": "SELECT COUNT(*) FROM another_table", "no_of_schemas": 1, 
             "recommendation": "Consider partitioning this table for better performance"},
        ]
        return JSONResponse(content={"data": mock_recommendations}, status_code=200)
    
    market = payload.market
    conn = None
    
    try:
        # if market not in user["markets"]:
        #     logger.warning("ACCESS_DENIED - User %s attempted to access unauthorized market: %s", username, market)
        #     raise HTTPException(status_code=403, detail="Market access denied")
        
        logger.debug("Loading database connection params for market: %s", market)
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing query to fetch recommendations")
        # "SELECT * FROM `{RECOM_BQ_PROJECT}.{RECOM_BQ_DATASET}.rulemaster` AS RM
        # INNER JOIN `{RECOM_BQ_PROJECT}.{RECOM_BQ_DATASET}.recommendation` AS RE
        # ON RM.rule_id = RE.rule_id"
        #cur.execute("SELECT rule_id, no_of_queries, total_queries, sample_query, no_of_schemas, recommendation FROM recommendation_table")
        cur.execute(f"SELECT * FROM rule_master_vw where project_id = '{payload.project_name}'")

        rows = cur.fetchall()
        
        if rows:
            colnames = [desc[0] for desc in cur.description]
            recommendations = []
            for row in rows:
                recommendations.append(jsonable_encoder(dict(zip(colnames, row))))
            logger.info("GET_RECOMMENDATIONS completed - User: %s, Market: %s, Found %s recommendations", 
                       username, market, len(recommendations))
        else:
            logger.warning("GET_RECOMMENDATIONS - No recommendations found for User: %s, Market: %s", 
                          username, market)
            recommendations = []
        return JSONResponse(
            status_code=200,
            content={"data": recommendations}
        )
        
    except HTTPException:
        raise
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - User: %s, Market: %s, PostgreSQL Error: %s", 
                    username, market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_RECOMMENDATIONS_ERROR - User: %s, Market: %s, Error: %s", 
                        username, market, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)



@pg_router.post("/get-audit-table")
def get_audit_table(payload: AuditTableRequest, user: dict = Depends(verify_token)):
    username = user.get('username', 'unknown')
    logger.info("GET_AUDIT_TABLE started - User: %s, Market: %s, Table: %s, Column: %s", 
                username, payload.market, payload.table_name, payload.column_name)
    
    if os.getenv("TEST_MODE") == "true":
        logger.info("TEST_MODE enabled - returning mock audit response")
        mock_audit = [
            {"user_id": "mock_user1", "user_name": "Mock User 1", "action": "UPDATE", "event_time": "2023-10-27T10:00:00Z"},
            {"user_id": "mock_user2", "user_name": "Mock User 2", "action": "CREATE", "event_time": "2023-10-26T12:30:00Z"},
        ]
        return JSONResponse(content={"result": mock_audit}, status_code=200)
    
    market = payload.market
    conn = None
    
    try:
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        cur = conn.cursor()

        entity_id = None
        if payload.table_name:
            try:
                config = load_config(market)
                project_id = config.get('Database', 'bigquery_project')
                dataset_id = config.get('Database', 'bigquery_dataset')
                if payload.column_name:
                    entity_id = generate_column_id_key(project_id, dataset_id, payload.table_name, payload.column_name)
                else:
                    entity_id = generate_id_key(project_id, dataset_id, payload.table_name)
                logger.debug("Filtering audit logs for entity_id: %s", entity_id)
            except Exception as e:
                logger.error("Could not generate entity_id for audit logs: %s", str(e))

        query = "SELECT user_id, user_name, action, event_time FROM audit_logs"
        params = []
        
        if entity_id:
            query += " WHERE entity_id = %s"
            params.append(entity_id)
        
        query += " ORDER BY event_time DESC LIMIT 5"
        
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        
        audit_data = []
        if rows:
            colnames = [desc[0] for desc in cur.description]
            audit_data = [dict(zip(colnames, row)) for row in rows]
            logger.info("GET_AUDIT_TABLE completed - User: %s, Market: %s, Rows: %d", 
                       username, payload.market, len(audit_data))
        else:
            logger.warning("GET_AUDIT_TABLE - No audit records found for market: %s, entity_id: %s", market, entity_id)

        return {"result": jsonable_encoder(audit_data)}

    except Exception as e:
        logger.exception("GET_AUDIT_TABLE_ERROR - Market: %s, Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail=f"Error fetching audit logs: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)

class StatsRequest(BaseModel):
    market: str

@pg_router.post("/get-total-schemas")
def get_total_schemas(payload: StatsRequest):
    username = "admin"
    market = payload.market
    conn = None
    
    try:
        logger.debug("Loading database connection params for market: %s", market)
        
        if os.getenv("TEST_MODE") == "true":
            logger.info("TEST_MODE enabled - returning mock total schemas")
            return JSONResponse(
                status_code=200,
                content={"data": {"total_schemas": 42}}
            )
            
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing query to fetch total schemas")
        cur.execute("SELECT SUM(no_of_schemas) AS total_schemas FROM recommendation_table")
        row = cur.fetchone()
        
        if row and row[0] is not None:
            # Convert Decimal to int/float to make it JSON serializable
            total_schemas = int(row[0]) if row[0].as_integer_ratio()[1] == 1 else float(row[0])
            logger.info("GET_TOTAL_SCHEMAS completed - Market: %s, Total schemas: %s", 
                       market, total_schemas)
        else:
            logger.warning("GET_TOTAL_SCHEMAS - No data found for Market: %s", market)
            total_schemas = 0

        return JSONResponse(
            status_code=200,
            content={"data": {"total_schemas": total_schemas}}
        )
        
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - Market: %s, PostgreSQL Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_TOTAL_SCHEMAS_ERROR - Market: %s, Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)

@pg_router.post("/get-total-queries")
def get_total_queries(payload: StatsRequest):
    username = "admin"
    market = payload.market
    conn = None
    
    try:
        logger.debug("Loading database connection params for market: %s", market)
        
        if os.getenv("TEST_MODE") == "true":
            logger.info("TEST_MODE enabled - returning mock total queries")
            return JSONResponse(
                status_code=200,
                content={"data": {"total_queries": 1000}}
            )
            
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing query to fetch total queries")
        cur.execute("SELECT SUM(no_of_queries) AS total_queries FROM recommendation_table")
        row = cur.fetchone()
        
        if row and row[0] is not None:
            # Convert Decimal to int/float to make it JSON serializable
            total_queries = int(row[0]) if row[0].as_integer_ratio()[1] == 1 else float(row[0])
            logger.info("GET_TOTAL_QUERIES completed - Market: %s, Total queries: %s", 
                       market, total_queries)
        else:
            logger.warning("GET_TOTAL_QUERIES - No data found for Market: %s", market)
            total_queries = 0

        return JSONResponse(
            status_code=200,
            content={"data": {"total_queries": total_queries}}
        )
        
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - Market: %s, PostgreSQL Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_TOTAL_QUERIES_ERROR - Market: %s, Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)

@pg_router.post("/get-total-query-scanned")
def get_total_query_scanned(payload: StatsRequest):
    username = "admin"
    market = payload.market
    conn = None
    
    try:
        logger.debug("Loading database connection params for market: %s", market)
        
        if os.getenv("TEST_MODE") == "true":
            logger.info("TEST_MODE enabled - returning mock total query scanned")
            return JSONResponse(
                status_code=200,
                content={"data": {"total_query_scanned": 5000}}
            )
            
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        
        logger.debug("Connecting to PostgreSQL - Host: %s, Database: %s", conn, connector)
        cur = conn.cursor()
        logger.debug("Executing query to fetch total query scanned")
        cur.execute("SELECT total_queries AS total_query_scanned FROM recommendation_table LIMIT 1")
        row = cur.fetchone()
        
        if row and row[0] is not None:
            # Convert Decimal to int/float to make it JSON serializable
            total_query_scanned = int(row[0]) if row[0].as_integer_ratio()[1] == 1 else float(row[0])
            logger.info("GET_TOTAL_QUERY_SCANNED completed - Market: %s, Total query scanned: %s", 
                       market, total_query_scanned)
        else:
            logger.warning("GET_TOTAL_QUERY_SCANNED - No data found for Market: %s", market)
            total_query_scanned = 0

        return JSONResponse(
            status_code=200,
            content={"data": {"total_query_scanned": total_query_scanned}}
        )
        
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - Market: %s, PostgreSQL Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("GET_TOTAL_QUERY_SCANNED_ERROR - Market: %s, Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)


class RuleIdPayload(BaseModel):
    rule_id: str
    market: str
    page: Optional[int] = 1
    page_size: Optional[int] = 10


@pg_router.post("/rule")
def rule_id(payload: RuleIdPayload):
    logger.info("RULE_ID started - Rule ID: %s, Market: %s", payload.rule_id, payload.market)
    market = payload.market
    conn = None
    ruleId = payload.rule_id
    page = payload.page or 1
    page_size = payload.page_size or 10

    # Map ruleId to table name
    rule_table_map = {
        "1": "rule_case_insensitive_comparison",
        "2": "rule_delete_with_true",
        "3": "rule_in_clause_has_constants",
        "4": "rule_in_clause_has_subquery",
        "5": "rule_jobs_failing_very_frequently",
        "6": "rule_jobs_failing_very_frequently_due_to_resource_error",
        "7": "rule_jobs_scanning_high_volume_of_data",
        "8": "rule_multiple_updates_on_a_single_table",
        "9": "rule_order_by_inside_subquery",
        "10": "rule_same_table_multiple_schemas",
        "11": "rule_table_clone"
    }

    table_name = rule_table_map.get(ruleId)
    if not table_name:
        raise HTTPException(status_code=400, detail="Invalid rule_id")

    try:
        db_util = GoogleCloudSqlUtility(market)
        conn, connector = db_util.get_db_connection()
        cur = conn.cursor()

        # Get total count
        count_sql = f"SELECT COUNT(*) FROM {table_name}"
        cur.execute(count_sql)
        total_count = cur.fetchone()[0]

        # Get paginated data
        offset = (page - 1) * page_size
        data_sql = f"SELECT * FROM {table_name} OFFSET {offset} LIMIT {page_size}"
        cur.execute(data_sql)
        rows = cur.fetchall()

        if rows:
            colnames = [desc[0] for desc in cur.description]
            rule_data = [dict(zip(colnames, row)) for row in rows]
            logger.info("RULE_ID completed - Rule ID: %s, Market: %s, Rows: %d",
                        payload.rule_id, payload.market, len(rule_data))
        else:
            logger.warning("RULE_ID - No data found for Rule ID: %s, Market: %s", payload.rule_id, payload.market)
            rule_data = []

        return JSONResponse(content={
            "data": jsonable_encoder(rule_data),
            "total_count": total_count
        }, status_code=200)

    except HTTPException:
        raise
    except psycopg2.Error as e:
        logger.error("DATABASE_ERROR - Market: %s, PostgreSQL Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
    except Exception as e:
        logger.exception("RULE_ID_ERROR - Rule ID: %s, Market: %s, Error: %s",
                         payload.rule_id, market, str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Database connection closed for market: %s", market)