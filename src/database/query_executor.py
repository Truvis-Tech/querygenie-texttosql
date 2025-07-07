from google.cloud import bigquery
from concurrent.futures import TimeoutError
from src.services.bq_client import get_bigquery_client
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, GoogleAPIError
from src.utils.logger import logger
from datetime import datetime

def serialize_row(row):
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}

def execute(sql_query: str, market: str, timeout: int) -> list:
    logger.info("QUERY_EXECUTION_START - Market: %s", market)
    logger.debug("SQL Query: %s", sql_query[:200] + "..." if len(sql_query) > 200 else sql_query)
    
    try:
        logger.debug("Getting BigQuery client for market: %s", market)
        client, project_id, dataset_id = get_bigquery_client(market)
        logger.info("BigQuery client obtained - Project: %s, Dataset: %s", project_id, dataset_id)

        job_config = bigquery.QueryJobConfig(
            dry_run=False, 
            use_query_cache=False,
            default_dataset=f"{project_id}.{dataset_id}"
        )

        logger.debug("Submitting query to BigQuery")
        query_job = client.query(sql_query, job_config=job_config)
        
        logger.info("Query job submitted - Job ID: %s", query_job.job_id)
        results = query_job.result(timeout=timeout)

        # Convert results to list
        result_list = [serialize_row(row) for row in results]
        row_count = len(result_list)
        
        logger.info("QUERY_EXECUTION_SUCCESS - Market: %s, Rows returned: %d, Job ID: %s", 
                   market, row_count, query_job.job_id)
        
        return result_list

    except BadRequest as e:
        logger.error("BIGQUERY_BAD_REQUEST - Market: %s, Error: %s", market, e.message)
        logger.debug("SQL Query that failed: %s", sql_query)
        raise Exception(f"BadRequest in BigQuery execution: {e.message}")
    except GoogleAPIError as e:
        logger.error("BIGQUERY_API_ERROR - Market: %s, Error: %s", market, str(e))
        logger.debug("SQL Query that failed: %s", sql_query)
        raise Exception(f"GoogleAPIError: {str(e)}")
    except TimeoutError as e:
        logger.error("BIGQUERY_TIMEOUT_ERROR - Market: %s, Error: %s", market, str(e))
        logger.debug("SQL Query that failed: %s", sql_query)
        raise Exception(f"TimeoutError: {str(e)}")
    except Exception as e:
        logger.exception("QUERY_EXECUTION_CRITICAL_ERROR - Market: %s, Error: %s", market, str(e))
        logger.debug("SQL Query that failed: %s", sql_query)
        raise Exception(f"Unexpected error during query execution: {str(e)}")