# import logging
# from typing import Dict, Any
# from google.cloud import bigquery

# from src.services.bq_metadata_extractor import BigQueryMetadataExtractor
# from src.services.bq_query_analyzer import get_query_plan_dry_run
# from src.services.llm_service import call_llm
# from src.utils.prompt_generator import generate_llm_prompt
# from src.services.bq_client import get_bigquery_client
# from src.utils.sql_parser2 import extract_table_columns_from_query
# from src.utils.static_metadata_reader import StaticMetadataReader

# logger = logging.getLogger(__name__)

# class SqlOptimisationService:
#     """
#     A service to orchestrate the SQL optimization process.
#     """

#     def __init__(self, market: str, llm_type: str):
#         """
#         Initializes the service.

#         Args:
#             market: The market context (e.g., 'US') to configure the BigQuery client.
#             llm_type: The type of LLM to use (e.g., 'gpt-4').
#         """
#         self.market = market
#         self.llm_type = llm_type
#         # get_bigquery_client returns: (client, project_id, dataset_id, location)
#         self.bq_client, self.project_id, self.dataset_id, self.location = get_bigquery_client(market)
#         # self.metadata_extractor = BigQueryMetadataExtractor(self.bq_client)
#         self.metadata_extractor = StaticMetadataReader("static_new.json")

#     def optimise_sql_query(self, sql_query: str) -> Dict[str, Any]:
#         """
#         Performs the end-to-end SQL optimization process.

#         Args:
#             sql_query: The SQL query to be optimized.

#         Returns:
#             A dictionary containing the optimization suggestions from the LLM.
#         """
#         try:
#             # 1. Get query plan analysis from BigQuery
#             logger.info("Step 1: Analyzing query plan...")
#             plan_data = get_query_plan_dry_run(sql_query, self.bq_client)
#             logger.info(f"Query plan analysis: {plan_data}")
#             if plan_data['validation_status'] != 'valid':
#                 logger.error(f"Invalid query or analysis failed: {plan_data.get('error')}")
#                 return {"error": "Query is invalid or analysis failed", "details": plan_data}

#             # 2. Extract table and column names from the query using sql_parser
#             logger.info("Step 2: Extracting tables and columns from SQL using sql_parser...")
            
#             # Extract tables and columns using sql_parser
#             table_columns = extract_table_columns_from_query(sql_query)
#             logger.info(f"Extracted tables and columns: {table_columns}")
#             if not table_columns:
#                 logger.warning("Could not extract any tables or columns from the query.")
#                 return {"error": "Could not parse the SQL query to identify tables and columns."}
#             # Convert the table_columns dictionary to the expected format
#             referenced_tables = list(table_columns.keys())
#             # Flatten the list of columns from all tables
#             referenced_columns = [col for cols in table_columns.values() for col in cols]
            
#             if not referenced_tables:
#                 logger.warning("Could not identify referenced tables.")
#                 return {"error": "Could not identify tables referenced in the query."}
#             logger.info(f"Extracted {len(referenced_tables)} tables and {len(referenced_columns)} columns from the query.")

#             # 3. Fetch metadata for the identified tables with column filtering
#             logger.info(f"Step 3: Fetching metadata for tables with column filtering: {referenced_tables}")
            
#             # Get metadata with column filtering
#             table_context = self.metadata_extractor.get_metadata_for_tables(
#                 table_names=referenced_tables,
#                 table_columns=table_columns
#             )
#             logger.info(f"Fetched metadata : {table_context}")
#             if not table_context:
#                 logger.warning("No metadata available for the specified tables and columns.")
#                 return {"error": "No metadata available for the referenced tables and columns."}

#             # 4. Generate the prompt for the LLM
#             logger.info("Step 4: Generating LLM prompt...")
#             prompt = generate_llm_prompt(
#                 sql_query=sql_query,
#                 table_context=table_context,
#                 query_plan_info=plan_data,
#                 model=self.llm_type
#             )
#             # print(prompt)

#             # 5. Call the LLM to get optimization suggestions
#             logger.info("Step 5: Calling LLM for optimization...")
#             # This assumes llm_config is handled within call_llm or sourced from a central config
#             # For this example, we'll construct a minimal one.
#             llm_config = {"model": self.llm_type}
#             llm_response = call_llm(prompt, llm_config)
#             if llm_response.get("error"):
#                 logger.error("LLM call failed: %s", llm_response["error"])
#                 return {"error": "LLM call failed"}
#             else :
#                 logger.info("Optimization process completed successfully.")
#                 return llm_response

#         except Exception as e:
#             logger.exception("An unexpected error occurred during the optimization process.")
#             return {"error": str(e)}


import logging
from typing import Dict, Any
from google.cloud import bigquery

from src.services.bq_metadata_extractor import BigQueryMetadataExtractor
from src.services.bq_query_analyzer import get_query_plan_dry_run
from src.services.llm_service import call_llm
from src.utils.prompt_generator import generate_llm_prompt
from src.services.bq_client import get_bigquery_client
from src.utils.sql_parser2 import extract_table_columns_from_query
from src.utils.postgres_metadata_reader import PostgresMetadataReader

logger = logging.getLogger(__name__)

class SqlOptimisationService:
    """
    A service to orchestrate the SQL optimization process using PostgreSQL-backed metadata.
    """

    def __init__(self, market: str, llm_type: str, db_config: Dict[str, str] = None):
        """
        Initializes the service.

        Args:
            market: The market context (e.g., 'US') to configure the BigQuery client.
            llm_type: The type of LLM to use (e.g., 'gpt-4').
            db_config: PostgreSQL database configuration dict with keys:
                      host, port, user, password, database
                      If None, falls back to default configuration.
        """
        self.market = market
        self.llm_type = llm_type
        
        # get_bigquery_client returns: (client, project_id, dataset_id, location)
        self.bq_client, self.project_id, self.dataset_id, self.location = get_bigquery_client(market)
        
        # Initialize PostgresMetadataReader with database configuration
        if db_config is None:
            # Default configuration - update as needed for your environment
            db_config = {
                "host": "192.168.1.10",
                "port": 5432,
                "user": "postgres", 
                "password": "PGTruvis@2025",
                "database": "postgres"
            }
        
        # Replace StaticMetadataReader with PostgresMetadataReader
        self.metadata_extractor = PostgresMetadataReader(db_config)
        logger.info(f"Initialized SQL optimization service with PostgreSQL metadata backend")

    def optimise_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Performs the end-to-end SQL optimization process using PostgreSQL-backed metadata.

        Args:
            sql_query: The SQL query to be optimized.

        Returns:
            A dictionary containing the optimization suggestions from the LLM.
        """
        try:
            # 1. Get query plan analysis from BigQuery
            logger.info("Step 1: Analyzing query plan...")
            plan_data = get_query_plan_dry_run(sql_query, self.bq_client)
            logger.info(f"Query plan analysis: {plan_data}")
            if plan_data['validation_status'] != 'valid':
                logger.error(f"Invalid query or analysis failed: {plan_data.get('error')}")
                return {"error": "Query is invalid or analysis failed", "details": plan_data}

            # 2. Extract table and column names from the query using sql_parser
            logger.info("Step 2: Extracting tables and columns from SQL using sql_parser...")
            
            # Extract tables and columns using sql_parser
            table_columns = extract_table_columns_from_query(
                sql_query,
                default_project=self.project_id,
                default_dataset=self.dataset_id
            )
            logger.info(f"Extracted tables and columns: {table_columns}")
            if not table_columns:
                logger.warning("Could not extract any tables or columns from the query.")
                return {"error": "Could not parse the SQL query to identify tables and columns."}
                
            # Convert the table_columns dictionary to the expected format
            referenced_tables = list(table_columns.keys())
            # Flatten the list of columns from all tables
            referenced_columns = [col for cols in table_columns.values() for col in cols]
            
            if not referenced_tables:
                logger.warning("Could not identify referenced tables.")
                return {"error": "Could not identify tables referenced in the query."}
            logger.info(f"Extracted {len(referenced_tables)} tables and {len(referenced_columns)} columns from the query.")

            # 3. Fetch metadata for the identified tables with column filtering FROM POSTGRESQL
            logger.info(f"Step 3: Fetching metadata from PostgreSQL for tables with column filtering: {referenced_tables}")
            
            # Get metadata with column filtering using PostgresMetadataReader
            table_context = self.metadata_extractor.get_metadata_for_tables(
                table_names=referenced_tables,
                table_columns=table_columns
            )
            logger.info(f"Fetched metadata from PostgreSQL: {len(table_context)} tables")
            if not table_context:
                logger.warning("No metadata available for the specified tables and columns.")
                return {"error": "No metadata available for the referenced tables and columns."}

            # 4. Generate the prompt for the LLM
            logger.info("Step 4: Generating LLM prompt...")
            prompt = generate_llm_prompt(
                sql_query=sql_query,
                table_context=table_context,
                query_plan_info=plan_data,
                model=self.llm_type
            )
            # print(prompt)

            # 5. Call the LLM to get optimization suggestions
            logger.info("Step 5: Calling LLM for optimization...")
            # This assumes llm_config is handled within call_llm or sourced from a central config
            # For this example, we'll construct a minimal one.
            llm_config = {"model": self.llm_type}
            llm_response = call_llm(prompt, llm_config)
            if llm_response.get("error"):
                logger.error("LLM call failed: %s", llm_response["error"])
                return {"error": "LLM call failed"}
            else:
                logger.info("Optimization process completed successfully using PostgreSQL metadata.")
                return llm_response

        except Exception as e:
            logger.exception("An unexpected error occurred during the optimization process.")
            return {"error": str(e)}

    # def get_metadata_statistics(self) -> Dict[str, Any]:
    #     """
    #     Get statistics about the metadata store for monitoring/debugging.
        
    #     Returns:
    #         Dictionary with metadata statistics from PostgreSQL.
    #     """
    #     try:
    #         return self.metadata_extractor.get_statistics()
    #     except Exception as e:
    #         logger.error(f"Error getting metadata statistics: {e}")
    #         return {"error": str(e)}
