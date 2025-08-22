import logging
from typing import Dict, Any
from google.cloud import bigquery

from src.services.bq_metadata_extractor import BigQueryMetadataExtractor
from src.services.bq_query_analyzer import get_query_plan_dry_run
from src.services.llm_service import call_llm
from src.utils.prompt_generator import generate_llm_prompt
from src.services.bq_client import get_bigquery_client

logger = logging.getLogger(__name__)

class SqlOptimisationService:
    """
    A service to orchestrate the SQL optimization process.
    """

    def __init__(self, market: str, llm_type: str):
        """
        Initializes the service.

        Args:
            market: The market context (e.g., 'US') to configure the BigQuery client.
            llm_type: The type of LLM to use (e.g., 'gpt-4').
        """
        self.market = market
        self.llm_type = llm_type
        # get_bigquery_client returns: (client, project_id, dataset_id, location)
        self.bq_client, self.project_id, self.dataset_id, self.location = get_bigquery_client(market)
        self.metadata_extractor = BigQueryMetadataExtractor(self.bq_client)

    def optimise_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Performs the end-to-end SQL optimization process.

        Args:
            sql_query: The SQL query to be optimized.

        Returns:
            A dictionary containing the optimization suggestions from the LLM.
        """
        try:
            # 1. Get query plan analysis from BigQuery
            logger.info("Step 1: Analyzing query plan...")
            plan_data = get_query_plan_dry_run(sql_query, self.bq_client)
            if plan_data['validation_status'] != 'valid':
                logger.error(f"Invalid query or analysis failed: {plan_data.get('error')}")
                return {"error": "Query is invalid or analysis failed", "details": plan_data}

            # 2. Extract table and column names from the query
            logger.info("Step 2: Extracting tables and columns from SQL...")
            # We get the referenced tables directly from the plan data, which is more reliable.
            referenced_tables = plan_data.get("query_metadata", {}).get("referenced_tables", [])
            if not referenced_tables:
                logger.warning("Could not identify referenced tables.")
                return {"error": "Could not identify tables referenced in the query."}

            # 3. Fetch metadata for the identified tables
            logger.info(f"Step 3: Fetching metadata for tables: {referenced_tables}")
            table_context = self.metadata_extractor.get_metadata_for_tables(referenced_tables)
            if not table_context:
                logger.warning("Could not fetch metadata for tables.")
                return {"error": "Failed to fetch metadata for the required tables."}

            # 4. Generate the prompt for the LLM
            logger.info("Step 4: Generating LLM prompt...")
            prompt = generate_llm_prompt(
                sql_query=sql_query,
                table_context=table_context,
                query_plan_info=plan_data,
                model=self.llm_type
            )

            # 5. Call the LLM to get optimization suggestions
            logger.info("Step 5: Calling LLM for optimization...")
            # This assumes llm_config is handled within call_llm or sourced from a central config
            # For this example, we'll construct a minimal one.
            llm_config = {"model": self.llm_type}
            llm_response = call_llm(prompt, llm_config)

            logger.info("Optimization process completed successfully.")
            return llm_response

        except Exception as e:
            logger.exception("An unexpected error occurred during the optimization process.")
            return {"error": str(e)}
