"""
BigQuery cost estimation functions using DryRun.
Enhanced with comprehensive logging for debugging.
"""
from google.cloud import bigquery
from datetime import datetime
import json
import os

from src.services.bq_client import get_bigquery_client
from src.services.cost_utils import calculate_query_cost, format_bytes
from src.utils.logger import logger

class Estimate:
    def __init__(self, query, market):
        self.market = market
        self.query = query
        logger.info(f"Initialized Estimate for market: {market}")
        logger.debug(f"Query to estimate: {query[:100]}{'...' if len(query) > 100 else ''}")

    def estimate_query_cost(self):
        """
        Estimate the cost of a BigQuery SQL query without executing it.

        Args:
            query (str): SQL query to estimate

        Returns:
            dict: Cost estimate information
        """
        logger.info(f"Starting cost estimation for market: {self.market}")
        
        try:
            # Get BigQuery client
            logger.debug("Retrieving BigQuery client")
            client, project_id, dataset_id = get_bigquery_client(self.market)
            logger.info(f"Connected to project: {project_id}, dataset: {dataset_id}")

            # Configure job for dry run
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
                default_dataset=f"{project_id}.{dataset_id}"
            )
            logger.debug("Configured dry run job")

            # Run the query as a dry run
            start_time = datetime.now()
            logger.debug("Executing dry run query")
            query_job = client.query(self.query, job_config=job_config)
            end_time = datetime.now()

            # Calculate elapsed time
            elapsed_ms = (end_time - start_time).total_seconds() * 1000
            logger.debug(f"Dry run completed in {elapsed_ms:.2f}ms")

            # Get bytes that would be processed
            bytes_processed = query_job.total_bytes_processed or 0
            logger.info(f"Query would process {format_bytes(bytes_processed)} ({bytes_processed} bytes)")

            # Calculate cost
            logger.debug("Calculating query cost")
            cost_info = calculate_query_cost(bytes_processed)

            # Add additional information
            cost_info.update({
                'query': self.query,
                'elapsed_ms': elapsed_ms,
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            })

            logger.info(f"Cost estimation successful: ${cost_info.get('estimated_cost_usd', 0):.6f} USD")

            # Save to history file
            try:
                self.save_estimate_to_history(cost_info)
                logger.debug("Cost estimate saved to history")
            except Exception as history_error:
                logger.warning(f"Failed to save estimate to history: {history_error}")

            return cost_info

        except Exception as e:
            logger.error(f"Cost estimation failed: {str(e)}", exc_info=True)
            error_response = {
                'status': 'error',
                'error_message': str(e),
                'query': self.query,
                'timestamp': datetime.now().isoformat()
            }
            return error_response

    def compare_queries_cost(self, original_query, optimized_query):
        """
        Compare the cost of two queries (original and optimized).

        Args:
            original_query (str): Original SQL query
            optimized_query (str): Optimized SQL query

        Returns:
            dict: Comparison of cost estimates
        """
        logger.info("Starting query cost comparison")
        logger.debug(f"Original query length: {len(original_query)} chars")
        logger.debug(f"Optimized query length: {len(optimized_query)} chars")

        try:
            # Estimate cost of original query
            logger.debug("Estimating original query cost")
            original_estimate = Estimate(original_query, self.market).estimate_query_cost()

            # Estimate cost of optimized query
            logger.debug("Estimating optimized query cost")
            optimized_estimate = Estimate(optimized_query, self.market).estimate_query_cost()

            # Calculate savings
            if original_estimate['status'] == 'success' and optimized_estimate['status'] == 'success':
                bytes_saved = original_estimate['bytes_processed'] - optimized_estimate['bytes_processed']
                cost_saved = original_estimate['estimated_cost_usd'] - optimized_estimate['estimated_cost_usd']

                # Calculate percentage saved
                if original_estimate['bytes_processed'] > 0:
                    percentage_saved = (bytes_saved / original_estimate['bytes_processed']) * 100
                else:
                    percentage_saved = 0

                logger.info(f"Query optimization results: {percentage_saved:.2f}% savings, ${cost_saved:.6f} USD saved")
                logger.info(f"Bytes saved: {format_bytes(bytes_saved)}")

                return {
                    'original_query': original_query,
                    'original_cost': original_estimate,
                    'optimized_query': optimized_query,
                    'optimized_cost': optimized_estimate,
                    'bytes_saved': bytes_saved,
                    'formatted_bytes_saved': format_bytes(bytes_saved),
                    'cost_saved_usd': round(cost_saved, 6),
                    'percentage_saved': round(percentage_saved, 2),
                    'status': 'success',
                    'timestamp': datetime.now().isoformat()
                }
            else:
                # Handle case where one or both queries failed
                logger.error("Query comparison failed - one or both estimates failed")
                if original_estimate['status'] != 'success':
                    logger.error(f"Original query estimation failed: {original_estimate.get('error_message', 'Unknown error')}")
                if optimized_estimate['status'] != 'success':
                    logger.error(f"Optimized query estimation failed: {optimized_estimate.get('error_message', 'Unknown error')}")

                return {
                    'original_query': original_query,
                    'original_cost': original_estimate,
                    'optimized_query': optimized_query,
                    'optimized_cost': optimized_estimate,
                    'status': 'error',
                    'error_message': 'One or both queries failed to estimate cost',
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Query comparison failed with exception: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'error_message': f'Query comparison failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }

    def get_query_execution_plan(self):
        """
        Get the execution plan for a query using EXPLAIN.

        Args:
            query (str): SQL query to analyze

        Returns:
            dict: Query execution plan and cost estimate
        """
        logger.info("Getting query execution plan")
        
        try:
            # Get BigQuery client
            logger.debug("Retrieving BigQuery client for execution plan")
            client, project_id, dataset_id = get_bigquery_client(self.market)

            # Create EXPLAIN query
            explain_query = f"EXPLAIN PLAN FOR {self.query}"
            logger.debug(f"Generated EXPLAIN query: {explain_query[:100]}{'...' if len(explain_query) > 100 else ''}")

            # Run the EXPLAIN query (this isn't a dry run)
            job_config = bigquery.QueryJobConfig(
                default_dataset=f"{project_id}.{dataset_id}"
            )
            
            logger.debug("Executing EXPLAIN query")
            explain_job = client.query(explain_query, job_config=job_config)
            explain_results = list(explain_job)
            logger.info(f"Execution plan retrieved with {len(explain_results)} steps")

            # Also get the cost estimate
            logger.debug("Getting cost estimate alongside execution plan")
            estimate = self.estimate_query_cost()

            # Combine results
            plan_info = {
                'query': self.query,
                'execution_plan': [dict(row) for row in explain_results],
                'cost_estimate': estimate,
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            }

            logger.info("Query execution plan analysis completed successfully")
            return plan_info

        except Exception as e:
            logger.error(f"Failed to get query execution plan: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'error_message': str(e),
                'query': self.query,
                'timestamp': datetime.now().isoformat()
            }

    def save_estimate_to_history(self, estimate_data):
        """
        Save a cost estimate to the history file.

        Args:
            estimate_data (dict): Cost estimate data
        """
        history_file = 'cost_estimate_history.json'
        logger.debug(f"Saving estimate to history file: {history_file}")

        try:
            # Load existing history
            history = []
            if os.path.exists(history_file):
                logger.debug("Loading existing history file")
                with open(history_file, 'r') as f:
                    try:
                        history = json.load(f)
                        logger.debug(f"Loaded {len(history)} existing history entries")
                    except json.JSONDecodeError as json_error:
                        logger.warning(f"History file corrupted, starting fresh: {json_error}")
                        history = []
            else:
                logger.debug("History file doesn't exist, creating new one")

            # Add new estimate
            history.append(estimate_data)
            logger.debug(f"Added new estimate to history, total entries: {len(history)}")

            # Save updated history
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
            logger.debug("Successfully saved estimate to history")

        except Exception as e:
            logger.error(f"Error saving estimate to history: {e}", exc_info=True)
            raise  # Re-raise to allow caller to handle

    def get_estimate_history(self, limit=10):
        """
        Get recent cost estimate history.

        Args:
            limit (int): Maximum number of history entries to return

        Returns:
            list: Recent cost estimate history
        """
        history_file = 'cost_estimate_history.json'
        logger.debug(f"Retrieving estimate history from {history_file}, limit: {limit}")

        try:
            # Load history
            history = []
            if os.path.exists(history_file):
                with open(history_file, 'r') as f:
                    try:
                        history = json.load(f)
                        logger.debug(f"Loaded {len(history)} history entries")
                    except json.JSONDecodeError as json_error:
                        logger.error(f"Failed to parse history file: {json_error}")
                        history = []
            else:
                logger.info("No history file found, returning empty history")

            # Sort by timestamp (newest first)
            history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

            # Limit results
            limited_history = history[:limit]
            logger.info(f"Returning {len(limited_history)} history entries")
            
            return limited_history

        except Exception as e:
            logger.error(f"Error getting estimate history: {e}", exc_info=True)
            return []