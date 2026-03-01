import logging
from typing import Dict, Any, List
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

logger = logging.getLogger(__name__)

def get_query_plan_dry_run(query: str, client: bigquery.Client) -> Dict[str, Any]:
    """
    Get comprehensive query plan using BigQuery dry run for LLM optimization.
    This method provides detailed analysis WITHOUT executing the query or incurring costs.
    """
    if not query.strip():
        raise ValueError("Query cannot be empty")
    
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(query, job_config=job_config)
        
        plan_info = {
            "validation_status": "valid",
            "query_metadata": {
                "total_bytes_processed": query_job.total_bytes_processed,
                "total_bytes_billed": query_job.total_bytes_billed,
                "cache_hit": query_job.cache_hit,
                "referenced_tables": [str(ref) for ref in query_job.referenced_tables] if query_job.referenced_tables else [],
                "statement_type": getattr(query_job, 'statement_type', 'SELECT'),
                "estimated_cost_usd": _calculate_estimated_cost(query_job.total_bytes_processed),
            },
            "optimization_context": {
                "full_table_scan_likely": _detect_full_table_scan(query),
                "join_complexity": _analyze_join_complexity(query),
                "aggregation_present": _detect_aggregations(query)
            }
        }
        
        if hasattr(query_job, 'query_plan') and query_job.query_plan:
            plan_info["execution_stages"] = _extract_execution_stages(query_job.query_plan)
        
        return plan_info
        
    except BadRequest as e:
        return {
            "validation_status": "invalid",
            "error": str(e),
            "error_type": "syntax_error",
        }
    except Exception as e:
        logger.error(f"Error getting query plan: {e}")
        return {
            "validation_status": "error",
            "error": str(e),
            "error_type": "execution_error",
        }

def _calculate_estimated_cost(bytes_processed: int) -> float:
    """Calculate estimated query cost based on bytes processed ($5 per TB)."""
    if bytes_processed:
        tb_processed = bytes_processed / (1024**4)
        return round(tb_processed * 5, 4)
    return 0.0

def _extract_execution_stages(query_plan) -> List[Dict[str, Any]]:
    """Extract detailed execution stages from query plan."""
    stages = []
    for stage in query_plan:
        stage_info = {
            "stage_id": getattr(stage, 'id', 'unknown'),
            "name": getattr(stage, 'name', 'unknown'),
            "status": getattr(stage, 'status', 'unknown'),
            "records_read": getattr(stage, 'records_read', 0),
            "records_written": getattr(stage, 'records_written', 0),
            "steps": []
        }
        if hasattr(stage, 'steps') and stage.steps:
            for step in stage.steps:
                step_info = {
                    "kind": getattr(step, 'kind', 'unknown'),
                    "substeps": list(getattr(step, 'substeps', []))
                }
                stage_info["steps"].append(step_info)
        stages.append(stage_info)
    return stages

def _detect_full_table_scan(query: str) -> bool:
    """Detect if query likely performs full table scan."""
    query_lower = query.lower()
    has_where = 'where' in query_lower
    has_limit = 'limit' in query_lower
    has_select_star = 'select *' in query_lower.replace(' ', '')
    return not has_where and has_select_star and not has_limit

def _analyze_join_complexity(query: str) -> Dict[str, Any]:
    """Analyze JOIN complexity in the query."""
    query_lower = query.lower()
    join_types = ['inner join', 'left join', 'right join', 'full join', 'cross join']
    join_count = sum(query_lower.count(join_type) for join_type in join_types)
    return {
        "join_count": join_count,
        "complexity": "low" if join_count <= 1 else "medium" if join_count <= 3 else "high",
        "has_cross_join": 'cross join' in query_lower
    }

def _detect_aggregations(query: str) -> bool:
    """Detect if query contains aggregation functions."""
    query_lower = query.lower()
    agg_functions = ['count(', 'sum(', 'avg(', 'min(', 'max(', 'group by']
    return any(func in query_lower for func in agg_functions)
