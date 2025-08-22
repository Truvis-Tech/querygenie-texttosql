import json
from typing import Any, Dict

def generate_llm_prompt(
    sql_query: str, 
    table_context: Any, 
    query_plan_info: Any, 
    model: str = "gpt-4"
) -> str:
    """
    Generates a detailed prompt for the LLM to optimize a BigQuery SQL query.

    Args:
        sql_query: The original SQL query.
        table_context: JSON-serializable object with schema and metadata of referenced tables.
        query_plan_info: JSON-serializable object with the dry-run analysis from BigQuery.
        model: The LLM model being used.

    Returns:
        A formatted string to be used as the prompt.
    """
    table_context_str = json.dumps(table_context, indent=2)
    query_plan_str = json.dumps(query_plan_info, indent=2)

    prompt = f"""
You are an expert Google BigQuery SQL optimizer for the {model} model.

Your task is to analyze a SQL query, its table context, and its execution plan to provide actionable optimizations. Please identify bottlenecks, rewrite the SQL for better performance and cost-efficiency, and suggest relevant schema changes.

Respond ONLY with a JSON object inside a Markdown block (```json ... ```) with the following structure:

{{
  "bottlenecks": [
    "A list of identified performance issues (e.g., 'Full table scan on large table X', 'Inefficient JOIN condition between A and B')."
  ],
  "optimized_sql": "The rewritten, optimized SQL query.",
  "schema_recommendations": [
    "A list of suggested schema changes (e.g., 'Partition table Y by date field', 'Cluster table Z by column C')."
  ],
  "estimated_cost_savings_usd": "A dollar amount representing the estimated cost savings (e.g., '$5.25').",
  "justification": "A brief explanation of why the optimized query is better, linking the changes to the identified bottlenecks."
}}

--- INPUTS ---

## Original SQL Query:

```sql
{sql_query}
```

## Table and Column Context:

```json
{table_context_str}
```

## Query Execution Plan Analysis:

```json
{query_plan_str}
```

Provide your analysis based *only* on the information given.
"""
    return prompt.strip()
