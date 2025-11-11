
import google.generativeai as genai
from google.generativeai.types import GenerateContentConfig, ThinkingConfig, Part

class GeminiConfig:
    def __init__(
        self,
        model="gemini-2.5-flash-002",
        temperature=0.2,
        top_p=0.95,
        top_k=40,
        max_output_tokens=400,
        thinking_budget=512,
        project_id="YOUR_PROJECT_ID",
        location="YOUR_REGION"
    ):
        self.model = model
        self.temperature = temperature
        self.top_p = top_p
        self.top_k = top_k
        self.max_output_tokens = max_output_tokens
        self.thinking_budget = thinking_budget
        self.project_id = project_id
        self.location = location

    def client(self):
        return genai.Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
            http_options={"api_version": "v1"}
        )

    def generation_config(self):
        return GenerateContentConfig(
            temperature=self.temperature,
            top_p=self.top_p,
            top_k=self.top_k,
            max_output_tokens=self.max_output_tokens,
            thinking_config=ThinkingConfig(thinking_budget=self.thinking_budget)
        )

class PromptBuilder:
    def __init__(self, best_practices):
        self.best_practices = best_practices

    def build(self, sql_query, table_context, plan_context):
        return f"""
SYSTEM:
You are an expert enterprise Google BigQuery SQL optimizer. Your primary goal is to minimize bytes scanned and overall query cost.

Respond ONLY with a JSON object inside a Markdown block (``````) using this exact structure:
{{
  "bottlenecks": [],
  "optimized_sql": "",
  "schema_recommendations": [],
  "estimated_cost_savings_usd": "",
  "justification": ""
}}

Apply your recommendations using these prioritized best practices:
{self.best_practices}

--- INPUTS ---

## Original SQL Query:
{sql_query}

## Table and Column Context:
{table_context}

## Query Execution Plan Analysis:
{plan_context}
"""

class SQLOptimizer:
    def __init__(self, config: GeminiConfig, prompt_builder: PromptBuilder):
        self.config = config
        self.prompt_builder = prompt_builder
        self.client = self.config.client()
        self.generation_config = self.config.generation_config()

    def optimize(self, sql_query, table_context, plan_context):
        prompt = self.prompt_builder.build(sql_query, table_context, plan_context)
        response = self.client.models.generate_content(
            model=self.config.model,
            contents=[Part.from_text(prompt)],
            config=self.generation_config
        )
        return response.text

# --- Example Usage ---
if __name__ == "__main__":
    # Insert your actual best practices list here as a big string
    best_practices = """[ ... your expanded best practices here ... ]"""
    
    config = GeminiConfig(
        model="gemini-2.5-flash-002",
        temperature=0.2,
        top_p=0.95,
        top_k=40,
        max_output_tokens=400,
        thinking_budget=512,
        project_id="YOUR_PROJECT_ID",
        location="YOUR_REGION"
    )
    prompt_builder = PromptBuilder(best_practices)
    optimizer = SQLOptimizer(config, prompt_builder)

    # Use real query/context/plan values below
    sql_query = """SELECT * FROM my_table WHERE ..."""
    table_context = """[ ...table and columns JSON... ]"""
    plan_context = """{ ...BigQuery dry run/explain output JSON... }"""

    result = optimizer.optimize(sql_query, table_context, plan_context)
    print(result)
