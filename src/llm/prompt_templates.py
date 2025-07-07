def get_prompt_for_analytical_intent(query: str):
    prompt = f'''Following query is the analytical query or not: 
                {query}
                Just print True or False and nothing else'''
    return prompt

def get_prompt_for_generating_sql_query(tables: str, schema: str, content: str):
    prompt = f'''
    ### Table Details
    The following are the table descriptions and join keys, separated by '|':
    
    TABLES: {tables}

    ### Column Schema
    Below are the matched table-column mappings, also separated by '|':

    MATCHED_SCHEMA: {schema}

    ### User Request
    Write a BigQuery SQL query that fulfills this requirement:

    {content}

    ### Rules and Guidelines (Follow all strictly)
    1. Use **only** column names listed in the MATCHED_SCHEMA section.
    2. Associate each column strictly with its corresponding table from MATCHED_SCHEMA.
    3. If the user prompt matches any known prompt examples, return the corresponding sample SQL query.
    4. Use `SAFE_DIVIDE`, `SAFE_CAST` where applicable.
    5. Use descriptive aliases for all column names in the SELECT clause, especially when:
       - The column name is long or complex
       - The column is a calculated field
       - Multiple columns from different tables have similar names
       Example: `SELECT u.user_id AS user_identifier, o.order_date AS purchase_date`
    6. Always use the full column reference (table.column or table_alias.column) in GROUP BY and ORDER BY clauses
    7. Use `UNNEST` when dealing with STRUCT or ARRAY<STRUCT> fields.
    8. Ensure clean formatting and consistent indentation.
    8. Use `GROUP BY` if the query includes aggregation functions (e.g., `COUNT`).
    9. Do **not** include GCP project IDs or dataset references in the output.
    10. Optimize the query for performance:
        - Use efficient joins and indexed columns.
        - Avoid redundant calculations or unnecessary casting.
    11. Remove `CAST()` where not needed.
    12. Use BigQuery functions based on data type:
        - For timestamps, use `TIMESTAMP_SUB` or `DATE_ADD` with `INTERVAL ... DAY`.
        - Convert months to days using a 30-day month approximation.
    13. Give the priority to return the values of the column provided in context in 'sample_values' section or else return the most similar with sample values given.
    14. If "columns_metadata" has "mode": "REPEATED", like "ids", use the provided sql_patterns. For repeated fields, never use direct dot notation. Always use UNNEST for repeated field access
    
    ### Output Format Rules (STRICT)
    - The output must be exactly in this format:
      Optimised Query:-SELECT * FROM table
    - Do NOT include any markdown formatting (```sql or ```)
    - Do NOT include any additional text, explanations, or formatting
    - The output must:
      * Start with "Optimised Query:-"
      * Follow immediately with the SQL query
      * Contain no other text before or after
    - Example of correct output:
      Optimised Query:-SELECT COUNT(*) FROM users WHERE status = 'active'
    - Example of INCORRECT output:
      ```sql
      SELECT COUNT(*) FROM users WHERE status = 'active'
      ```
      Or any output that includes markdown formatting or additional text.

    Respond with ONLY the SQL query in the exact format specified above.
    '''
    return prompt

def get_prompt_verify_sql_injection(sql_query: str):
    prompt = f"""
        You are a cybersecurity expert specializing in SQL injection detection.  
        Analyze the given SQL query and determine if it **has SQL injection intent**.  

        **Rules for detection:**  
        - Query contains patterns like `OR 1=1`, `UNION SELECT`, `--`, `DROP TABLE`, etc.  
        - Use of **string concatenation** that modifies query logic dynamically.  
        - **Stacked queries** (`;` multiple statements in one query).  
        - **Unusual use of quotes or comments** that might escape intended query structure.  

        **Return only `True` or `False` (without any explanation):**  
        - **`True`** → If SQL injection risk is detected.  
        - **`False`** → If no SQL injection risk is detected.  

        **Analyze this SQL query and return only True or False:**
        {sql_query}
    """
    return prompt
  
def get_prompt_verify_invalid_domain_query(content: str):
    prompt = f"""
      You are a classifier that determines whether an SQL query is related to the banking or financial domain.

      Instructions:
      Return True if the input SQL query is associated with banking or finance. Otherwise, return False.

      Consider the query to be financial or banking related if it contains:
      Keywords or references such as:
      - Entities: bank, account, customer, user, client
      - Actions/Operations: deposit, withdraw, transfer, payment, transaction, statement, audit
      - Financial Terms: balance, loan, mortgage, interest rate, credit, debit, savings, checking, investment, insurance, capital, budget, tax, currency, fund, ATM
      - General: financial, finance

      Be case-insensitive and look for both exact keywords and domain-specific intent.

      Input SQL Query:
      {content}

      Output:
      A single boolean value: True or False
    """
    return prompt  
