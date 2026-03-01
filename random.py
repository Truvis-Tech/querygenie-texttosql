SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY table_schema, table_name, ordinal_position;


SELECT table_schema, table_name, table_rows
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');

{
  "market_name": "linen-striker-454116-c9",
  "llm_type": "gpt-4-0125-preview",
  "sql_query": "SELECT * FROM linen-striker-454116-c9.techsteer.event_store\nWHERE \n  CASE \n    WHEN payment_status = 'COMPLETED' THEN 1 \n    ELSE 0 \n  END = 1\n  AND \n  CASE \n    WHEN sender_transaction_amount > 500 THEN 1 \n    ELSE 0 \n  END = 1"}

curl -X 'POST' \
  'http://localhost:8000/optimize_sql_query_by_llm' \
  -H 'Content-Type: application/json' \
  -d '{
    "market_name": "US",
    "llm_type": "gpt-4",
    "sql_query": "SELECT * FROM your_table LIMIT 10"
  }'
