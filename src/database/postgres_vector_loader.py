import json
import psycopg2
from psycopg2 import sql
from src.utils.config_reader import load_config
from src.llm.embedding import Embedding

from src.database.db_connector import get_postgres_connection_params

def generate_id_key(data_source_id, data_namespace, table_name_details):
    namespace = data_namespace if data_namespace and data_namespace.strip() else "NA"
    return f"{data_source_id}~{namespace}~{table_name_details}"

def generate_column_id_key(data_source_id, data_namespace, table_name_details, column_name_details):
    namespace = data_namespace if data_namespace and data_namespace.strip() else "NA"
    return f"{data_source_id}~{namespace}~{table_name_details}~{column_name_details}"

class PostgresVectorLoader:
    def __init__(self, market):
        self.market = market
        self.host, self.port, self.database, self.user, self.password = get_postgres_connection_params(market)
        # load config and initialize embeddings client
        config = load_config(market)
        emb = Embedding(config)
        self.client = emb.get_embeddings()

    def create_embedding_table(self, table_name="context_embeddings"):
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()
            cur.execute(f"""
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id_key VARCHAR(255) PRIMARY KEY,
                    embedding VECTOR(1536)
                );
            """)
            conn.commit()
            cur.close()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"[ERROR] Creating table {table_name} failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_openai_embedding(self, text):
        try:
            # generate embedding using LangChain interface
            embedding = self.client.embed_query(text)
            return embedding
        except Exception as e:
            print(f"[ERROR] OpenAI API call failed: {e}")
            return None

    def insert_context_embeddings(self, id_key, embedding, table_name="context_embeddings"):
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host, port=self.port, dbname=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()
            insert_query = sql.SQL("""
                INSERT INTO {table} (id_key, embedding)
                VALUES (%s, %s)
                ON CONFLICT (id_key) DO UPDATE SET embedding = EXCLUDED.embedding;
            """).format(table=sql.Identifier(table_name))
            cur.execute(insert_query, (id_key, embedding))
            conn.commit()
            cur.close()
            print(f"Embedding for {id_key} inserted successfully.")
        except Exception as e:
            print(f"[ERROR] Inserting embedding failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _to_readable_text(self, record: dict, exclude_keys=None):
        exclude_keys = exclude_keys or {"data_source_id", "data_namespace"}
        
        def clean(val):
            val = str(val).replace('\n', ' ').replace('\r', ' ')
            return ''.join(c for c in val).strip()
        
        # Handle column context format
        if ("column_name_details" in record and "table_name_details" in record) or \
             ("column_name" in record and "table_name" in record):
            # This is a column context record
            table_name = record.get("table_name_details", record.get("table_name", ""))
            column_name = record.get("column_name_details", record.get("column_name", ""))
            data_type = record.get("data_type", "")
            description = record.get("description", "")
            
            sections = [
                f"# Column: {column_name}",
                f"Table: {table_name}",
                f"Data Type: {data_type}",
                f"Description: {description}"
            ]
            
            # Add filterable/aggregatable properties
            is_filterable = record.get("is_filterable", False)
            is_aggregatable = record.get("is_aggregatable", False)
            properties = []
            if is_filterable:
                properties.append("Filterable")
            if is_aggregatable:
                properties.append("Aggregatable")
            
            # Add sample values if available
            if "sample_values" in record and record["sample_values"]:
                samples = [clean(str(v)) for v in record["sample_values"]]
                if samples:
                    sections.append(f"Sample Values: {', '.join(samples)}")
            
            # Add related business terms
            if "related_business_terms" in record and record["related_business_terms"]:
                terms = [clean(term) for term in record["related_business_terms"]]
                if terms:
                    sections.append(f"Related Business Terms: {', '.join(terms)}")
            
            # Add sample usage examples
            if "sample_usage" in record and record["sample_usage"]:
                usage_sections = ["Sample Usage:"]
                for i, usage in enumerate(record["sample_usage"]):
                    desc = usage.get("description", "")
                    sql = usage.get("sql", "")
                    if desc and sql:
                        usage_sections.append(f"  {i+1}. {desc}\n     `{sql}`")
                
                if len(usage_sections) > 1:  # Only add if we have actual usage examples
                    sections.append("\n".join(usage_sections))
            
            # Add any other relevant column properties
            for key, value in record.items():
                if key in exclude_keys or key in {"table_name", "table_name_details", "column_name", "column_name_details", 
                                                 "data_type", "description", "is_filterable", "is_aggregatable", 
                                                 "sample_values", "related_business_terms", "sample_usage"}:
                    continue
                
                if isinstance(value, list):
                    flat = ", ".join(clean(v) if not isinstance(v, dict) else clean(str(v)) for v in value)
                    sections.append(f"{key.replace('_', ' ').title()}: {flat}")
                elif isinstance(value, dict):
                    inner = ", ".join(f"{k} {clean(v)}" for k, v in value.items())
                    sections.append(f"{key.replace('_', ' ').title()}: {inner}")
                else:
                    sections.append(f"{key.replace('_', ' ').title()}: {clean(value)}")
            
            return "\n".join(sections)
        
        # Handle table context format
        elif ("table_name" in record or "table_name_details" in record) and "description" in record:
            # This is a table context record
            table_name = record.get("table_name") or record.get("table_name_details")
            display_name = record.get("display_name", table_name)
            description = record.get("description", "")
            
            sections = [
                f"# Table: {table_name} ({display_name})",
                f"Description: {description}"
            ]
            
            # Process key columns
            if "key_columns" in record and record["key_columns"]:
                key_cols = "\n      – " + "\n      – ".join(clean(col) for col in record["key_columns"])
                sections.append(f"Columns:\n  • Key columns:{key_cols}")
            
            # Process filter columns
            if "filter_columns" in record and record["filter_columns"]:
                filter_cols = "\n      – " + "\n      – ".join(clean(col) for col in record["filter_columns"])
                sections.append(f"  • Filterable columns:{filter_cols}")
            
            # Process aggregate columns
            if "aggregate_columns" in record and record["aggregate_columns"]:
                agg_cols = "\n      – " + "\n      – ".join(clean(col) for col in record["aggregate_columns"])
                sections.append(f"  • Aggregatable columns:{agg_cols}")
            
            # Process sort columns
            if "sort_columns" in record and record["sort_columns"]:
                sort_cols = "\n      – " + "\n      – ".join(clean(col) for col in record["sort_columns"])
                sections.append(f"  • Sortable columns:{sort_cols}")
            
            # Process join tables
            if "join_tables" in record and record["join_tables"]:
                joins = []
                for join in record["join_tables"]:
                    join_type = join.get("join_type", "JOIN")
                    join_table = join.get("table_name", "")
                    join_condition = join.get("join_condition", "")
                    if join_table and join_condition:
                        joins.append(f"  – {join_type} {join_table}  \n      ON {join_condition}    ")
                
                if joins:
                    sections.append(f"Joins:\n{chr(10).join(joins)}")
            
            # Process business terms
            if "related_business_terms" in record and record["related_business_terms"]:
                terms = ", ".join(clean(term) for term in record["related_business_terms"])
                sections.append(f"Business terms:\n  {terms}")
            
            # Process sample usage
            if "sample_usage" in record and record["sample_usage"]:
                usage_sections = ["Sample queries:"]
                for i, sample in enumerate(record["sample_usage"]):
                    desc = sample.get("description", "")
                    sql = sample.get("sql", "")
                    if desc and sql:
                        usage_sections.append(f"  {i+1}. {desc}\n     `{sql};`")
                
                if len(usage_sections) > 1:  # Only add if we have actual usage examples
                    sections.append("\n".join(usage_sections))
            
            # Process tags
            if "tags" in record and record["tags"]:
                tags = ", ".join(clean(tag) for tag in record["tags"])
                sections.append(f"Tags:\n  {tags}")
            
            return "\n\n".join(sections)
        
        # Default formatting for other record types
        else:
            lines = []
            for key, value in record.items():
                if key in exclude_keys:
                    continue
                if isinstance(value, list):
                    flat = ", ".join(clean(v) if not isinstance(v, dict) else clean(str(v)) for v in value)
                    lines.append(f"{key}: {flat}.")
                elif isinstance(value, dict):
                    inner = ", ".join(f"{k} {clean(v)}" for k, v in value.items())
                    lines.append(f"{key}: {inner}.")
                else:
                    lines.append(f"{key}: {clean(value)}.")
            return "\n\n".join(lines)

    def insert_table_context(self, record: dict, table_name: str = "table_context"):
        table_name_val = record.get("table_name") or record.get("table_name_details")
        id_key = generate_id_key(
            record.get("data_source_id"),
            record.get('data_namespace', ''),
            table_name_val
        )
        raw_text = self._to_readable_text(record)
        embedding = self.get_openai_embedding(raw_text)

        conn = None
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.database,
                                    user=self.user, password=self.password)
            cur = conn.cursor()
            insert_sql = sql.SQL("""
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE IF NOT EXISTS {table} (
                    id_key VARCHAR(255) PRIMARY KEY,
                    embedding VECTOR(1536),
                    raw_text TEXT
                );
                INSERT INTO {table} (id_key, embedding, raw_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_key) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    raw_text = EXCLUDED.raw_text;
            """).format(table=sql.Identifier(table_name))
            cur.execute(insert_sql, (id_key, embedding, raw_text))
            conn.commit()
            cur.close()
            print(f"Table context stored: {id_key}")
        except Exception as e:
            print(f"[ERROR] insert_table_context failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def insert_column_context(self, record: dict, table_name: str = "column_context"):
        table_name_val = record.get("table_name_details") or record.get("table_name")
        column_name_val = record.get("column_name_details") or record.get("column_name")
        
        id_key = generate_column_id_key(
            record.get("data_source_id"),
            record.get('data_namespace', ''),
            table_name_val,
            column_name_val
        )

        raw_text = self._to_readable_text(record)
        print(raw_text)
        embedding = self.get_openai_embedding(raw_text)

        conn = None
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.database,
                                    user=self.user, password=self.password)
            cur = conn.cursor()
            insert_sql = sql.SQL("""
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE IF NOT EXISTS {table} (
                    id_key       VARCHAR(255) PRIMARY KEY,
                    embedding    VECTOR(1536),
                    raw_text     TEXT
                );
                INSERT INTO {table} (id_key, embedding, raw_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_key) DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    raw_text  = EXCLUDED.raw_text;
            """).format(table=sql.Identifier(table_name))
            cur.execute(insert_sql, (id_key, embedding, raw_text))
            conn.commit()
            cur.close()
            print(f"Column context stored: {id_key}")
            return id_key
        except Exception as e:
            print(f"[ERROR] insert_column_context failed: {e}")
            raise
        finally:
            if conn:
                conn.close()