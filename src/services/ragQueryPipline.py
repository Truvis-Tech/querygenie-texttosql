import os
from openai import OpenAI
import psycopg2
import re

from src.database.db_connector import get_postgres_connection_params
from src.utils.config_reader import load_config
from src.llm.embedding import Embedding

# Class for the RAG pipeline
# Creates a pipeline to retrieve relevant context from a PostgreSQL database
class RAGPipeline:
    def __init__(self, market):
        self.market = market
        self.host, self.port, self.database, self.user, self.password = get_postgres_connection_params(market)
        
        self.config = load_config(self.market)
        emb = Embedding(self.config)
        self.client = emb.get_embeddings()
        
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        
        self.cur = self.conn.cursor()
        self.TOP_K_DB_FETCH = 5
        self.RESPONSE_LIMIT = 10
        self.SHOW_SCORE = True
        self.SHOW_STRUCTURED_CONTEXT = True

    # Generates an embedding for the given text using the configured embedding client
    def get_embedding(self, text):
        text = text.replace("\n", " ")
        embedding = self.client.embed_query(text)
        return embedding

    # Retrieves similar contexts from the database based on the query text and returns a tuple of (column_contexts, table_contexts) where each is a list of (raw_text, similarity_score)
    def retrieve_similar(self, query_text):
        query_embedding = self.get_embedding(query_text)
        
        # Query column contexts
        self.cur.execute("""
            SELECT raw_text,
                   ((2 - (embedding <-> %s::vector)) / 2) AS similarity_score
            FROM column_context
            ORDER BY embedding <-> %s::vector
            LIMIT %s
        """, (query_embedding, query_embedding, self.TOP_K_DB_FETCH))
        column_results = [(row[0], round(row[1] * 100, 2)) for row in self.cur.fetchall()]
        
        # Query table contexts
        self.cur.execute("""
            SELECT raw_text,
                   ((2 - (embedding <-> %s::vector)) / 2) AS similarity_score
            FROM table_context
            ORDER BY embedding <-> %s::vector
            LIMIT %s
        """, (query_embedding, query_embedding, self.TOP_K_DB_FETCH))
        table_results = [(row[0], round(row[1] * 100, 2)) for row in self.cur.fetchall()]
        
        return column_results, table_results

    # Check if this is a markdown-style header format (our new format starting with '#')
    def parse_context_text(self, text):
        if text.strip().startswith('#'):
            return self._parse_markdown_context(text)
        else:
            # Fall back to the original parsing method for backward compatibility
            return self._parse_legacy_context(text)
    
    # Use to parse the context text based on the new markdown-style format (start with '#')
    def _parse_markdown_context(self, text):
        """Parse the new markdown-style context format with sample usage"""
        context_dict = {}
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        if not lines:
            return [{}], [""]
            
        # Extract the header (column or table name)
        if lines[0].startswith('#'):
            header = lines[0].strip('# :')
            if 'Column:' in header:
                context_dict['type'] = 'column'
                context_dict['name'] = header.replace('Column:', '').strip()
            elif 'Table:' in header:
                context_dict['type'] = 'table'
                context_dict['name'] = header.replace('Table:', '').strip()
        
        # Process the remaining lines
        current_section = None
        section_content = []
        
        for line in lines[1:]:  # Skip the header line
            line = line.strip()
            if not line:
                continue
                
            # Check for section headers (lines ending with ':')
            if line.endswith(':'):
                # Save previous section if exists
                if current_section and section_content:
                    context_dict[current_section] = ' '.join(section_content).strip()
                    section_content = []
                current_section = line[:-1].strip()  # Remove the trailing colon
                continue
                
            # Handle bullet points and other list items
            if line.startswith(('•', '-', '*')):
                line = line[1:].strip()
                
            # Add line to current section
            if current_section is not None:
                section_content.append(line)
            # If no section header yet, treat as description
            elif 'Description' not in context_dict:
                context_dict['Description'] = line
            else:
                context_dict['Description'] += ' ' + line
        
        # # Add sample usage to context dictionary
        # if sample_usage_lines:
        #     context_dict['Sample Usage'] = ' - '.join(sample_usage_lines)
        
        # Special handling for table context
        if context_dict.get('type') == 'table':
            # Clean up empty or whitespace-only values
            context_dict = {k: v for k, v in context_dict.items() if v and str(v).strip()}
            
            # Format the output for better readability
            formatted_context = []
            if 'name' in context_dict:
                formatted_context.append(f"Table: {context_dict['name']}")
            if 'Description' in context_dict:
                formatted_context.append(f"Description: {context_dict['Description']}")
                
            # Add sections in a structured way
            sections = [
                'Key columns', 'Filterable columns', 
                'Aggregatable columns', 'Sortable columns',
                'Joins', 'Business terms', 'Tags'
            ]
            
            for section in sections:
                if section in context_dict and context_dict[section]:
                    formatted_context.append(f"{section}: {context_dict[section]}")
            
            flat_text = ' | '.join(formatted_context)
            return [context_dict], [flat_text]
        
        # For column context or other types, use the original format
        flat_text = ', '.join([f"{k}: {v}" for k, v in context_dict.items()])
        return [context_dict], [flat_text]
    
    # it returns a tuple containing a list of dictionaries (structured data) and a list of flat text strings for context processing.
    def _parse_legacy_context(self, text):
        """Original parsing method for backward compatibility or if the config is not in markdown format (does not start with '#')"""
        context_blocks = [block.strip() for block in text.split("Context:") if block.strip()]
        parsed_structured = []
        parsed_text = []

        for block in context_blocks:
            context_dict = {}
            fields = re.split(r"\n+|\.\n+", block)
            for field in fields:
                if ":" in field:
                    key, value = field.split(":", 1)
                    context_dict[key.strip()] = value.strip().rstrip('.')
            parsed_structured.append(context_dict)
            flat_text = ', '.join([f"{k}: {v}" for k, v in context_dict.items()])
            parsed_text.append(flat_text)

        return parsed_structured, parsed_text
    
    # Queries the RAG pipeline for a given question and returns top relevant contexts.
    def query(self, question):
        column_contexts, table_contexts = self.retrieve_similar(question)
        
        # Log column contexts
        logger.debug("=== COLUMN CONTEXTS ===")
        for i, (raw_text, score) in enumerate(column_contexts, 1):
            structured, flat = self.parse_context_text(raw_text)
            logger.debug(f"Column Context #{i} (Score: {score}):")
            # logger.debug(f"Raw text: {raw_text}")
            # logger.debug(f"Structured: {structured[0] if structured else {}}")
            # logger.debug(f"Flat: {flat[0] if flat else ''}")

        # Log table contexts
        logger.debug("=== TABLE CONTEXTS ===")
        for i, (raw_text, score) in enumerate(table_contexts, 1):
            structured, flat = self.parse_context_text(raw_text)
            logger.debug(f"Table Context #{i} (Score: {score}):")
            # logger.debug(f"Raw text: {raw_text}")
            # logger.debug(f"Structured: {structured[0] if structured else {}}")
            # logger.debug(f"Flat: {flat[0] if flat else ''}")
    
        if not column_contexts and not table_contexts:
            return {
                "text_context": [],
                "structured_context": []
            }

        structured_results = []
        text_blocks = []

        # Process column contexts
        for raw_text, score in column_contexts:
            structured_list, flat_texts = self.parse_context_text(raw_text)
            for i in range(len(flat_texts)):
                entry = {
                    "context": flat_texts[i],
                    "context_type": "column"
                }
                if self.SHOW_SCORE:
                    entry["similarity_score"] = score
                text_blocks.append(entry)

                if self.SHOW_STRUCTURED_CONTEXT:
                    structured_results.append({
                        "key_value_context": structured_list[i],
                        "similarity_score": score if self.SHOW_SCORE else None,
                        "context_type": "column"
                    })
        
        # Process table contexts
        for raw_text, score in table_contexts:
            structured_list, flat_texts = self.parse_context_text(raw_text)
            for i in range(len(flat_texts)):
                entry = {
                    "context": flat_texts[i],
                    "context_type": "table"
                }
                if self.SHOW_SCORE:
                    entry["similarity_score"] = score
                text_blocks.append(entry)

                if self.SHOW_STRUCTURED_CONTEXT:
                    structured_results.append({
                        "key_value_context": structured_list[i],
                        "similarity_score": score if self.SHOW_SCORE else None,
                        "context_type": "table"
                    })

        # Sort results by score if needed
        if self.SHOW_SCORE:
            text_blocks.sort(key=lambda x: x.get("similarity_score", 0), reverse=True)
            structured_results.sort(key=lambda x: x.get("similarity_score", 0), reverse=True)

        return {
            "text_context": text_blocks[:self.RESPONSE_LIMIT],
            "structured_context": structured_results[:self.RESPONSE_LIMIT] if self.SHOW_STRUCTURED_CONTEXT else []
        }

    def close(self):
        self.cur.close()
        self.conn.close()