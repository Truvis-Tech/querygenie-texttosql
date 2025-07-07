from langchain_community.document_loaders import JSONLoader
from langchain_community.vectorstores import FAISS
import json
from src.llm.embedding import Embedding
from src.utils.logger import logger

class RelevantData():
    def __init__(self, user_query: str, config, db: str = ''):
        self.user_query = user_query
        self.db = db
        self.config = config

    def get_relevant_tables(self):
        tables_path = self.config.get('Database', 'tables_path')
        emb = Embedding(self.config)
        logger.info("Dynamically loading documents from file")
        documents = JSONLoader(file_path=tables_path, jq_schema='.', text_content=False, json_lines=False).load()
        db = FAISS.from_documents(documents=documents, embedding=emb.get_embeddings())
        retriever = db.as_retriever(search_type='mmr', search_kwargs={'k': 5, 'lambda_mult': 1})
        matched_documents = retriever.get_relevant_documents(query=self.user_query)
        matched_tables = []
        for document in matched_documents:
            page_content = document.page_content
            page_content = json.loads(page_content)
            table_name = page_content['table_name']
            desc = page_content['description']
            example_queries = page_content['sample_usage']
            matched_tables.append(f'{table_name}|{desc}|{example_queries}')
        matched_tables = '\n'.join(matched_tables)    
        return matched_tables

    def get_relevant_columns(self):
        columns_path = self.config.get('Database', 'columns_path')
        emb = Embedding(self.config)
        logger.info("Dynamically loading documents from file")
        documents = JSONLoader(file_path=columns_path, jq_schema='.[]', text_content=False, json_lines=False).load()
        db = FAISS.from_documents(documents=documents, embedding=emb.get_embeddings())

        search_kwargs = {
            'k': 20
        }

        retriever = db.as_retriever(search_type='similarity', search_kwargs=search_kwargs)
        matched_docs = retriever.get_relevant_documents(query=self.user_query)

        filtered_cols = []
        # LangChain filters does not support multiple values at the moment
        for i, column in enumerate(matched_docs):
            page_content = json.loads(column.page_content)
            filtered_cols.append(page_content)

        matched_columns = []
        for doc in filtered_cols:
            table_name = doc['table_name']
            column_name = doc['column_name']
            data_type = doc['data_type']
            col_desc = doc['description']
            matched_columns.append(f'table_name={table_name}|column_name={column_name}|data_type={data_type}|description={col_desc}') 
        matched_columns = '\n'.join(matched_columns)
        return matched_columns