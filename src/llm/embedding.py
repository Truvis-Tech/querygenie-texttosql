from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.embeddings import OpenAIEmbeddings
from openai import OpenAI
from src.utils.logger import logger

class Embedding():
    def __init__(self, config):
        logger.debug("Initializing Embedding class")
        self.config = config
        
        try:
            self.LLM_BASE_EMBEDDING_URL = self.config.get('LLM', 'base_embedding_url')
            self.LLM_PROJECT_ID = self.config.get('LLM', 'project_id')
            self.LLM_API_KEY = self.config.get('LLM', 'api_key')
            self.LLM_API_VERSION = self.config.get('LLM', 'api_version')
            self.LLM_EMBEDDING_MODEL = self.config.get('LLM', 'embedding_model')
            
            logger.info(f"Embedding configuration loaded - Model: {self.LLM_EMBEDDING_MODEL}")
            logger.debug(f"Using base URL: {bool(self.LLM_BASE_EMBEDDING_URL)}")
            
        except Exception as e:
            logger.error(f"Failed to load embedding configuration: {str(e)}", exc_info=True)
            raise

    def get_embeddings(self):
        logger.info("Getting embeddings instance")
        
        try:
            if self.LLM_BASE_EMBEDDING_URL:
                logger.info("Initializing Azure OpenAI embeddings")
                
                headers = {
                    'HSBC-Params': f'{{"req_from":"{self.LLM_PROJECT_ID}", "type":"embedding"}}',
                    'Authorization-Type': 'genai',
                    'Authorization': f'Bearer {self.LLM_API_KEY}',
                    'Content-Type': 'application/json'
                }
                logger.debug("LLM headers configured")
                
                embedding = AzureOpenAIEmbeddings(
                    azure_endpoint=self.LLM_BASE_EMBEDDING_URL,
                    api_key=self.LLM_API_KEY,
                    api_version=self.LLM_API_VERSION,
                    deployment=self.LLM_EMBEDDING_MODEL,
                    default_headers=headers
                )
                logger.info("LLM embeddings initialized successfully")
                
            else:
                logger.info("Initializing default OpenAI embeddings")
                
                embedding = OpenAIEmbeddings(
                    model=self.LLM_EMBEDDING_MODEL,
                    api_key=self.LLM_API_KEY
                )
                logger.info("Standard LLM embeddings initialized successfully")
                
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to initialize embeddings: {str(e)}", exc_info=True)
            raise