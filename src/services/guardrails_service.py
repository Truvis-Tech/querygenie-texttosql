from src.llm.prompt_templates import get_prompt_for_analytical_intent, get_prompt_verify_sql_injection, get_prompt_verify_invalid_domain_query
from src.llm.llm_connector import *
from src.utils.logger import logger

def validate_query_intent_for_analytical(user_query, config):
    logger.info("Starting analytical intent validation")
    logger.debug(f"User query length: {len(user_query)} characters")
    
    try:
        logger.debug("Getting analytical intent prompt")
        prompt = get_prompt_for_analytical_intent(user_query)
        messages = [{"role": "user", "content": f"{prompt}"}]
        
        logger.debug("Sending request to LLM for intent validation")
        res = LLMConnector(messages, config)
        response = res
        
        logger.info("Analytical intent validation completed successfully")
        logger.debug(f"Validation response: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Analytical intent validation failed: {str(e)}", exc_info=True)
        raise

def validate_query_for_sql_injection(sql_query: str, config):
    logger.info("Starting SQL injection validation")
    logger.debug(f"SQL query length: {len(sql_query)} characters")
    
    try:
        logger.debug("Getting SQL injection verification prompt")
        prompt = get_prompt_verify_sql_injection(sql_query=sql_query)
        messages = [{"role": "user", "content": f"{prompt}"}]
        
        logger.debug("Sending request to LLM for SQL injection check")
        res = LLMConnector(messages, config)
        response = res.get_llm_response()
        
        logger.info("SQL injection validation completed successfully")
        logger.debug(f"SQL injection check response: {response}")
        return response
        
    except Exception as e:
        logger.error(f"SQL injection validation failed: {str(e)}", exc_info=True)
        raise

def validate_query_for_invalid_domain_query(content: str, config):
    logger.info("Starting invalid domain query validation")
    logger.debug(f"Content length: {len(content)} characters")
    
    try:
        logger.debug("Getting invalid domain query verification prompt")
        prompt = get_prompt_verify_invalid_domain_query(content)
        messages = [{"role": "user", "content": f"{prompt}"}]
        
        logger.debug("Sending request to LLM for invalid domain query check")
        res = LLMConnector(messages, config)
        response = res
        
        logger.info("Invalid domain query validation completed successfully")
        logger.debug(f"Invalid domain query check response: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Invalid domain query validation failed: {str(e)}", exc_info=True)
        raise