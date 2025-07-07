import requests
import json
from openai import OpenAI
from src.utils.logger import logger

class LLMConnector:
    def __init__(self, messages_prompt, config):
        logger.debug("Initializing LLMConnector")
        self.messages_prompt = messages_prompt
        self.config = config
        
        try:
            self.LLM_BASE_URL = self.config.get('LLM', 'base_url')
            self.LLM_PROJECT_ID = self.config.get('LLM', 'project_id')
            self.LLM_API_KEY = self.config.get('LLM', 'api_key')
            self.LLM_MODEL = self.config.get('LLM', 'model')
            
            logger.info(f"LLM configuration loaded - Model: {self.LLM_MODEL}")
            logger.debug(f"Using custom base URL: {bool(self.LLM_BASE_URL)}")
            logger.debug(f"Message count: {len(messages_prompt)}")
            
        except Exception as e:
            logger.error(f"Failed to load LLM configuration: {str(e)}", exc_info=True)
            raise

    def get_llm_response(self):
        logger.info(f"Getting LLM response using model: {self.LLM_MODEL}")
        
        try:
            # Initialize LLM client with or without base_url
            if self.LLM_BASE_URL:
                logger.debug("Using custom LLM endpoint")
                
                headers = {
                    'HSBC-Params': f'{{"req_from":"{self.LLM_PROJECT_ID}", "type":"chat"}}',
                    'Authorization-Type': 'genai',
                    'Authorization': f'Bearer {self.LLM_API_KEY}',
                    'Content-Type': 'application/json'
                }

                data = {
                    "messages": self.messages_prompt,
                    "temperature": 0.0,
                    "top_p": 0.1,
                    "frequency_penalty": 0.1,
                    "presence_penalty": 0.1,
                    "max_tokens": 2919,
                    "stop": None,
                    "stream": False
                }
                
                logger.debug(f"Making request to custom endpoint: {self.LLM_BASE_URL}")
                response = requests.post(self.LLM_BASE_URL, headers=headers, json=data, verify=False)
                
                if response.status_code != 200:
                    logger.error(f"LLM request failed with status {response.status_code}: {response.text}")
                    raise Exception(f"LLM request failed with status {response.status_code}")
                
                response_data = json.loads(response.text)
                logger.debug(f"LLM response received, status: {response.status_code}")
                
                if 'choices' not in response_data or not response_data['choices']:
                    logger.error(f"Invalid LLM response format: {response_data}")
                    raise Exception("Invalid response format from LLM")
                
                answer = response_data['choices'][0]['message']['content'].strip()
                logger.info("LLM response processed successfully")

            else:
                logger.debug("Using standard LLM client")
                
                client = OpenAI(api_key=self.LLM_API_KEY)
                response = client.chat.completions.create(
                    model=self.LLM_MODEL,
                    messages=self.messages_prompt,
                    temperature=0.0,
                    top_p=0.1,
                    presence_penalty=0.1,
                    frequency_penalty=0.1
                )
                
                answer = response.choices[0].message.content.strip()
                logger.info("Standard LLM response processed successfully")
                logger.debug(f"Token usage: {response.usage}")
            
            logger.debug(f"Response length: {len(answer)} characters")
            return answer
            
        except requests.exceptions.RequestException as req_error:
            logger.error(f"Network error during LLM request: {str(req_error)}", exc_info=True)
            raise
        except json.JSONDecodeError as json_error:
            logger.error(f"Failed to parse LLM response JSON: {str(json_error)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"LLM request failed: {str(e)}", exc_info=True)
            raise