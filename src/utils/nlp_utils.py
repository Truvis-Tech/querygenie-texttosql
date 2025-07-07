import nltk
import re

# english_vocab = set(w.lower() for w in nltk.corpus.words.words())

def invalid_utterance_in_prompt(nlp_query):
    """Check if the input is a greeting or invalid query."""
    if not nlp_query or not isinstance(nlp_query, str):
        return True
        
    invalid_utterances = {
        'hi', 'hello', 'hey', 'good morning', 'morning', 'good',
        'how are you?', 'how are you', 'how is it going', '',
        'hey there', 'hi there', 'good day', 'good evening', 'good afternoon'
    }
    
    query_lower = nlp_query.strip().lower()
    return query_lower in invalid_utterances or any(
        query_lower.startswith(greeting) 
        for greeting in ['hi ', 'hello ', 'hey ']
    )

def validate_english(nlp_query):
    destructive_keywords = {'delete', 'drop', 'update', 'truncate','alter'}

    cleaned_query = re.sub(r'[^\w\s]', '', nlp_query).lower().strip()
    query_words = cleaned_query.split()

    if not query_words:
        return True

    # Block if any destructive keywords are present
    if any(word in destructive_keywords for word in query_words):
        return True

    # Check if the first valid word is in the English vocabulary
    return query_words[0] not in english_vocab

def check_for_modification_in_query(sql_query: str) -> bool:
    """
    Check if the SQL query contains data modification operations.
    
    Args:
        sql_query: The SQL query to check
        
    Returns:
        bool: True if the query contains modification keywords, False otherwise
    """
    if not sql_query or not isinstance(sql_query, str):
        return False
    
    # Common SQL modification keywords
    modification_keywords = {
        'DELETE', 'TRUNCATE', 'UPDATE', 'DROP', 'ALTER',
        'INSERT', 'CREATE', 'REPLACE', 'GRANT', 'REVOKE'
    }
    
    # Remove string literals to avoid false positives
    query_upper = re.sub(r'\'.*?\'|".*?"', '', sql_query, flags=re.DOTALL).upper()
    
    # Check for any modification keywords as whole words
    return any(
        f' {keyword} ' in f' {query_upper} '
        for keyword in modification_keywords
    )
    
destructive_keywords = {'delete', 'drop', 'update', 'truncate', 'insert'}

def is_dml_query(nlp_query):
    cleaned_query = re.sub(r'[^\w\s]', '', nlp_query).lower()
    return any(word in destructive_keywords for word in cleaned_query.split())
