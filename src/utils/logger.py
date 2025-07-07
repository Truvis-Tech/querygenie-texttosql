import logging
import logging.handlers
import os
import sys
import io

class UsernameFilter(logging.Filter):
    """Add username attribute to log records if missing"""
    def filter(self, record):
        if not hasattr(record, 'username'):
            record.username = 'SYSTEM'  # Default value
        return True

def setup_logger():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    

    if not logger.handlers:
        username_filter = UsernameFilter()
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(username)s - %(levelname)s - %(message)s'
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(username)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(username_filter)
        logger.addHandler(console_handler)
        
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "application.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8' # Explicitly set encoding for file handlers
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(username)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        file_handler.addFilter(username_filter)
        logger.addHandler(file_handler)
        
        error_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "errors.log"),
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3,
            encoding='utf-8' # Explicitly set encoding for file handlers
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        error_handler.addFilter(username_filter)
        logger.addHandler(error_handler)
    
    return logger

logger = setup_logger()