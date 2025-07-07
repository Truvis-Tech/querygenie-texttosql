"""
Utility functions for BigQuery cost calculations.
Enhanced with comprehensive logging for debugging.
"""
import configparser
import os
import logging
from datetime import datetime
from src.utils.logger import logger


def get_pricing_config():
    """
    Get the pricing configuration from the pricing_config.ini file.

    Returns:
        dict: Pricing configuration values
    """
    logger.debug("Loading pricing configuration")
    
    try:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'pricing.ini')
        logger.debug(f"Config file path: {config_path}")
        
        if not os.path.exists(config_path):
            logger.warning(f"Pricing config file not found at {config_path}, using defaults")
        
        config.read(config_path)
        
        pricing_config = {
            'query_price_per_tb': float(config.get('Pricing', 'query_price_per_tb', fallback='5.0')),
            'safety_margin_percentage': float(config.get('Pricing', 'safety_margin_percentage', fallback='5')),
            'enable_cost_logging': config.getboolean('Logging', 'enable_cost_logging', fallback=True),
            'cost_log_file': config.get('Logging', 'cost_log_file', fallback='cost_estimates.log'),
            'default_daily_budget_usd': float(config.get('Budget', 'default_daily_budget_usd', fallback='0'))
        }
        
        logger.info(f"Pricing config loaded - Price per TB: ${pricing_config['query_price_per_tb']}, Safety margin: {pricing_config['safety_margin_percentage']}%")
        logger.debug(f"Cost logging enabled: {pricing_config['enable_cost_logging']}")
        
        return pricing_config
        
    except Exception as e:
        logger.error(f"Failed to load pricing configuration: {str(e)}", exc_info=True)
        # Return defaults if config loading fails
        default_config = {
            'query_price_per_tb': 5.0,
            'safety_margin_percentage': 5.0,
            'enable_cost_logging': True,
            'cost_log_file': 'cost_estimates.log',
            'default_daily_budget_usd': 0.0
        }
        logger.warning("Using default pricing configuration due to config load failure")
        return default_config


def calculate_query_cost(bytes_processed):
    """
    Calculate the cost of a query based on bytes processed.

    Args:
        bytes_processed (int): Number of bytes processed by the query

    Returns:
        dict: Cost information including bytes, TB, and USD cost
    """
    logger.debug(f"Calculating cost for {bytes_processed} bytes")
    
    try:
        # Get pricing configuration
        pricing_config = get_pricing_config()

        # Convert bytes to terabytes
        tb_processed = bytes_processed / (1024 ** 4)
        logger.debug(f"Converted to {tb_processed:.6f} TB")

        # Calculate base cost
        base_cost = tb_processed * pricing_config['query_price_per_tb']
        logger.debug(f"Base cost: ${base_cost:.6f}")

        # Apply safety margin
        safety_margin = base_cost * (pricing_config['safety_margin_percentage'] / 100)
        total_cost = base_cost + safety_margin
        logger.debug(f"Safety margin: ${safety_margin:.6f}, Total cost: ${total_cost:.6f}")

        # Log cost estimate if enabled
        if pricing_config['enable_cost_logging']:
            try:
                log_cost_estimate(bytes_processed, tb_processed, total_cost)
            except Exception as log_error:
                logger.warning(f"Failed to log cost estimate: {log_error}")

        cost_result = {
            'bytes_processed': bytes_processed,
            'megabytes_processed': bytes_processed / (1024 ** 2),
            'gigabytes_processed': bytes_processed / (1024 ** 3),
            'terabytes_processed': tb_processed,
            'base_cost_usd': round(base_cost, 6),
            'safety_margin_usd': round(safety_margin, 6),
            'estimated_cost_usd': round(total_cost, 6),
            'price_per_tb_usd': pricing_config['query_price_per_tb']
        }
        
        logger.info(f"Cost calculation completed: {format_bytes(bytes_processed)} = ${total_cost:.6f} USD")
        return cost_result
        
    except Exception as e:
        logger.error(f"Cost calculation failed: {str(e)}", exc_info=True)
        raise


def log_cost_estimate(bytes_processed, tb_processed, cost_usd):
    """
    Log a cost estimate to the cost log file.

    Args:
        bytes_processed (int): Number of bytes processed
        tb_processed (float): Number of terabytes processed
        cost_usd (float): Estimated cost in USD
    """
    try:
        pricing_config = get_pricing_config()
        log_file = pricing_config['cost_log_file']
        logger.debug(f"Logging cost estimate to {log_file}")

        # Create logger
        cost_logger = logging.getLogger('cost_estimator')
        cost_logger.setLevel(logging.INFO)

        # Create file handler if not already created
        if not cost_logger.handlers:
            try:
                handler = logging.FileHandler(log_file)
                formatter = logging.Formatter('%(asctime)s - %(message)s')
                handler.setFormatter(formatter)
                cost_logger.addHandler(handler)
                logger.debug(f"Created cost log handler for {log_file}")
            except Exception as handler_error:
                logger.error(f"Failed to create cost log handler: {handler_error}")
                return

        # Log the cost estimate
        cost_logger.info(
            f"Cost Estimate: {bytes_processed:,} bytes ({tb_processed:.6f} TB) = ${cost_usd:.6f}"
        )
        logger.debug("Cost estimate logged successfully")
        
    except Exception as e:
        logger.error(f"Failed to log cost estimate: {str(e)}", exc_info=True)


def format_bytes(bytes_value):
    """
    Format bytes into a human-readable format.

    Args:
        bytes_value (int): Number of bytes

    Returns:
        str: Formatted string (e.g., "1.23 MB")
    """
    try:
        if bytes_value < 0:
            logger.warning(f"Negative bytes value received: {bytes_value}")
            return f"{bytes_value} B"
        
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 ** 2:
            return f"{bytes_value / 1024:.2f} KB"
        elif bytes_value < 1024 ** 3:
            return f"{bytes_value / (1024 ** 2):.2f} MB"
        elif bytes_value < 1024 ** 4:
            return f"{bytes_value / (1024 ** 3):.2f} GB"
        else:
            return f"{bytes_value / (1024 ** 4):.2f} TB"
            
    except Exception as e:
        logger.error(f"Error formatting bytes {bytes_value}: {str(e)}")
        return f"{bytes_value} B"