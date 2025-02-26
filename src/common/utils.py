import logging
import os
from datetime import datetime
import ctypes
import json
from common.config import global_conf


# Global logger instance
_logger = None

def get_logger(logging_level="INFO"):
    """
    Configures and returns a singleton logger instance.

    Args:
        logging_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        logging.Logger: Configured logger instance
    """
    global _logger

    if _logger is not None:
        return _logger

    # Create logs directory if it does not exist
    os.makedirs("logs", exist_ok=True)

    # Mapping log level string to logging constants
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    log_level = log_levels.get(logging_level.upper(), logging.INFO)

    # Logger initialization
    logger = logging.getLogger("analytics_logs")
    logger.setLevel(log_level)

    # Ensure handlers are not added multiple times
    if not logger.hasHandlers():
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)

        # File handler
        if global_conf.get("GENERAL.ENV") == "local":
            log_filename = datetime.now().strftime("logs/app_%Y%m%d.log")
        else:
            # TODO Send log to cloud logging on gcp
            log_filename = datetime.now().strftime("app_%Y%m%d.log")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    _logger = logger
    return logger


def log_message(logging_level, message):
    """
    Logs a message using the configured logger.

    Args:
        logging_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        message (str): The message to log
    """
    logger = get_logger(logging_level)

    # Log the message based on the given level
    log_methods = {
        "DEBUG": logger.debug,
        "INFO": logger.info,
        "WARNING": logger.warning,
        "ERROR": logger.error,
        "CRITICAL": logger.critical
    }

    log_method = log_methods.get(logging_level.upper(), logger.info)
    log_method(message)



def dict_to_string(d):
    # TODO Write documentation
    if not isinstance(d, dict):
        # raise ValueError("Give dictionary")
        return ""
    
    return json.dumps(d, ensure_ascii=False)

