import logging
import logging.config
import os

import yaml


def ensure_log_files_exist(log_config):
    """
    Ensure log directories and files exist based on the logging configuration.

    Parameters:
        - log_config (dict): The logging configuration dictionary.

    Creates necessary directories and empty log files if they do not exist.
    """
    handlers = log_config.get('handlers', {})
    for handler in handlers.values():
        if 'filename' in handler:
            log_file_path = handler['filename']
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            open(log_file_path, 'a').close()


def setup_logging(log_config_file):
    """
    Set up logging configuration.

    Parameters:
        - log_config_file (str): Path to the logging configuration file.

    Returns:
        - logging.Logger: Configured logger instance.
    """
    with open(log_config_file, 'r') as file:
        log_config = yaml.safe_load(file)

    ensure_log_files_exist(log_config)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    return logger


def load_config(config_file):
    """
    Load configuration settings from a YAML file.

    Parameters:
        - config_file (str): Path to the configuration file.

    Returns:
        - dict: Dictionary containing configuration settings.
    """
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config


def are_dataframes_equal(df1, df2):
    """
    Compare two PySpark DataFrames regardless of row order.

    Parameters:
        - df1 (pyspark.sql.DataFrame): First DataFrame.
        - df2 (pyspark.sql.DataFrame): Second DataFrame.

    Returns:
        - bool: True if DataFrames are equal, False otherwise.
    """
    return df1.exceptAll(df2).count() == 0 and df2.exceptAll(df1).count() == 0
