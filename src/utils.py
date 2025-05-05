import os
import yaml
import logging
from typing import Any


def load_config(path: str) -> dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        path (str): Path to the YAML configuration file.

    Returns:
        dict[str, Any]: Dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the configuration file doesn't exist.
        yaml.YAMLError: If there's an error parsing the YAML file.
        ValueError: If the file is empty or contains invalid YAML.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found at: {path}")

    try:
        with open(path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        if config is None:
            raise ValueError(f"Configuration file is empty: {path}")

        return config
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {path}: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error loading configuration from {path}: {str(e)}")


def get_nested_config(config: dict[str, Any], keys: str, default=None) -> Any:
    """
    Safely access nested configuration values using dot notation.

    Args:
        config (dict[str, Any]): The configuration dictionary.
        keys (str): Dot-separated string of keys (e.g., 'postgres.host').
        default: Value to return if the key doesn't exist.

    Returns:
        Any: The configuration value or default if not found.

    Example:
        >>> config = {'postgres': {'host': 'localhost', 'port': 5432}}
        >>> get_nested_config(config, 'postgres.host')
        'localhost'
        >>> get_nested_config(config, 'postgres.username', 'default_user')
        'default_user'
    """
    current = config
    for key in keys.split('.'):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def setup_logging(config: dict[str, Any]) -> None:
    """
    Set up logging based on configuration.

    Args:
        config (dict[str, Any]): Configuration dictionary containing logging settings.
            Expected keys in config['logging']:
            - file: Path to the log file
            - level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            - max_size: Maximum log file size in MB (optional)
            - backup_count: Number of backup logs to keep (optional)

    Returns:
        None

    Raises:
        KeyError: If required logging configuration is missing.
        ValueError: If log level is invalid.
        OSError: If log directory creation fails.
    """
    # Extract logging configuration with defaults
    logging_config = config.get('logging', {})
    log_file = logging_config.get('file', 'logs/pipeline.log')
    log_level_str = logging_config.get('level', 'INFO').upper()

    # Map log level string to logging constant
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    log_level = log_level_map.get(log_level_str, logging.INFO)

    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            raise OSError(f"Failed to create log directory {log_dir}: {str(e)}")

    # Configure logging with both file and console handlers
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]

    # Configure logging format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Configure the root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )

    # Log the setup completion
    logging.info(f"Logging configured: level={log_level_str}, file={log_file}")
