import logging
import os
from typing import Any, cast

import yaml
from pyspark.sql import SparkSession


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
    except ValueError as e:
        raise ValueError(f"Invalid configuration in {path}: {str(e)}")
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

def get_spark_session(config: dict[str, Any] | None = None, app_name: str = "MovieRecommendation") -> SparkSession:
    """
    Create or get a configured SparkSession for the pipeline.

    Args:
        config (dict[str, Any], optional): Configuration dictionary. If provided,
            it will use the spark settings from the config.
        app_name (str, optional): Name of the Spark application.
            Defaults to "MovieRecommender".

    Returns:
        SparkSession: The configured SparkSession instance.

    Example:
        >>> config = load_config("config.yaml")
        >>> spark = get_spark_session(config)
        >>> # Do operations with spark
        >>> spark.stop()
    """
    # Start with default builder
    builder = cast(SparkSession.Builder, SparkSession.builder)
    builder = builder.appName(app_name)

    # Use local mode by default
    master = "local[*]"

    # If config is provided, use it to override defaults
    if config and 'spark' in config:
        spark_config = config['spark']

        # Override app_name if specified in config
        if 'app_name' in spark_config:
            builder = builder.appName(spark_config['app_name'])

        # Override master if specified in config
        if 'master' in spark_config:
            master = spark_config['master']

    # Set the master
    builder = builder.master(master)

    # Add PostgreSQL JDBC driver
    builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")

    # Add any additional configuration options from the config
    if config and 'spark' in config and 'config' in config['spark']:
        for key, value in config['spark']['config'].items():
            builder = builder.config(key, value)

    # Create or get the SparkSession
    spark = builder.getOrCreate()

    # Set log level if specified in config
    if config and 'spark' in config and 'log_level' in config['spark']:
        spark.sparkContext.setLogLevel(config['spark']['log_level'])

    logging.info(f"Created SparkSession with appName={spark.sparkContext.appName} and master={spark.sparkContext.master}")

    return spark
