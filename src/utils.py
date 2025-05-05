import os
import yaml
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
        config (Dict[str, Any]): Configuration dictionary.

    Note:
        This is a placeholder function for future implementation.
    """
    # TODO: Implement logging setup
    pass
