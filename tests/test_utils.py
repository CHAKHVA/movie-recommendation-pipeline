import os
import re
import pytest
import yaml
import logging
from src.utils import load_config, get_nested_config, setup_logging


def test_load_config(tmp_path):
    """Test loading configuration from a YAML file."""
    # Create a temporary config file
    config_path = tmp_path / "test_config.yaml"
    test_config = {
        "storage": {
            "mode": "local"
        },
        "postgres": {
            "host": "localhost",
            "port": 5432
        },
        "als": {
            "rank": 10,
            "maxIter": 15
        }
    }

    with open(config_path, 'w') as f:
        yaml.dump(test_config, f)

    # Test successful config loading
    loaded_config = load_config(str(config_path))
    assert loaded_config == test_config
    assert loaded_config["storage"]["mode"] == "local"
    assert loaded_config["postgres"]["port"] == 5432
    assert loaded_config["als"]["rank"] == 10

    # Test file not found error
    nonexistent_path = tmp_path / "nonexistent.yaml"
    with pytest.raises(FileNotFoundError):
        load_config(str(nonexistent_path))

    # Test empty file error
    empty_path = tmp_path / "empty.yaml"
    with open(empty_path, 'w') as f:
        pass  # Create empty file

    with pytest.raises(ValueError, match="Configuration file is empty"):
        load_config(str(empty_path))

    # Test invalid YAML
    invalid_path = tmp_path / "invalid.yaml"
    with open(invalid_path, 'w') as f:
        f.write("this: that: invalid")

    with pytest.raises(yaml.YAMLError):
        load_config(str(invalid_path))


def test_get_nested_config():
    """Test retrieving nested configuration values."""
    config = {
        "postgres": {
            "host": "localhost",
            "port": 5432,
            "credentials": {
                "user": "postgres",
                "password": "secretpass"
            }
        },
        "als": {
            "rank": 10
        }
    }

    # Test successful retrieval
    assert get_nested_config(config, "postgres.host") == "localhost"
    assert get_nested_config(config, "postgres.port") == 5432
    assert get_nested_config(config, "postgres.credentials.user") == "postgres"
    assert get_nested_config(config, "als.rank") == 10

    # Test default values
    assert get_nested_config(config, "postgres.schema", "public") == "public"
    assert get_nested_config(config, "nonexistent.key", "default") == "default"
    assert get_nested_config(config, "postgres.username", None) is None

    # Test deeply nested nonexistent path
    assert get_nested_config(config, "a.very.deeply.nested.path", "default") == "default"

def test_setup_logging(tmp_path):
    """Test setting up logging configuration."""
    # Create a dummy config
    log_file = tmp_path / "test_log.log"
    config = {
        "logging": {
            "file": str(log_file),
            "level": "INFO"
        }
    }

    # Reset logging before testing
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Set up logging with our function
    setup_logging(config)

    # Check if root logger is configured correctly
    root_logger = logging.getLogger()
    assert root_logger.level == logging.INFO

    # Verify we have two handlers (file and console)
    assert len(root_logger.handlers) == 2

    # Identify file handler and stream handler
    file_handler = None
    stream_handler = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            file_handler = handler
        elif isinstance(handler, logging.StreamHandler):
            stream_handler = handler

    # Check that both handlers exist
    assert file_handler is not None, "No FileHandler found"
    assert stream_handler is not None, "No StreamHandler found"

    # Check file handler path
    assert file_handler.baseFilename == str(log_file)

    # Log a test message
    test_msg = "Test log message"
    logging.info(test_msg)

    # Verify log file was created and contains the message
    assert os.path.exists(log_file)
    with open(log_file, 'r') as f:
        log_content = f.read()
        assert test_msg in log_content
        # Check format with regex (timestamp - level - message)
        timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        expected_pattern = f"{timestamp_pattern} - INFO - {test_msg}"
        assert re.search(expected_pattern, log_content)


def test_setup_logging_invalid_level(tmp_path):
    """Test logging setup with invalid log level."""
    log_file = tmp_path / "test_invalid_level.log"
    config = {
        "logging": {
            "file": str(log_file),
            "level": "INVALID_LEVEL"  # Invalid level
        }
    }

    # Reset logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Should default to INFO when given invalid level
    setup_logging(config)
    assert logging.getLogger().level == logging.INFO


def test_setup_logging_create_directory(tmp_path):
    """Test that logging creates directory if it doesn't exist."""
    # Use a nested directory that doesn't exist yet
    log_dir = tmp_path / "logs" / "nested"
    log_file = log_dir / "test_log.log"

    config = {
        "logging": {
            "file": str(log_file),
            "level": "DEBUG"
        }
    }

    # Reset logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # This should create the directory
    setup_logging(config)

    # Verify directory was created
    assert os.path.exists(log_dir)

    # Log a message and verify it works
    logging.debug("Debug test message")
    assert os.path.exists(log_file)


if __name__ == "__main__":
    pytest.main()
