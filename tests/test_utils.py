import pytest
import yaml
from src.utils import load_config, get_nested_config


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


if __name__ == "__main__":
    pytest.main()
