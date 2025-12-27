"""
Tests for data loading functionality.

Note on S3 Testing:
    This module includes tests for both local and S3 data loading. For S3 testing, there are two approaches:
    1. Integration Testing (Recommended for CI/CD):
       - Requires actual AWS credentials and a test S3 bucket
       - Set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
       - Use a dedicated test bucket with test data
       - Mark tests with @pytest.mark.integration

    2. Mock Testing (Recommended for local development):
       - Use moto library to mock S3
       - No AWS credentials required
       - Faster and more isolated
       - Example setup:
         ```python
         @pytest.fixture
         def s3_mock():
             with moto.mock_s3():
                 s3 = boto3.client('s3')
                 s3.create_bucket(Bucket='test-bucket')
                 yield s3
         ```
"""

import os

import pytest

from src.data_loader import load_csv_data


def test_load_csv_data(spark_session):
    """Test loading CSV data from local filesystem."""
    sample_path = os.path.join(os.path.dirname(__file__), "data/sample_movies.csv")
    df = load_csv_data(spark_session, sample_path)
    # Check row count
    assert df.count() == 3
    # Check schema
    expected_fields = {"movieId", "title", "genres"}
    assert set(df.columns) == expected_fields
    # Check types (movieId should be int, title and genres should be string)
    schema = dict((f.name, f.dataType.typeName()) for f in df.schema.fields)
    assert schema["movieId"] in ("int", "integer", "long")
    assert schema["title"] == "string"
    assert schema["genres"] == "string"


def test_load_csv_data_not_found(spark_session):
    """Test handling of non-existent local file."""
    with pytest.raises(Exception):
        load_csv_data(spark_session, "tests/data/nonexistent.csv")


@pytest.mark.integration
def test_load_csv_data_from_s3(spark_session):
    """
    Test loading CSV data from S3.

    This test requires:
    1. AWS credentials set in environment variables:
       - AWS_ACCESS_KEY_ID
       - AWS_SECRET_ACCESS_KEY
       - AWS_SESSION_TOKEN (optional)

    2. Test data uploaded to S3:
       - Bucket: dev-movie-ratings-data
       - Path: data/test/sample_movies.csv

    To run this test:
    ```bash
    # Set AWS credentials
    export AWS_ACCESS_KEY_ID=your_access_key
    export AWS_SECRET_ACCESS_KEY=your_secret_key

    # Run only integration tests
    pytest -v -m integration tests/test_data_loader.py
    ```
    """
    s3_path = "s3a://dev-movie-ratings-data/data/test/sample_movies.csv"
    df = load_csv_data(spark_session, s3_path)

    # Check row count
    assert df.count() == 3
    # Check schema
    expected_fields = {"movieId", "title", "genres"}
    assert set(df.columns) == expected_fields
    # Check types
    schema = dict((f.name, f.dataType.typeName()) for f in df.schema.fields)
    assert schema["movieId"] in ("int", "integer", "long")
    assert schema["title"] == "string"
    assert schema["genres"] == "string"
