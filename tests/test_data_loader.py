import os
import pytest
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import AnalysisException
from src.data_loader import load_csv_data

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("pytest-movie-pipeline").getOrCreate()
    yield spark
    spark.stop()

def test_load_csv_data(spark_session):
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
    with pytest.raises(Exception):
        load_csv_data(spark_session, "tests/data/nonexistent.csv") 