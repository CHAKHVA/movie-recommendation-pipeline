# import os
import pytest
from pyspark.sql import SparkSession, Row
from src.writer import write_to_postgres
from src.utils import load_config

# # Test DB config (use environment variables or a safe method)
# DB_CONFIG = {
#     "host": os.getenv("TEST_DB_HOST", "localhost"),
#     "port": int(os.getenv("TEST_DB_PORT", 5432)),
#     "database": os.getenv("TEST_DB_NAME", "test_moviedb"),
#     "user": os.getenv("TEST_DB_USER", "test_user"),
#     "password": os.getenv("TEST_DB_PASSWORD", "test_password"),
#     "schema": "public"
# }

# Load configuration from config.yaml
config = load_config("config.yaml")
DB_CONFIG = config["postgres"]

def test_write_to_postgres(spark_session):
    # Sample data
    data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=2, movieId=20, rating=3.5),
        Row(userId=3, movieId=30, rating=5.0)
    ]
    df = spark_session.createDataFrame(data)
    table_name = "test_recommendations"
    # Write DataFrame to PostgreSQL
    write_to_postgres(df, DB_CONFIG, table_name)
    # Read back via JDBC
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }
    read_df = spark_session.read.jdbc(
        url=jdbc_url,
        table=f"{DB_CONFIG['schema']}.{table_name}",
        properties=properties
    )
    # Assert count and content
    assert read_df.count() == df.count()
    results = {(row.userId, row.movieId, row.rating) for row in read_df.collect()}
    for row in df.collect():
        assert (row.userId, row.movieId, row.rating) in results
    # Test overwrite
    new_data = [
        Row(userId=4, movieId=40, rating=4.5),
        Row(userId=5, movieId=50, rating=3.0)
    ]
    new_df = spark_session.createDataFrame(new_data)
    write_to_postgres(new_df, DB_CONFIG, table_name)
    read_df = spark_session.read.jdbc(
        url=jdbc_url,
        table=f"{DB_CONFIG['schema']}.{table_name}",
        properties=properties
    )
    assert read_df.count() == new_df.count()
    results = {(row.userId, row.movieId, row.rating) for row in read_df.collect()}
    for row in new_df.collect():
        assert (row.userId, row.movieId, row.rating) in results
    # Cleanup table
    spark_session.sql(f"DROP TABLE IF EXISTS {DB_CONFIG['schema']}.{table_name}") 