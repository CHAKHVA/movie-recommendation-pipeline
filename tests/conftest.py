import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="session")
def spark_session():
    # Get the current test function name
    test_name = os.environ.get('PYTEST_CURRENT_TEST', '').split(':')[-1].split(' ')[0]
    
    # Create a unique app name based on the test function
    app_name = f"pytest-{test_name}"
    
    # Create Spark session with PostgreSQL JDBC driver
    spark = (SparkSession.builder
            .master("local[1]")
            .appName(app_name)
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate())
    
    yield spark
    
    # Clean up
    spark.stop()
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession() 