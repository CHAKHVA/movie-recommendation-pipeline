import pytest
from pyspark.sql import SparkSession
import os
import logging
import sys

# Configure Py4J logging to use stderr instead of a file
logging.getLogger("py4j").setLevel(logging.WARNING)
py4j_handler = logging.StreamHandler(sys.stderr)
py4j_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logging.getLogger("py4j").addHandler(py4j_handler)


@pytest.fixture(scope="function")
def spark_session():
    # Get the current test function name
    test_name = os.environ.get("PYTEST_CURRENT_TEST", "").split(":")[-1].split(" ")[0]

    # Create a unique app name based on the test function
    app_name = f"pytest-{test_name}"

    # Create Spark session with PostgreSQL JDBC driver
    spark = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
        .getOrCreate()
    )

    yield spark

    # Clean up
    spark.stop()

    # Remove Py4J handler to prevent logging after cleanup
    for handler in logging.getLogger("py4j").handlers[:]:
        logging.getLogger("py4j").removeHandler(handler)
