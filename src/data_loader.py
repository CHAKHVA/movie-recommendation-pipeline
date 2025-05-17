import logging
import os
from typing import Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.errors.exceptions.base import AnalysisException, ParseException


def load_csv_data(
    spark: SparkSession, path: str, options: dict[str, Any] | None = None
) -> DataFrame:
    """
    Load data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        path (str): Path to the CSV file. Can be a local path or a supported
                    remote path (e.g., s3://, hdfs://).
        options (dict[str, Any] | None): Dictionary of options to pass to the CSV reader.
                    Defaults to None, which uses standard options (header=True, inferSchema=True).

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.

    Raises:
        AnalysisException: If the file doesn't exist or can't be accessed.
        ParseException: If there's an error parsing the CSV file.
        ValueError: If the path is invalid or empty.

    Example:
        >>> spark = get_spark_session()
        >>> movies_df = load_csv_data(spark, "data/movies.csv")
        >>> movies_df.show(5)
    """
    if not path:
        raise ValueError("CSV path cannot be empty")

    # Log the loading attempt
    logging.info(f"Loading CSV data from: {path}")

    # Set default options
    read_options = {"header": "true", "inferSchema": "true"}

    # Update with any user-provided options
    if options:
        read_options.update(options)

    try:
        # Check if the file exists (for local files)
        if path.startswith("file://") or (
            not path.startswith("s3://") and not path.startswith("hdfs://")
        ):
            local_path = path.replace("file://", "")
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"CSV file not found at path: {local_path}")

        # Load the CSV file
        df = spark.read.options(**read_options).csv(path)

        # Log successful loading
        row_count = df.count()
        column_count = len(df.columns)
        logging.info(
            f"Successfully loaded CSV data: {row_count} rows, {column_count} columns"
        )
        logging.debug(f"DataFrame schema: {df.schema}")

        return df

    except FileNotFoundError as e:
        logging.error(f"File not found: {str(e)}")
        raise

    except ParseException as e:
        logging.error(f"Error parsing CSV file: {str(e)}")
        raise

    except AnalysisException as e:
        logging.error(f"Spark analysis error loading CSV: {str(e)}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error loading CSV file: {str(e)}")
        raise
