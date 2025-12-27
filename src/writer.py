import logging

from pyspark.sql import DataFrame


def write_to_postgres(df: DataFrame, db_config: dict, table_name: str) -> None:
    """
    Write a DataFrame to a PostgreSQL table using JDBC.

    Args:
        df (DataFrame): DataFrame to write.
        db_config (dict): Database configuration dictionary with host, port, database, user, password, schema.
        table_name (str): Name of the target table.

    Raises:
        Exception: If writing to PostgreSQL fails.
    """
    try:
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
        properties = {
            "user": db_config["user"],
            "password": db_config["password"],
            "driver": "org.postgresql.Driver",
        }
        df.write.jdbc(
            url=jdbc_url,
            table=f"{db_config['schema']}.{table_name}",
            mode="overwrite",
            properties=properties,
        )
        logging.info(
            f"Successfully wrote DataFrame to PostgreSQL table {db_config['schema']}.{table_name}."
        )
    except Exception as e:
        logging.error(f"Error writing DataFrame to PostgreSQL: {e}")
        raise
