import logging
import sys
import time
import os

from src.utils import load_config, setup_logging, get_spark_session
from src.data_loader import load_csv_data
from src.cleaner import clean_movies, clean_ratings, clean_tags, clean_links
from src.feature_engineer import join_dataframes
from src.model import train_als_model

# Path to the configuration file
CONFIG_PATH = "config.yaml"


def run_pipeline(config):
    """
    Execute the movie recommendation pipeline with the given configuration.

    Args:
        config (dict): The configuration dictionary.

    Returns:
        bool: True if pipeline completes successfully, False otherwise.
    """
    # This is a placeholder for the actual pipeline implementation
    # Future steps will be added here
    logging.info("Processing pipeline steps...")

    # Simulate some processing time
    time.sleep(1)

    # In the future, this function will call modules for:
    # - Data loading
    # - Data cleaning
    # - Feature engineering
    # - Model training
    # - Recommendation generation
    # - Result writing

    return True


if __name__ == "__main__":
    try:
        # Start timing the pipeline
        start_time = time.time()

        # Load configuration
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        logging.info(f"Loading configuration from {CONFIG_PATH}...")
        config = load_config(CONFIG_PATH)

        # Set up proper logging based on configuration
        setup_logging(config)

        # Log pipeline start
        logging.info("=" * 50)
        logging.info("Movie Recommendation Pipeline Started")
        logging.info("=" * 50)

        spark = get_spark_session(config)
        try:
            # Load movies dataset
            movies_path = config["storage"][config["storage"]["mode"]]["movies"]
            movies_df = load_csv_data(spark, movies_path)
            logging.info(f"Movies DataFrame schema: {movies_df.schema}")
            logging.info(f"Movies DataFrame row count: {movies_df.count()}")
            # Clean movies
            cleaned_movies = clean_movies(movies_df)
            logging.info(f"Cleaned Movies DataFrame row count: {cleaned_movies.count()}")

            # Load ratings dataset
            ratings_path = config["storage"][config["storage"]["mode"]]["ratings"]
            ratings_df = load_csv_data(spark, ratings_path)
            logging.info(f"Ratings DataFrame schema: {ratings_df.schema}")
            logging.info(f"Ratings DataFrame row count: {ratings_df.count()}")
            # Clean ratings
            cleaned_ratings = clean_ratings(ratings_df)
            logging.info(f"Cleaned Ratings DataFrame row count: {cleaned_ratings.count()}")

            # Load tags dataset
            tags_path = config["storage"][config["storage"]["mode"]]["tags"]
            tags_df = load_csv_data(spark, tags_path)
            logging.info(f"Tags DataFrame schema: {tags_df.schema}")
            logging.info(f"Tags DataFrame row count: {tags_df.count()}")
            # Clean tags
            cleaned_tags = clean_tags(tags_df)
            logging.info(f"Cleaned Tags DataFrame row count: {cleaned_tags.count()}")

            # Load links dataset
            links_path = config["storage"][config["storage"]["mode"]]["links"]
            links_df = load_csv_data(spark, links_path)
            logging.info(f"Links DataFrame schema: {links_df.schema}")
            logging.info(f"Links DataFrame row count: {links_df.count()}")
            # Clean links
            cleaned_links = clean_links(links_df)
            logging.info(f"Cleaned Links DataFrame row count: {cleaned_links.count()}")

            # Join all cleaned DataFrames
            joined_df = join_dataframes(
                cleaned_ratings,
                cleaned_movies,
                tags_df=cleaned_tags,
                links_df=cleaned_links
            )
            logging.info("Successfully joined ratings, movies, tags, and links DataFrames.")
            logging.info(f"Joined DataFrame schema: {joined_df.schema}")
            logging.info(f"Joined DataFrame row count: {joined_df.count()}")
            logging.info(f"Sample rows from joined DataFrame: {joined_df.show(5, truncate=False)}")

            # Train ALS model
            model = train_als_model(joined_df, config)
            logging.info("ALS model training complete.")
            # Optional: Save model
            if not os.path.exists("models"):
                os.makedirs("models")
            model.save("models/als_model")
            logging.info("ALS model saved to models/als_model.")

            # Run the pipeline (placeholder)
            success = run_pipeline(config)
        finally:
            spark.stop()
            logging.info("SparkSession stopped.")

        # Calculate execution time
        execution_time = time.time() - start_time

        if success:
            logging.info("=" * 50)
            logging.info(
                f"Pipeline completed successfully in {execution_time:.2f} seconds"
            )
            logging.info("=" * 50)
            sys.exit(0)
        else:
            logging.error("Pipeline failed to complete successfully")
            sys.exit(1)

    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")
        sys.exit(1)
