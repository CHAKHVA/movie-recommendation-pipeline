from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame
import logging


def train_als_model(data_df: DataFrame, config: dict) -> ALS:
    """
    Train an ALS model using the provided DataFrame and configuration.

    Args:
        data_df (DataFrame): Input DataFrame with userId, movieId, rating columns.
        config (dict): Configuration dictionary with ALS parameters.

    Returns:
        ALSModel: Trained ALS model.
    """
    als_params = config.get("als", {})
    try:
        als = ALS(
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            rank=als_params.get("rank", 10),
            maxIter=als_params.get("maxIter", 10),
            regParam=als_params.get("regParam", 0.1),
            alpha=als_params.get("alpha", 1.0),
            coldStartStrategy=als_params.get("coldStartStrategy", "drop"),
            nonnegative=True
        )
        logging.info(f"Training ALS model with params: {als._input_kwargs}")
        model = als.fit(data_df)
        logging.info("ALS model training complete.")
        return model
    except Exception as e:
        logging.error(f"Error training ALS model: {e}")
        raise
