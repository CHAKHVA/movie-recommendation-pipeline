from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def join_dataframes(ratings_df: DataFrame, movies_df: DataFrame) -> DataFrame:
    """
    Join ratings and movies DataFrames on movieId and select relevant columns.

    Args:
        ratings_df (DataFrame): DataFrame containing ratings data.
        movies_df (DataFrame): DataFrame containing movies data.

    Returns:
        DataFrame: Joined DataFrame with selected columns.
    """
    joined_df = ratings_df.join(movies_df, on="movieId", how="inner")
    # Select relevant columns
    selected_df = joined_df.select(
        "userId",
        "movieId",
        "rating",
        "title",
        "genres"
    )
    return selected_df
