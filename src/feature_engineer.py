from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def join_dataframes(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    tags_df: Optional[DataFrame] = None,
    links_df: Optional[DataFrame] = None
) -> DataFrame:
    """
    Join ratings and movies DataFrames on movieId, and optionally join tags and links DataFrames.

    Args:
        ratings_df (DataFrame): DataFrame containing ratings data.
        movies_df (DataFrame): DataFrame containing movies data.
        tags_df (Optional[DataFrame]): DataFrame containing tags data.
        links_df (Optional[DataFrame]): DataFrame containing links data.

    Returns:
        DataFrame: Joined DataFrame with selected columns.
    """
    joined_df = ratings_df.join(movies_df, on="movieId", how="inner")
    if tags_df is not None:
        joined_df = joined_df.join(tags_df, on=["userId", "movieId"], how="left")
    if links_df is not None:
        joined_df = joined_df.join(links_df, on="movieId", how="left")
    # Select columns
    columns = ["userId", "movieId", "rating", "title", "genres"]
    if tags_df is not None:
        columns.append("tag")
    if links_df is not None:
        columns.extend(["imdbId", "tmdbId"])
    selected_df = joined_df.select(*columns)
    return selected_df
