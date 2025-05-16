from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split

def clean_movies(df: DataFrame) -> DataFrame:
    """
    Clean the movies DataFrame by dropping rows with null movieId and splitting genres into an array.

    Args:
        df (DataFrame): Input movies DataFrame.

    Returns:
        DataFrame: Cleaned movies DataFrame.
    """
    # Drop rows where movieId is null
    cleaned_df = df.dropna(subset=["movieId"])
    # Split genres string into array
    cleaned_df = cleaned_df.withColumn("genres", split(col("genres"), "\\|"))
    return cleaned_df

def clean_ratings(df: DataFrame) -> DataFrame:
    """
    Clean the ratings DataFrame by filtering valid rating range and dropping nulls in userId, movieId, or rating.

    Args:
        df (DataFrame): Input ratings DataFrame.

    Returns:
        DataFrame: Cleaned ratings DataFrame.
    """
    cleaned_df = df.dropna(subset=["userId", "movieId", "rating"])
    cleaned_df = cleaned_df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
    return cleaned_df

def clean_tags(df: DataFrame) -> DataFrame:
    """
    Clean the tags DataFrame by dropping nulls in userId/movieId, filtering null/empty tags, and dropping duplicates.

    Args:
        df (DataFrame): Input tags DataFrame.

    Returns:
        DataFrame: Cleaned tags DataFrame.
    """
    cleaned_df = df.dropna(subset=["userId", "movieId"])
    cleaned_df = cleaned_df.filter(col("tag").isNotNull() & (col("tag") != ""))
    cleaned_df = cleaned_df.dropDuplicates(["userId", "movieId", "tag"])
    return cleaned_df

def clean_links(df: DataFrame) -> DataFrame:
    """
    Clean the links DataFrame by dropping nulls in movieId, imdbId, and tmdbId.

    Args:
        df (DataFrame): Input links DataFrame.

    Returns:
        DataFrame: Cleaned links DataFrame.
    """
    cleaned_df = df.dropna(subset=["movieId", "imdbId", "tmdbId"])
    return cleaned_df
