import pytest
from pyspark.sql import SparkSession, Row
from src.feature_engineer import join_dataframes

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("pytest-feature-engineer").getOrCreate()
    yield spark
    spark.stop()

def test_join_dataframes_basic(spark_session):
    ratings_data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=2, movieId=20, rating=3.5),
        Row(userId=3, movieId=30, rating=5.0)
    ]
    movies_data = [
        Row(movieId=10, title="Movie A", genres=["Action", "Comedy"]),
        Row(movieId=20, title="Movie B", genres=["Drama"]),
        Row(movieId=40, title="Movie C", genres=["Thriller"])
    ]
    ratings_df = spark_session.createDataFrame(ratings_data)
    movies_df = spark_session.createDataFrame(movies_data)
    joined = join_dataframes(ratings_df, movies_df)
    # Should only join on movieId 10 and 20
    assert joined.count() == 2
    expected_schema = {"userId", "movieId", "rating", "title", "genres"}
    assert set(joined.columns) == expected_schema
    results = {(row.userId, row.movieId, row.title) for row in joined.collect()}
    assert (1, 10, "Movie A") in results
    assert (2, 20, "Movie B") in results 

def test_join_dataframes_with_tags(spark_session):
    ratings_data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=2, movieId=20, rating=3.5),
        Row(userId=3, movieId=30, rating=5.0)
    ]
    movies_data = [
        Row(movieId=10, title="Movie A", genres=["Action", "Comedy"]),
        Row(movieId=20, title="Movie B", genres=["Drama"]),
        Row(movieId=40, title="Movie C", genres=["Thriller"])
    ]
    tags_data = [
        Row(userId=1, movieId=10, tag="funny"),
        Row(userId=2, movieId=20, tag="sad")
    ]
    ratings_df = spark_session.createDataFrame(ratings_data)
    movies_df = spark_session.createDataFrame(movies_data)
    tags_df = spark_session.createDataFrame(tags_data)
    joined = join_dataframes(ratings_df, movies_df, tags_df=tags_df)
    assert joined.count() == 2
    expected_schema = {"userId", "movieId", "rating", "title", "genres", "tag"}
    assert set(joined.columns) == expected_schema
    results = {(row.userId, row.movieId, row.tag) for row in joined.collect()}
    assert (1, 10, "funny") in results
    assert (2, 20, "sad") in results

def test_join_dataframes_all(spark_session):
    ratings_data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=2, movieId=20, rating=3.5),
        Row(userId=3, movieId=30, rating=5.0)
    ]
    movies_data = [
        Row(movieId=10, title="Movie A", genres=["Action", "Comedy"]),
        Row(movieId=20, title="Movie B", genres=["Drama"]),
        Row(movieId=40, title="Movie C", genres=["Thriller"])
    ]
    tags_data = [
        Row(userId=1, movieId=10, tag="funny"),
        Row(userId=2, movieId=20, tag="sad")
    ]
    links_data = [
        Row(movieId=10, imdbId="tt0000010", tmdbId=10010),
        Row(movieId=20, imdbId="tt0000020", tmdbId=10020)
    ]
    ratings_df = spark_session.createDataFrame(ratings_data)
    movies_df = spark_session.createDataFrame(movies_data)
    tags_df = spark_session.createDataFrame(tags_data)
    links_df = spark_session.createDataFrame(links_data)
    joined = join_dataframes(ratings_df, movies_df, tags_df=tags_df, links_df=links_df)
    assert joined.count() == 2
    expected_schema = {"userId", "movieId", "rating", "title", "genres", "tag", "imdbId", "tmdbId"}
    assert set(joined.columns) == expected_schema
    results = {(row.userId, row.movieId, row.tag, row.imdbId, row.tmdbId) for row in joined.collect()}
    assert (1, 10, "funny", "tt0000010", 10010) in results
    assert (2, 20, "sad", "tt0000020", 10020) in results 