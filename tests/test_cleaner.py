import pytest
from pyspark.sql import SparkSession, Row
from src.cleaner import clean_movies, clean_ratings, clean_tags, clean_links

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("pytest-cleaner").getOrCreate()
    yield spark
    spark.stop()

def test_clean_movies(spark_session):
    data = [
        Row(movieId=1, title="Toy Story (1995)", genres="Adventure|Animation|Children|Comedy|Fantasy"),
        Row(movieId=None, title="Missing MovieId", genres="Comedy|Drama"),
        Row(movieId=2, title="Jumanji (1995)", genres="Adventure|Children|Fantasy")
    ]
    df = spark_session.createDataFrame(data)
    cleaned = clean_movies(df)
    # Should drop the row with null movieId
    assert cleaned.count() == 2
    # genres should be array type
    genres_type = dict((f.name, f.dataType.typeName()) for f in cleaned.schema.fields)["genres"]
    assert genres_type == "array"
    # Check content of genres array
    genres_lists = [row.genres for row in cleaned.collect()]
    assert ["Animation", "Children"] in genres_lists[0] or ["Children", "Fantasy"] in genres_lists[1]

def test_clean_ratings(spark_session):
    data = [
        Row(userId=1, movieId=1, rating=4.0),
        Row(userId=2, movieId=2, rating=0.0),  # out of range
        Row(userId=3, movieId=3, rating=5.0),
        Row(userId=4, movieId=4, rating=5.5),  # out of range
        Row(userId=None, movieId=5, rating=3.0),  # null userId
        Row(userId=5, movieId=None, rating=3.0),  # null movieId
        Row(userId=6, movieId=6, rating=None),  # null rating
        Row(userId=7, movieId=7, rating=0.5),  # valid lower bound
    ]
    df = spark_session.createDataFrame(data)
    cleaned = clean_ratings(df)
    # Should only keep rows with valid userId, movieId, rating and rating in [0.5, 5.0]
    result = cleaned.select("userId", "movieId", "rating").collect()
    assert len(result) == 3
    ratings = [row.rating for row in result]
    assert 4.0 in ratings
    assert 5.0 in ratings
    assert 0.5 in ratings 

def test_clean_tags(spark_session):
    data = [
        Row(userId=1, movieId=1, tag="funny"),
        Row(userId=2, movieId=2, tag=""),  # empty tag
        Row(userId=3, movieId=3, tag=None),  # null tag
        Row(userId=None, movieId=4, tag="action"),  # null userId
        Row(userId=5, movieId=None, tag="drama"),  # null movieId
        Row(userId=6, movieId=6, tag="funny"),  # duplicate tag for same user/movie
        Row(userId=6, movieId=6, tag="funny"),  # duplicate tag for same user/movie
        Row(userId=7, movieId=7, tag="thriller"),
    ]
    df = spark_session.createDataFrame(data)
    cleaned = clean_tags(df)
    # Should only keep rows with valid userId, movieId, non-empty tag, and no duplicates
    result = cleaned.select("userId", "movieId", "tag").collect()
    assert len(result) == 3
    tags = [row.tag for row in result]
    assert "funny" in tags
    assert "thriller" in tags
    assert "drama" in tags 

def test_clean_links(spark_session):
    data = [
        Row(movieId=1, imdbId="tt0114709", tmdbId=862),
        Row(movieId=2, imdbId=None, tmdbId=8844),  # null imdbId
        Row(movieId=None, imdbId="tt0113497", tmdbId=105),  # null movieId
        Row(movieId=4, imdbId="tt0113228", tmdbId=None),  # null tmdbId
        Row(movieId=5, imdbId="tt0113497", tmdbId=105),
    ]
    df = spark_session.createDataFrame(data)
    cleaned = clean_links(df)
    # Should only keep rows with valid movieId, imdbId, and tmdbId
    result = cleaned.select("movieId", "imdbId", "tmdbId").collect()
    assert len(result) == 2
    movie_ids = [row.movieId for row in result]
    assert 1 in movie_ids
    assert 5 in movie_ids 