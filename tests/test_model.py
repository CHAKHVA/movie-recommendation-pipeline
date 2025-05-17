import pytest
from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALSModel
from src.model import train_als_model, generate_recommendations


def test_train_als_model(spark_session):
    # Sample data
    data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=1, movieId=20, rating=3.5),
        Row(userId=2, movieId=10, rating=5.0),
        Row(userId=2, movieId=30, rating=4.0),
        Row(userId=3, movieId=20, rating=3.0),
        Row(userId=3, movieId=30, rating=4.5),
    ]
    df = spark_session.createDataFrame(data)
    # Dummy config
    config = {
        "als": {
            "rank": 2,
            "maxIter": 5,
            "regParam": 0.1,
            "alpha": 1.0,
            "coldStartStrategy": "drop",
        }
    }
    model = train_als_model(df, config)
    assert isinstance(model, ALSModel)
    # Basic transform test
    predictions = model.transform(df)
    assert predictions.count() == df.count()
    assert "prediction" in predictions.columns


def test_generate_recommendations(spark_session):
    # Sample data
    data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=1, movieId=20, rating=3.5),
        Row(userId=2, movieId=10, rating=5.0),
        Row(userId=2, movieId=30, rating=4.0),
        Row(userId=3, movieId=20, rating=3.0),
        Row(userId=3, movieId=30, rating=4.5),
    ]
    df = spark_session.createDataFrame(data)
    # Dummy config
    config = {
        "als": {
            "rank": 2,
            "maxIter": 5,
            "regParam": 0.1,
            "alpha": 1.0,
            "coldStartStrategy": "drop",
        }
    }
    model = train_als_model(df, config)
    top_n = 2
    recommendations = generate_recommendations(model, top_n)
    # Assert schema
    expected_schema = {"userId", "movieId", "predicted_rating"}
    assert set(recommendations.columns) == expected_schema
    # Assert count per user <= top_n
    user_counts = recommendations.groupBy("userId").count().collect()
    for row in user_counts:
        assert row["count"] <= top_n
    # Assert rating type
    for row in recommendations.collect():
        assert isinstance(row.predicted_rating, float)
