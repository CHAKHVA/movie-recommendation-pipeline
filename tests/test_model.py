import pytest
from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALSModel
from src.model import train_als_model

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("pytest-model").getOrCreate()
    yield spark
    spark.stop()

def test_train_als_model(spark_session):
    # Sample data
    data = [
        Row(userId=1, movieId=10, rating=4.0),
        Row(userId=1, movieId=20, rating=3.5),
        Row(userId=2, movieId=10, rating=5.0),
        Row(userId=2, movieId=30, rating=4.0),
        Row(userId=3, movieId=20, rating=3.0),
        Row(userId=3, movieId=30, rating=4.5)
    ]
    df = spark_session.createDataFrame(data)
    # Dummy config
    config = {
        "als": {
            "rank": 2,
            "maxIter": 5,
            "regParam": 0.1,
            "alpha": 1.0,
            "coldStartStrategy": "drop"
        }
    }
    model = train_als_model(df, config)
    assert isinstance(model, ALSModel)
    # Basic transform test
    predictions = model.transform(df)
    assert predictions.count() == df.count()
    assert "prediction" in predictions.columns 