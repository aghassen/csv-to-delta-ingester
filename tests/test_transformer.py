import pytest
from pyspark.sql import SparkSession
from src.transformer import tag_with_uuid, tag_with_ingestion_time


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


def test_tag_with_uuid(spark_session):
    data = [(1, "Alice", 25), (2, "Bob", 30)]
    schema = ["id", "name", "age"]
    df = spark_session.createDataFrame(data, schema=schema)

    tagged_df = df.transform(tag_with_uuid)

    assert "batch_id" in tagged_df.columns


def test_tag_with_ingestion_time(spark_session):
    data = [(1, "Alice", 25), (2, "Bob", 30)]
    schema = ["id", "name", "age"]
    df1 = spark_session.createDataFrame(data, schema=schema)

    tagged_df = df1.transform(tag_with_ingestion_time)

    assert "ingestion_tms" in tagged_df.columns
