import pytest
from pyspark.sql import SparkSession
from src.ingester import read_csv_files
from src.utils import are_dataframes_equal


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


def test_read_csv_file_with_header(spark_session):
    csv_with_header_path = "./data/input_file_with_header.csv"
    has_header = True
    result = read_csv_files(spark_session, csv_with_header_path, has_header)

    expected_data = [(1, "Ghassen", 25), (2, "Houssem", 30), (3, "Mohsen", 28)]
    expected_schema = ["id", "name", "age"]
    expected_result = spark_session.createDataFrame(expected_data, schema=expected_schema)

    assert are_dataframes_equal(result, expected_result)


def test_read_csv_file_without_header(spark_session):
    csv_without_header_path = "./data/files_without_header/input_file_without_header.csv"
    has_header = False
    result = read_csv_files(spark_session, csv_without_header_path, has_header)

    expected_data = [(1, "Ghassen", 25), (2, "Houssem", 30), (3, "Mohsen", 28)]
    expected_schema = ["_c0", "_c1", "_c2"]
    expected_result = spark_session.createDataFrame(expected_data, schema=expected_schema)

    assert are_dataframes_equal(result, expected_result)


def test_read_multiple_csv_files_in_directory(spark_session):
    csv_without_header_path = "./data/files_without_header"
    has_header = False
    result = read_csv_files(spark_session, csv_without_header_path, has_header)

    expected_data = [(1, "Ghassen", 25), (2, "Houssem", 30), (3, "Mohsen", 28), (4, "Saker", 15), (5, "Alexis", 20)]
    expected_schema = ["_c0", "_c1", "_c2"]
    expected_result = spark_session.createDataFrame(expected_data, schema=expected_schema)

    assert result.count() == 5
    assert are_dataframes_equal(result, expected_result)
