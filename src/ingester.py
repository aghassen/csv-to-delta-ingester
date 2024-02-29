from pyspark.sql import DataFrame


def read_csv_files(spark, input_path, has_header) -> DataFrame:
    """
    Read one or multiple csv files with or without header

    Parameters:
        - spark: SparkSession
        - input_path: Path to input data file
        - has_header: Boolean csv has header or not

    Returns: Dataframe containing data
    """
    csv_dataframe = spark.read.csv(input_path, header=has_header, inferSchema=True)
    return csv_dataframe
