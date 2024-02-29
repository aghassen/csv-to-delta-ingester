from pyspark.sql import SparkSession


def get_spark_session(app_name="DefaultSparkApp"):
    """
    Initialize and return a Spark session.

    Parameters:
        - app_name (str): The name of the Spark application.

    Returns:
        - SparkSession: An initialized Spark session.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    return spark
