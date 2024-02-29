import pyspark.sql.functions as sf


def tag_with_uuid(df):
    """
    add column batch_id with generated UUID v4

    Parameters:
        - df : dataframe to be tagged

    Returns:
        - Dataframe tagged with UUID v4 id
    """
    return df.withColumn("batch_id", sf.expr("uuid()"))


def tag_with_ingestion_time(df):
    """
        add column ingestion_tms with ingestion timestamp in format : YYYY-MM-DD HH:mm:SS

        Parameters:
            - df : dataframe to be tagged

        Returns:
            -Dataframe tagged with ingestion_tms
        """
    return df.withColumn("ingestion_tms", sf.date_format(sf.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
