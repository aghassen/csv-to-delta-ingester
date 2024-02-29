def persist_into_delta_table(dataframe, output_path):
    """
    Persist dataframe to delta table in output path with mode append

    Parameters:
        - dataframe: dataframe containing data to be persisted
        - output_path: Path to output delta table

    Returns: Dataframe Writer
    """
    dataframe.write.format("delta").mode("append").save(output_path)
