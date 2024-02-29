from utils import *
from spark_session import get_spark_session
from ingester import read_csv_files
from transformer import tag_with_uuid, tag_with_ingestion_time
from persister import persist_into_delta_table

log_config_file = "./config/logging_config.json"
logger = setup_logging(log_config_file)


def main():
    logger.info("Starting Spark application")
    config = load_config("./config/config.yaml")

    spark = get_spark_session(app_name=config['spark']['app_name'])
    logger.info("SparkSession started successfully")

    input_path = config['data']['input_path']
    has_header = config['csv']['header']
    csv_dataframe = read_csv_files(spark, input_path, has_header)

    tagged_df = csv_dataframe \
        .transform(tag_with_uuid) \
        .transform(tag_with_ingestion_time)

    output_path = config['data']['output_delta_table_path']
    persist_into_delta_table(tagged_df, output_path)
    logger.info("Data loaded, enriched and persisted to delta table successfully")


if __name__ == "__main__":
    main()
