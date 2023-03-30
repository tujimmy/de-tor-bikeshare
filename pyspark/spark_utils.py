import argparse
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict

def create_spark_session(app_name: str, gcs_bucket: str = None) -> SparkSession:
    """
    Create a Spark session with the given app_name and configure the temporary GCS bucket if provided.
    :param app_name: The name of the Spark application.
    :param gcs_bucket: The GCS bucket name for temporary BigQuery export data (optional).
    :return: A SparkSession object.
    """
    spark = SparkSession.builder.master('yarn').appName(app_name).getOrCreate()
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    if gcs_bucket:
        spark.conf.set('temporaryGcsBucket', gcs_bucket)
    return spark

def read_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Read data from a Parquet file.
    :param spark: A SparkSession object.
    :param path: The path to the Parquet file.
    :return: A DataFrame with the Parquet data.
    """
    return spark.read.format("parquet").option("mergeSchema", "true").load(path)


def parse_arguments(arguments: List[str]) -> Dict[str, str]:
    """
    Parses command line arguments for target_date, bucket, project_id, and dataset_id.

    :param arguments: A list of command line arguments.
    :return: A dictionary containing the parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_date', type=str, required=True,
                        help="The target date in format 'YYYY-MM-DD' to process data.")
    parser.add_argument('--bucket', type=str, required=True,
                        help="The Google Cloud Storage bucket name to be used for temporary storage.")
    parser.add_argument('--project_id', type=str, required=True,
                        help="The Google Cloud project ID.")
    parser.add_argument('--dataset_id', type=str, required=True,
                        help="The BigQuery dataset ID where the data will be processed and stored.")
    args, _ = parser.parse_known_args(arguments)
    return vars(args)