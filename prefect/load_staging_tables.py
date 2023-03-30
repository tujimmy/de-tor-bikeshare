from datetime import datetime, timedelta

from google.cloud import dataproc_v1 as dataproc
from google.cloud.bigquery import TimePartitioning

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from schema import (create_bike_availability_schema,
                    create_station_infomation_schema,
                    create_station_status_schema)
from utils import read_local_config

@task(log_prints=True)
def submit_batch(
    job_name: str,
    python_file: str,
    target_date: str,
    bucket: str,
    cluster_name: str,
    project_id: str,
    region: str,
    dataset_id: str,
):
    gcp = GcpCredentials.load("zoom-gcs-creds")

    jar = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
    job_client = dataproc.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    current_timestamp = round(datetime.now().timestamp())
    job = {
        "placement": {
            "cluster_name": cluster_name
        },
        "reference": {
            "job_id": f"job-{job_name}---{current_timestamp}",
            "project_id": project_id
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket}/code/{python_file}",
            "properties": {},
            "args": [
                f"--target_date={target_date}",
                f"--bucket={bucket}",
                f"--project_id={project_id}",
                f"--dataset_id={dataset_id}",
            ],
            "jar_file_uris": [
                jar
            ],
            "python_file_uris": [
                f"gs://{bucket}/code/spark_utils.py"
            ]
        }
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    return operation.result()


@flow()
def load_staging_tables(target_date: str = None) -> None:
    """The main ETL function"""
    if not target_date:
        target_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    gcp = GcpCredentials.load("zoom-gcs-creds")
    config = read_local_config()

    # Creating staging table for station_status
    bigquery_create_table(
        dataset="bikeshare",
        table="stg_station_status",
        schema=create_station_status_schema(),
        gcp_credentials=gcp,
        clustering_fields=["station_id", "hour"],
        time_partitioning=TimePartitioning(field="date"),
    )

    # Creating staging table for station_information
    bigquery_create_table(
        dataset="bikeshare",
        table="stg_station_infomation",
        schema=create_station_infomation_schema(),
        gcp_credentials=gcp
    )

    # Creating staging table for bike_availability
    bigquery_create_table(
        dataset="bikeshare",
        table="bike_availability",
        schema=create_bike_availability_schema(),
        gcp_credentials=gcp,
        clustering_fields=["station_id"],
        time_partitioning=TimePartitioning(field="date"),
    )

    # Defining common variables to submit spark job   
    project_id = config['project_id']
    cluster_name = config['cluster_name']
    region = config['region']
    bucket = config['bucket']
    dataset_id = config['dataset_id']

    submit_batch(
        job_name="load_stg_station_status",
        python_file="load_stg_station_status.py",
        target_date=target_date,
        bucket=bucket,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region,
        dataset_id=dataset_id
    )

    submit_batch(
        job_name="load_stg_station_infomation",
        python_file="load_stg_station_infomation.py",
        target_date=target_date,
        bucket=bucket,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region,
        dataset_id=dataset_id
    )

    submit_batch(
        job_name="load_bike_availability",
        python_file="load_bike_availability.py",
        target_date=target_date,
        bucket=bucket,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region,
        dataset_id=dataset_id
    )


if __name__ == '__main__':
    load_staging_tables()
