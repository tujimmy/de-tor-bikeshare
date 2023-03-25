from google.cloud import dataproc_v1 as dataproc
from prefect import flow, task
from prefect_gcp.bigquery import GcpCredentials, bigquery_create_table
from prefect_gcp import GcpCredentials
from datetime import datetime, timedelta
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud.bigquery import TimePartitioning
from utils import create_station_infomation_schema, create_station_status_schema


@task(log_prints=True)
def submit_batch(
    job_name: str,
    python_file: str,
    target_date: str,
    cluster_name: str,
    project_id: str,
    region: str,
):
    gcp = GcpCredentials.load("zoom-gcs-creds")

    jar = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
    job_client = dataproc.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    current_timestamp = round(datetime.datetime.now().timestamp())
    job = {
        "placement": {
            "cluster_name": cluster_name
        },
        "reference": {
            "job_id": f"job-{job_name}---{current_timestamp}",
            "project_id": project_id
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://dtc_data_lake_root-welder-375217/code/{python_file}",
            "properties": {},
            "args": [
                f"--target_date={target_date}"
            ],
            "jar_file_uris": [
                jar
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
        target_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    gcp = GcpCredentials.load("zoom-gcs-creds")

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

    # Defining common variables to submit spark job
    project_id = "root-welder-375217"
    cluster_name = "cluster-62de"
    region = "us-central1"

    submit_batch(
        job_name="load_stg_station_status",
        python_file="load_stg_station_status.py",
        target_date=target_date,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region
    )

    submit_batch(
        job_name="load_stg_station_infomation",
        python_file="load_stg_station_infomation.py",
        target_date=target_date,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region
    )


if __name__ == '__main__':
    load_staging_tables()
