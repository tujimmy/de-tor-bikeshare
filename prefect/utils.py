import datetime
from google.cloud.bigquery import SchemaField
from google.cloud import dataproc_v1
import json
import time

def round_to_nearest_10min(dt):
    """
    Rounds a given datetime object to the nearest 10 minutes.
    """
    rounded_minute = (dt.minute // 10) * 10
    dt_rounded = dt.replace(minute=rounded_minute, second=0, microsecond=0)
    if dt.minute % 10 >= 5:
        dt_rounded += datetime.timedelta(minutes=10)
    return dt_rounded


def read_local_config(config_file_path: str = "prefect/config/config.json"):
    with open(config_file_path, "r") as file:
        config = json.load(file)
    return config

def get_dataproc_client(gcp_key: dict, region: str) -> dataproc_v1.ClusterControllerClient:
    credentials = gcp_key.get_credentials_from_service_account()
    client_options = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    return dataproc_v1.ClusterControllerClient(credentials=credentials, client_options=client_options)


def get_cluster_status(gcp_key, project_id, region, cluster_name):
    # Create credentials using the gcp_key
    credentials = gcp_key.get_credentials_from_service_account()

    # Create the Dataproc client with the credentials
    dataproc_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"},
        credentials=credentials
    )

    # Get the cluster status
    cluster_status = dataproc_client.get_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    ).status.state

    return cluster_status

def wait_for_cluster_state(client: dataproc_v1.ClusterControllerClient, project_id: str, region: str, cluster_name: str, target_state: dataproc_v1.ClusterStatus.State, poll_interval_secs: int = 5):
    while True:
        cluster = client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        if cluster.status.state == target_state:
            break
        time.sleep(poll_interval_secs)