from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import requests
import json
from pathlib import Path
import datetime
from utils import read_local_config

@task(log_prints=True, retries=3, retry_delay_seconds=30)
def fetch_api(api_url: str) -> json:
    response = requests.get(api_url)
    return response.json()['data']['stations']


@task(log_prints=True)
def flatten_json(data: json) -> pd.DataFrame:
    now = datetime.datetime.now()
    date_string = now.strftime("%Y-%m-%d")

    flat_data = []
    for station in data:
        flat_station = {
            'station_id': station['station_id'],
            'name': station['name'],
            'physical_configuration': station['physical_configuration'],
            'lat': station['lat'],
            'lon': station['lon'],
            'address': station['address'],
            'capacity': station['capacity'],
            'is_charging_station': station['is_charging_station'],
            'is_virtual_station': station['is_virtual_station'],
            'date': date_string
        }

        flat_data.append(flat_station)
    df = pd.DataFrame(flat_data)
    return df


@task(log_prints=True, retries=3, retry_delay_seconds=30)
def write_gcs(df: pd.DataFrame, path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df=df,
        to_path=path,
        serialization_format='parquet'
    )


@flow()
def etl_api_to_gcs(dt: str = None) -> None:
    """The main ETL function"""
    config = read_local_config()
    bucket = config['bucket']
    api_url = config['station_infomation_url']
    if dt is None:
        now = datetime.datetime.now()
        date_string = now.strftime("%Y%m%d") 
    else:
        date_string = dt
    path = f"data/bikeshare/station_infomation/station_infomation_{date_string}.parquet"
    json_obj = fetch_api(api_url)
    df = flatten_json(json_obj)
    write_gcs(df, path)


if __name__ == '__main__':
    etl_api_to_gcs()
