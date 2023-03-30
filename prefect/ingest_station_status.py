from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import requests
import json
from pathlib import Path
import datetime
from utils import read_local_config, round_to_nearest_10min


@task(log_prints=True, retries=3, retry_delay_seconds=30)
def fetch_api(api_url: str) -> json:
    response = requests.get(api_url)
    return response.json()['data']['stations']


@task(log_prints=True)
def flatten_json(data: json) -> pd.DataFrame:
    now = round_to_nearest_10min(datetime.datetime.now())
    date_string = now.strftime("%Y-%m-%d")
    hour_string = now.strftime("%H")
    minute_string = now.strftime("%M")

    flat_data = []
    for station in data:
        flat_station = {
            'station_id': station['station_id'],
            'num_bikes_available': station['num_bikes_available'],
            'num_docks_available': station['num_docks_available'],
            'num_docks_disabled': station['num_docks_disabled'],
            'last_reported': None,
            'is_charging_station': station['is_charging_station'],
            'status': station['status'],
            'is_installed': station['is_installed'],
            'is_renting': station['is_renting'],
            'is_returning': station['is_returning'],
            'iconic_count': 0,
            'boost_count': 0,
            'efit_count': 0,
            'date': date_string,
            'hour': hour_string,
            'minute': minute_string
        }
        try:
            flat_station['last_reported'] = station['last_reported']
        except KeyError:
            pass  # handle missing last_reported value

        vehicle_types = station['vehicle_types_available']
        for v in vehicle_types:
            if v['vehicle_type_id'] == 'ICONIC':
                flat_station['iconic_count'] = v['count']
            elif v['vehicle_type_id'] == 'BOOST':
                flat_station['boost_count'] = v['count']
            elif v['vehicle_type_id'] == 'EFIT':
                flat_station['efit_count'] = v['count']

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
    api_url = config['station_status_url']
    if dt is None:
        now = round_to_nearest_10min(datetime.datetime.now())
        date_string = now.strftime("%Y%m%d_%H-%M")
    else:
        date_string = dt
    path = f"data/bikeshare/station_status/station_status_{date_string}.parquet"
    json_obj = fetch_api(api_url)
    df = flatten_json(json_obj)
    write_gcs(df, path)


if __name__ == '__main__':
    etl_api_to_gcs()
