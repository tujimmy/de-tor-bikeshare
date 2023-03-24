from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import requests
import json
from pathlib import Path
import datetime
from io import BytesIO
import pyarrow.parquet as pq


@task(log_prints=True)
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


@task(log_prints=True)
def write_gcs(df: pd.DataFrame, path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df=df,
        to_path=path,
        serialization_format='parquet'
    )


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write to BQ"""
    print(f"writing rows: {len(df)}")
    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")
    df.to_gbq(
        destination_table="trips_data_all.test_station_info_1",
        project_id="root-welder-375217",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
    )

@task(log_prints=True)
def read_gcs(path: Path) -> pd.DataFrame:
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    data = gcp_cloud_storage_bucket_block.read_path(path)
    blob = BytesIO(data)
    table = pq.read_table(blob)
    df = table.to_pandas()
    return df


@flow()
def etl_api_to_gcs(dt: str = None) -> None:
    """The main ETL function"""
    station_api_url = "https://toronto-us.publicbikesystem.net/customer/gbfs/v2/en/station_information"
    if dt is None:
        now = datetime.datetime.now()
        date_string = now.strftime("%Y%m%d") 
    else:
        date_string = dt
    path = f"data/bikeshare/station_infomation/station_infomation_{date_string}.parquet"
    json_obj = fetch_api(station_api_url)
    df = flatten_json(json_obj)
    write_gcs(df, path)
    df = read_gcs(path)



if __name__ == '__main__':
    etl_api_to_gcs()
