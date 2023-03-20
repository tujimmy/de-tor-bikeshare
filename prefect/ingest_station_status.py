from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import requests
import json
from pathlib import Path
import datetime
from io import StringIO, BytesIO
import pyarrow.parquet as pq


@task(log_prints=True)
def fetch_api(api_url: str) -> json:
    response = requests.get(api_url)
    return response.json()['data']['stations']


@task(log_prints=True)
def flatten_json(data: json) -> pd.DataFrame:
    now = datetime.datetime.now()
    date_string = now.strftime("%Y-%m-%d")
    hour_string = now.strftime("%H")

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
            'status': station['status'],
            'is_installed': station['is_installed'],
            'is_renting': station['is_renting'],
            'is_returning': station['is_returning'],
            'iconic_count': 0,
            'boost_count': 0,
            'efit_count': 0,
            'date': date_string,
            'hour': hour_string
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
        destination_table="trips_data_all.test_station_info",
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

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # df['last_reported_datetime'] = datetime.datetime.fromtimestamp(df['last_reported']).strftime("%Y-%m-%d %H:%M:%S")
    df = df[df['last_reported'].notna()]
    df['last_reported_datetime'] = df['last_reported'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
    # print(df)
    return df 

@flow()
def etl_api_to_gcs(dt: str = None) -> None:
    """The main ETL function"""
    station_api_url = "https://toronto-us.publicbikesystem.net/customer/gbfs/v2/en/station_status"
    if dt is None:
        now = datetime.datetime.now()
        # date_string = now.strftime("%Y%m%d_%H")
        date_string = now.strftime("%Y%m%d_%H")
    else:
        date_string = dt
    path = f"data/bikeshare/station_status/station_status_{date_string}.parquet"
    json_obj = fetch_api(station_api_url)
    df = flatten_json(json_obj)
    write_gcs(df, path)
    df = read_gcs(path)
    df = transform(df)
    write_bq(df)


if __name__ == '__main__':
    # year = 2023
    # month = 3
    # day = 1
    # hour = 0
    # date_string = f"{year}{month:02}{day:02}_{hour:02}"
    # date_string = None
    # etl_api_to_gcs(date_string)
    etl_api_to_gcs()