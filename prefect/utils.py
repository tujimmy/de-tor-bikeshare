import datetime
from google.cloud.bigquery import SchemaField
import json

def round_to_nearest_10min(dt):
    """
    Rounds a given datetime object to the nearest 10 minutes.
    """
    rounded_minute = (dt.minute // 10) * 10
    dt_rounded = dt.replace(minute=rounded_minute, second=0, microsecond=0)
    if dt.minute % 10 >= 5:
        dt_rounded += datetime.timedelta(minutes=10)
    return dt_rounded

def create_station_status_schema():
    schema = [
        SchemaField("station_id", "INTEGER"),
        SchemaField("num_bikes_available", "INTEGER"),
        SchemaField("num_docks_available", "INTEGER"),
        SchemaField("num_docks_disabled", "INTEGER"),
        SchemaField("last_reported", "TIMESTAMP"),
        SchemaField("is_charging_station", "BOOLEAN"),
        SchemaField("status", "STRING"),
        SchemaField("is_installed", "BOOLEAN"),
        SchemaField("is_renting", "BOOLEAN"),
        SchemaField("is_returning", "BOOLEAN"),
        SchemaField("iconic_count", "INTEGER"),
        SchemaField("boost_count", "INTEGER"),
        SchemaField("efit_count", "INTEGER"),
        SchemaField("date", "DATE"),
        SchemaField("hour", "INTEGER"),
        SchemaField("minute", "INTEGER"),
        SchemaField("last_reported_datetime", "DATETIME"),
        SchemaField("electric_count", "INTEGER"),
        SchemaField("week_day", "STRING")
    ]
    return schema

def create_station_infomation_schema():
    schema = [
        SchemaField("station_id", "INTEGER"),
        SchemaField("name", "STRING"),
        SchemaField("physical_configuration", "STRING"),
        SchemaField("lat", "FLOAT"),
        SchemaField("lon", "FLOAT"),
        SchemaField("address", "STRING"),
        SchemaField("capacity", "INTEGER"),
        SchemaField("is_charging_station", "BOOLEAN"),
        SchemaField("is_virtual_station", "BOOLEAN"),
        SchemaField("date", "DATE"),
        SchemaField("street_1", "STRING"),
        SchemaField("street_2", "STRING")
    ]
    return schema

def read_local_config(config_file_path: str = "prefect/config/config.json"):
    with open(config_file_path, "r") as file:
        config = json.load(file)
    return config