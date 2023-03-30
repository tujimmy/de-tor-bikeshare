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


def read_local_config(config_file_path: str = "prefect/config/config.json"):
    with open(config_file_path, "r") as file:
        config = json.load(file)
    return config