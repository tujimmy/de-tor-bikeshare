from google.cloud.bigquery import SchemaField


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

def create_bike_availability_schema():
    schema = [
        SchemaField("station_id", "INTEGER", mode="NULLABLE"),
        SchemaField("name", "STRING", mode="NULLABLE"),
        SchemaField("date", "DATE", mode="NULLABLE"),
        SchemaField("time_period", "STRING", mode="REQUIRED"),
        SchemaField("period_start", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("week_day", "STRING", mode="NULLABLE"),
        SchemaField("capacity", "INTEGER", mode="NULLABLE"),
        SchemaField("is_charging_station", "BOOLEAN", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="NULLABLE"),
        SchemaField("lon", "FLOAT", mode="NULLABLE"),
        SchemaField("avg_bikes_available", "FLOAT", mode="NULLABLE"),
        SchemaField("avg_ebikes_available", "FLOAT", mode="NULLABLE"),
        SchemaField("avg_docks_available", "FLOAT", mode="NULLABLE"),
        SchemaField("avg_docks_disabled", "FLOAT", mode="NULLABLE"),
        SchemaField("avg_availability_ratio", "FLOAT", mode="NULLABLE")
    ]
    return schema
