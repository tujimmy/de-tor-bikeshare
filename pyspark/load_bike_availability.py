import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, hour, avg, lit, to_timestamp, concat
)
from spark_utils import (
    parse_arguments, create_spark_session
)

from google.cloud import bigquery



parsed_args = parse_arguments(sys.argv[1:])
target_date = parsed_args['target_date']
bucket = parsed_args['bucket']
project_id = parsed_args['project_id']
dataset_id = parsed_args['dataset_id']

table_id = 'bike_availability'
partition_id = f"{table_id}${target_date.replace('-', '')}"

# Create a BigQuery client
client = bigquery.Client()
# Delete the partition if it exists
client.delete_table(f"{dataset_id}.{partition_id}", not_found_ok=True)

# Print the value of target_date
print(f"Loading stg_station_status for date: {target_date}")
spark = create_spark_session("spark-load-bike-availability", bucket)


# target_date='2023-03-23'
station_status_df = spark.read.format('bigquery') \
    .option('table', f'{project_id}.{dataset_id}.stg_station_status') \
    .option('filter', f"date  = '{target_date}'") \
    .load()

station_info_df = spark.read.format('bigquery') \
    .option('table', f'{project_id}.{dataset_id}.stg_station_infomation') \
    .load()

hourly_avg_df = station_status_df.groupBy(
    col('station_id'),
    col('week_day'),
    col('date'),
    col('hour')
).agg(
    avg('num_bikes_available').alias('avg_bikes_available'),
    avg('electric_count').alias('avg_ebikes_available'),
    avg('num_docks_available').alias('avg_docks_available'),
    avg('num_docks_disabled').alias('avg_docks_disabled')
).withColumn(
    'avg_availability_ratio',
    col('avg_bikes_available') / (col('avg_bikes_available') + col('avg_docks_available'))
).withColumn(
    'time_period', lit('hourly')
).withColumn(
    'period_start',
    to_timestamp(concat(col('date'), col('hour')), 'yyyy-MM-ddHH')
)

df = hourly_avg_df.join(
    station_info_df.drop(
    col("date")
    ),
    on='station_id',
    how='left'
).select(
    'station_id',
    'name',
    'date',
    'time_period',
    'period_start',
    'week_day',
    'capacity',
    'is_charging_station',
    'lat',
    'lon',
    'avg_bikes_available',
    'avg_ebikes_available',
    'avg_docks_available',
    'avg_docks_disabled',
    'avg_availability_ratio'
)


df.write.format('bigquery') \
    .option('table', f'{dataset_id}.bike_availability') \
    .mode('append') \
    .option("partitionField", "date") \
    .option("clusteredFields", "station_id") \
    .save()
