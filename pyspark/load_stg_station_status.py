import sys
from pyspark.sql.functions import col, to_date, date_format
from google.cloud import bigquery

from spark_utils import create_spark_session, read_parquet_data, parse_arguments


parsed_args = parse_arguments(sys.argv[1:])

target_date = parsed_args['target_date']
bucket = parsed_args['bucket']
dataset_id = parsed_args['dataset_id']
table_id = 'stg_station_status'
# Set the table and partition identifiers
partition_id = f"{table_id}${target_date.replace('-', '')}"

# Create a BigQuery client
client = bigquery.Client()
# Delete the partition if it exists
client.delete_table(f"{dataset_id}.{partition_id}", not_found_ok=True)

# Print the value of target_date
print(f"Loading stg_station_status for date: {target_date}")
spark = create_spark_session('spark-load-stg-station-status', bucket)

df = read_parquet_data(
    spark, 
    f"gs://{bucket}/data/bikeshare/station_status/station_status_{target_date.replace('-', '')}*.parquet"
    )

df = df.withColumn(
    "station_id",
    col("station_id").cast("int")
).withColumn(
    "date",
    to_date("date")
).withColumn(
    "hour",
    col("hour").cast("int")
).withColumn(
    "minute",
    col("minute").cast("int")
).withColumn(
    "electric_count",
    col("boost_count") + col("efit_count")
).withColumn(
    "week_day",
    date_format(to_date(col("date"), "yyyy-MM-dd"), "E")
)

print("Saving to table")
df.write.format('bigquery') \
    .option('table', 'bikeshare.stg_station_status') \
    .mode('append') \
    .option("partitionField", "date") \
    .option("clusteredFields", "station_id,hour") \
    .save()

