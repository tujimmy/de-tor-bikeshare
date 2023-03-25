from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, date_format, to_date
from google.cloud import bigquery
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--target_date', type=str, required=True)
args, unknown = parser.parse_known_args()
target_date = args.target_date

# Create a BigQuery client
client = bigquery.Client()

# Set the table and partition identifiers
dataset_id = 'bikeshare'
table_id = 'stg_station_status'
partition_id = f"{table_id}${target_date.replace('-', '')}"

# Delete the partition if it exists
client.delete_table(f"{dataset_id}.{partition_id}", not_found_ok=True)

# Print the value of target_date
print(f"Loading stg_station_status for date: {target_date}")
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-load-stg-station-status') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dtc_data_lake_root-welder-375217"
spark.conf.set('temporaryGcsBucket', bucket)

df = spark.read.format("parquet") \
    .option("mergeSchema", "true") \
    .load(f"gs://{bucket}/data/bikeshare/station_status/station_status_{target_date.replace('-', '')}*.parquet")

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
    .mode('overwrite') \
    .option("partitionField", "date") \
    .option("clusteredFields", "station_id,hour") \
    .save()

