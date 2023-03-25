from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, date_format, to_date, split
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--target_date', type=str, required=True)
args, unknown = parser.parse_known_args()
target_date = args.target_date

print(f"Loading stg_station_infomation for date: {target_date}")
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-load-stg-station-infomation') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dtc_data_lake_root-welder-375217"
spark.conf.set('temporaryGcsBucket', bucket)

df = spark.read.format("parquet") \
    .option("mergeSchema", "true") \
    .load(f"gs://{bucket}/data/bikeshare/station_infomation/station_infomation_{target_date.replace('-', '')}*.parquet")

df = df.withColumn(
    "station_id",
    col("station_id").cast("int")
).withColumn(
    "date",
    to_date("date")
).withColumn(
    "street_1",
    split("name", " / ").getItem(0)
).withColumn(
    "street_2",
    split("name", " / ").getItem(1)
)

df.write.format('bigquery') \
    .option('table', 'bikeshare.stg_station_infomation') \
    .mode('overwrite') \
    .save()
