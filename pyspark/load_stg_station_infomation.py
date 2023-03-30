import sys
from pyspark.sql.functions import col, to_date, split

from spark_utils import create_spark_session, read_parquet_data, parse_arguments


parsed_args = parse_arguments(sys.argv[1:])

target_date = parsed_args['target_date']
bucket = parsed_args['bucket']
table_id = 'stg_station_infomation'

print(f"Loading stg_station_infomation for date: {target_date}")
spark = create_spark_session('spark-load-stg-station-infomation', bucket)

df = read_parquet_data(
    spark, 
    f"gs://{bucket}/data/bikeshare/station_infomation/station_infomation_{target_date.replace('-', '')}*.parquet"
    )

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
