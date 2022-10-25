from pyspark.sql.functions import explode, arrays_zip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, coalesce, when, col, avg
import argparse
import datetime
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
parser = argparse.ArgumentParser(description="insert the folder where are file are located")
parser.add_argument('-date_t','--date_t', metavar='', required=True,  type=str, help='the folder where the file are located')
args = parser.parse_args()

date_t = args.date_t
s3_Silver_path_sites_metadata = "s3://bse-silver/sites_metadata/dt={}/".format(date_t)
s3_Gold_path_sites_metadata = "s3://bse-gold/sites_metadata/dt={}/".format(date_t)
df_sites_metadata = spark.read.parquet(s3_Silver_path_sites_metadata)
exp_step1_df_sites_metadata = df_sites_metadata.select("Id",
                                                       "Name",
                                                       "Status",
                                                       "Peak_Power",
                                                       "Installation_Date",
                                                       "Comment",
                                                       "Type",
                                                       "Account_id"
                                                      )\
                                               .withColumn("Latitude", lit(0))\
                                               .withColumn("Longitude", lit(0))
exp_step1_df_sites_metadata.write.mode("overwrite").parquet(s3_Gold_path_sites_metadata)