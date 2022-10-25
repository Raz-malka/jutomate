from pyspark.sql.functions import explode, arrays_zip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, coalesce, col
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
s3_Bronze_path_sites_metadata = "s3://bse-bronze/sites_metadata/dt={}/".format(date_t)
s3_Silver_path_sites_metadata = "s3://bse-silver/sites_metadata/dt={}/".format(date_t)
df_sites_metadata = spark.read.json(s3_Bronze_path_sites_metadata)
exp_step1_df_sites_metadata = df_sites_metadata.select(explode(arrays_zip("sites.site.accountId",
                                                                          "sites.site.id",
                                                                          "sites.site.installationDate",
                                                                          "sites.site.name",
                                                                          "sites.site.notes",
                                                                          "sites.site.peakPower",
                                                                          "sites.site.status",
                                                                          "sites.site.type",
                                                                          "sites.site.location.address",
                                                                          "sites.site.location.address2",
                                                                          "sites.site.location.city",
                                                                          "sites.site.location.country",
                                                                          "sites.site.location.countryCode",
                                                                          "sites.site.location.timeZone",
                                                                          "sites.site.location.zip",
                                                                        #   "sites.site.alertQuantity",
                                                                        #   "sites.site.highestImpact",
                                                                        #   "sites.site.ptoDate",
                                                                        #   "sites.site.primaryModule.manufacturerName",
                                                                        #   "sites.site.primaryModule.maximumPower",
                                                                        #   "sites.site.primaryModule.modelName",
                                                                        #   "sites.site.primaryModule.temperatureCoef",
                                                                        #   "sites.site.uris.DATA_PERIOD",
                                                                        #   "sites.site.uris.DETAILS",
                                                                        #   "sites.site.uris.OVERVIEW",
                                                                        #   "sites.site.publicSettings.isPublic"
                                                                                           )
                                                                                )
                                                                        )
exp_step2_df_sites_metadata = exp_step1_df_sites_metadata.select(col("col.0").alias("Account_id"),
                                                                 col("col.1").alias("Id"),
                                                                 col("col.2").alias("Installation_Date"),
                                                                 col("col.3").alias("Name"),
                                                                 col("col.4").alias("Comment"),
                                                                 col("col.5").alias("Peak_Power"),
                                                                 col("col.6").alias("Status"),
                                                                 col("col.7").alias("Type"),
                                                                 col("col.8").alias("location_address"),
                                                                 col("col.9").alias("location_address2"),
                                                                 col("col.10").alias("location_city"),
                                                                 col("col.11").alias("location_country"),
                                                                 col("col.12").alias("location_countryCode"),
                                                                 col("col.13").alias("location_timeZone"),
                                                                 col("col.14").alias("location_zip"),
                                                                #  col("col.15").alias("Alert_Quantity"),
                                                                #  col("col.16").alias("Highest_Impact"),
                                                                #  col("col.17").alias("ptoDate"),
                                                                #  col("col.18").alias("primaryModule_manufacturerName"),
                                                                #  col("col.19").alias("primaryModule_maximumPower"),
                                                                #  col("col.20").alias("primaryModule_modelName"),
                                                                #  col("col.21").alias("primaryModule_temperatureCoef"),
                                                                #  col("col.22").alias("uris_DATA_PERIOD"),
                                                                #  col("col.23").alias("uris_DETAILS"),
                                                                #  col("col.24").alias("uris_OVERVIEW"),
                                                                #  col("col.25").alias("publicSettings_isPublic")
                                                                                  )
exp_step2_df_sites_metadata.write.mode("overwrite").parquet(s3_Silver_path_sites_metadata)