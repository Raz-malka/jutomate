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
s3_Bronze_path_sites_invertory_details = "s3://bse-bronze/sites_invertory_details/dt={}/".format(date_t)
s3_Silver_path_sites_invertory_details = "s3://bse-silver/sites_invertory_details/dt={}/".format(date_t)
df_sites_invertory_details = spark.read.json(s3_Bronze_path_sites_invertory_details)
exp_step1_df_sites_invertory_details = df_sites_invertory_details.select(col("site_id").alias("Local_Parent_Id"),
                                                                         explode(arrays_zip("Inventory.inverters.manufacturer",
                                                                                            "Inventory.inverters.model",
                                                                                            "Inventory.inverters.name",
                                                                                            "Inventory.inverters.SN"
                                                                                            # "Inventory.inverters.communicationMethod",
                                                                                            # "Inventory.inverters.connectedOptimizers",
                                                                                            # "Inventory.inverters.cpuVersion",
                                                                                            # "Inventory.inverters.dsp1Version",
                                                                                            # "Inventory.inverters.dsp2Version"
                                                                                           )
                                                                                )
                                                                        )
exp_step2_df_sites_invertory_details = exp_step1_df_sites_invertory_details.select("Local_Parent_Id",
                                                                                   col("col.0").alias("Manufacturer"),
                                                                                   col("col.1").alias("Model"),
                                                                                   col("col.2").alias("Name"),
                                                                                   col("col.3").alias("Id")
                                                                                   # col("col.4").alias("communicationMethod"),
                                                                                   # col("col.5").alias("connectedOptimizers"),
                                                                                   # col("col.6").alias("cpuVersion"),
                                                                                   # col("col.7").alias("dsp1Version"),
                                                                                   # col("col.8").alias("dsp2Version")
                                                                                  )
exp_step2_df_sites_invertory_details.write.mode("overwrite").parquet(s3_Silver_path_sites_invertory_details)