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
s3_Gold_path_inverters_data = "s3://bse-gold/inverters_data/dt={}/".format(date_t)
s3_Gold_path_inverters_data_agg = "s3://bse-gold/inverters_data_agg/dt={}/".format(date_t)
df_inverters_data_agg = spark.read.parquet(s3_Gold_path_inverters_data)
exp_step_df_inverters_data_agg = df_inverters_data_agg.groupBy("Account_id",
                                                               "Data_Source",
                                                               "DC_Current"
                                                              )\
                                                       .agg(avg("AC_Voltage").alias("AC_Voltage"),
                                                            avg("AC_Power").alias("AC_Power"),
                                                            avg("AC_Energy").alias("AC_Energy"),
                                                            avg("DC_Voltage").alias("DC_Voltage"),
                                                            avg("Power_Factor").alias("Power_Factor"),
                                                            avg("Active_Power").alias("Active_Power"),
                                                            avg("Temperature").alias("Temperature"),
                                                            # avg("DC_Current").alias("DC_Current"),
                                                            avg("DC_Power").alias("DC_Power"),
                                                            avg("AC_Current_L1").alias("AC_Current_L1"),
                                                            avg("AC_Current_L2").alias("AC_Current_L2"),
                                                            avg("AC_Current_L3").alias("AC_Current_L3"),
                                                            avg("AC_Voltage_L1").alias("AC_Voltage_L1"),
                                                            avg("AC_Voltage_L2").alias("AC_Voltage_L2"),
                                                            avg("AC_Voltage_L3").alias("AC_Voltage_L3"),
                                                            avg("AC_Current").alias("AC_Current"),
                                                            avg("Reactive_Power").alias("Reactive_Power")
                                                           )
exp_step_df_inverters_data_agg.write.mode("overwrite").parquet(s3_Gold_path_inverters_data_agg)