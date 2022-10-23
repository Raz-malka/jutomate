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
s3_Silver_path_inverters_data = "s3://bse-silver/inverters_data/dt={}/".format(date_t)
s3_Gold_path_inverters_data = "s3://bse-gold/inverters_data/dt={}/".format(date_t)
df_inverters_data = spark.read.parquet(s3_Silver_path_inverters_data)
exp_step1_df_inverters_data = df_inverters_data.select("Datetime",
                                                       "AC_Voltage",
                                                       "AC_Power",
                                                       "AC_Energy",
                                                       "DC_Voltage",
                                                       "Power_Factor",
                                                       "Active_Power",
                                                       "Temperature",
                                                       "DC_Current",
                                                       "DC_Power",
                                                       "Account_id",
                                                       when(col("AC_Current_L1") < -1, lit(''))
                                                       .when(col("AC_Current_L1").between(-1 ,0), 0)
                                                       .when(col("AC_Current_L1") > 210, lit(''))
                                                       .when(col("AC_Current_L1").between(200 ,210), 200)
                                                       .otherwise(col("AC_Current_L1")).alias("AC_Current_L1"),
                                                       when(col("AC_Current_L2") < -1, lit(''))
                                                       .when(col("AC_Current_L2").between(-1 ,0), 0)
                                                       .when(col("AC_Current_L2") > 210, lit(''))
                                                       .when(col("AC_Current_L2").between(200 ,210), 200)
                                                       .otherwise(col("AC_Current_L2")).alias("AC_Current_L2"),
                                                       when(col("AC_Current_L3") < -1, lit(''))
                                                       .when(col("AC_Current_L3").between(-1 ,0), 0)
                                                       .when(col("AC_Current_L3") > 210, lit(''))
                                                       .when(col("AC_Current_L3").between(200 ,210), 200)
                                                       .otherwise(col("AC_Current_L3")).alias("AC_Current_L3"),
                                                       when(col("AC_Voltage_L1") < -1, lit(''))
                                                       .when(col("AC_Voltage_L1").between(-1 ,0), 0)
                                                       .when(col("AC_Voltage_L1") > 210, lit(''))
                                                       .when(col("AC_Voltage_L1").between(200 ,210), 200)
                                                       .otherwise(col("AC_Voltage_L1")).alias("AC_Voltage_L1"),
                                                       when(col("AC_Voltage_L2") < -1, lit(''))
                                                       .when(col("AC_Voltage_L2").between(-1 ,0), 0)
                                                       .when(col("AC_Voltage_L2") > 250, lit(''))
                                                       .when(col("AC_Voltage_L2").between(240 ,250), 240)
                                                       .otherwise(col("AC_Voltage_L2")).alias("AC_Voltage_L2"),
                                                       when(col("AC_Voltage_L3") < -1, lit(''))
                                                       .when(col("AC_Voltage_L3").between(-1 ,0), 0)
                                                       .when(col("AC_Voltage_L3") > 250, lit(''))
                                                       .when(col("AC_Voltage_L3").between(240 ,250), 240)
                                                       .otherwise(col("AC_Voltage_L3")).alias("AC_Voltage_L3"),
                                                       when(col("AC_Current") < -1, lit(''))
                                                       .when(col("AC_Current").between(-1 ,0), 0)
                                                       .when(col("AC_Current") > 260, lit(''))
                                                       .when(col("AC_Current").between(240 ,260), 240)
                                                       .otherwise(col("AC_Current")).alias("AC_Current"),
                                                       # when(col("DC_Current") < -1, lit(''))
                                                       # .when(col("DC_Current").between(-1 ,0), 0)
                                                       # .otherwise(col("DC_Current")).alias("DC_Current"),
                                                       # when(col("DC_Power") < -20, lit(''))
                                                       # .when(col("DC_Power").between(-20 ,0), 0)
                                                       # .when(col("DC_Power") > 500, lit(''))
                                                       # .when(col("DC_Power").between(490 ,500), 490)
                                                       # .otherwise(col("DC_Power")).alias("DC_Power"),
                                                       when(col("Reactive_Power") < -1, lit(''))
                                                       .when(col("Reactive_Power").between(-1 ,0), 0)
                                                       .otherwise(col("Reactive_Power")).alias("Reactive_Power")
                                                       # when(col("Account_id") < -90, lit(''))
                                                       # .when(col("Account_id").between(-90 ,-40), -40)
                                                       # .when(col("Account_id") > 175, lit(''))
                                                       # .when(col("Account_id").between(85 ,175), 85)
                                                       # .otherwise(col("Account_id")).alias("Account_id")
                                                      )\
                                               .withColumn("Data_Source", lit('Solar Edge'))
exp_step1_df_inverters_data.write.mode("overwrite").parquet(s3_Gold_path_inverters_data)

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