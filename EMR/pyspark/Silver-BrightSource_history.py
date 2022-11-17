from pyspark.sql.functions import explode, arrays_zip
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import lit, coalesce, col
from pyspark.sql import SparkSession
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
s3_Bronze_path_inverters_data = "s3://bse-bronze/inverters_data/dt={}/".format(date_t)
s3_Silver_path_inverters_data = "s3://bse-silver/inverters_data/dt={}/".format(date_t)
df_inverters_data = spark.read.json(s3_Bronze_path_inverters_data)
exp_step1_df_inverters_data = df_inverters_data.select(col("site_id").alias("Local_site_id"),
                                                       col("inv_id").alias("Local_inverter_id"),
                                                       explode(arrays_zip("data.telemetries.date",
                                                                          "data.telemetries.dcVoltage",
                                                                          # "data.telemetries.inverterMode",
                                                                          # "data.telemetries.operationMode",
                                                                          # "data.telemetries.powerLimit",
                                                                          "data.telemetries.temperature",
                                                                          "data.telemetries.totalActivePower",
                                                                          "data.telemetries.totalEnergy",
                                                                          # "data.telemetries.vL1To2",
                                                                          # "data.telemetries.vL2To3",
                                                                          # "data.telemetries.vL3To1",
                                                                          "data.telemetries.L1Data.acCurrent",
                                                                          # "data.telemetries.L1Data.acFrequency",
                                                                          # "data.telemetries.L1Data.activePower",
                                                                          "data.telemetries.L1Data.acVoltage",
                                                                          # "data.telemetries.L1Data.apparentPower",
                                                                          "data.telemetries.L1Data.cosPhi",
                                                                          "data.telemetries.L1Data.reactivePower",
                                                                          "data.telemetries.L2Data.acCurrent",
                                                                          # "data.telemetries.L2Data.acFrequency",
                                                                          # "data.telemetries.L2Data.activePower",
                                                                          "data.telemetries.L2Data.acVoltage",
                                                                          # "data.telemetries.L2Data.apparentPower",
                                                                          "data.telemetries.L2Data.cosPhi",
                                                                          "data.telemetries.L2Data.reactivePower",
                                                                          "data.telemetries.L3Data.acCurrent",
                                                                          # "data.telemetries.L3Data.acFrequency",
                                                                          # "data.telemetries.L3Data.activePower",
                                                                          "data.telemetries.L3Data.acVoltage",
                                                                          # "data.telemetries.L3Data.apparentPower",
                                                                          "data.telemetries.L3Data.cosPhi",
                                                                          "data.telemetries.L3Data.reactivePower"
                                                                         )
                                                              )
                                                      )
exp_step2_df_inverters_data = exp_step1_df_inverters_data.select("Local_site_id",
                                                                 "Local_inverter_id",
                                                                 col("col.0").alias("Datetime"),
                                                                 col("col.1").alias("DC_Voltage"),
                                                                 col("col.2").alias("Temperature"),
                                                                 col("col.3").alias("Active_Power"),
                                                                 col("col.4").alias("AC_Energy"),
                                                                 col("col.5").alias("AC_Current_L1"),
                                                                 col("col.6").alias("AC_Voltage_L1"),
                                                                 col("col.7").alias("L1Data_cosPhi"),
                                                                 col("col.8").alias("L1Data_reactivePower"),
                                                                 col("col.9").alias("AC_Current_L2"),
                                                                 col("col.10").alias("AC_Voltage_L2"),
                                                                 col("col.11").alias("L2Data_cosPhi"),
                                                                 col("col.12").alias("L2Data_reactivePower"),
                                                                 col("col.13").alias("AC_Current_L3"),
                                                                 col("col.14").alias("AC_Voltage_L3"),
                                                                 col("col.15").alias("L3Data_cosPhi"),
                                                                 col("col.16").alias("L3Data_reactivePower"))
                                                                 #  "col.17",
                                                                 #  "col.18",
                                                                 #  "col.19",
                                                                 #  "col.20",
                                                                 #  "col.21",
                                                                 #  "col.22",
                                                                 #  "col.23",
                                                                 #  "col.24",
                                                                 #  "col.25",
                                                                 #  "col.26",
                                                                 #  "col.27",
                                                                 #  "col.28",
                                                                 #  "col.29",
                                                                 #  "col.30",
                                                                 #  "col.31")
exp_step2_df_inverters_data = exp_step2_df_inverters_data.withColumn("AC_Current", (exp_step2_df_inverters_data.AC_Current_L1 + exp_step2_df_inverters_data.AC_Current_L2 + exp_step2_df_inverters_data.AC_Current_L3)/3)\
                                                         .withColumn("AC_Voltage", (exp_step2_df_inverters_data.AC_Voltage_L1 + exp_step2_df_inverters_data.AC_Voltage_L2 + exp_step2_df_inverters_data.AC_Voltage_L3)/3)\
                                                         .withColumn("Reactive_Power", (exp_step2_df_inverters_data.L1Data_reactivePower + exp_step2_df_inverters_data.L2Data_reactivePower + exp_step2_df_inverters_data.L3Data_reactivePower))\
                                                         .withColumn("Power_Factor", coalesce(exp_step2_df_inverters_data.L1Data_cosPhi, exp_step2_df_inverters_data.L2Data_cosPhi, exp_step2_df_inverters_data.L3Data_cosPhi))\
                                                         .withColumn("AC_Power", lit(None).cast(DoubleType()))\
                                                         .withColumn("DC_Current", lit(None).cast(DoubleType()))\
                                                         .withColumn("DC_Power", lit(None).cast(DoubleType()))\
                                                         .withColumn("Account_id", lit(None).cast(StringType()))
exp_step2_df_inverters_data.write.mode("overwrite").parquet(s3_Silver_path_inverters_data)