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
                                                         .withColumn("AC_Power", lit(''))\
                                                         .withColumn("DC_Current", lit(''))\
                                                         .withColumn("DC_Power", lit(''))\
                                                         .withColumn("Account_id", lit(''))
exp_step2_df_inverters_data.write.mode("overwrite").parquet(s3_Silver_path_inverters_data)

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