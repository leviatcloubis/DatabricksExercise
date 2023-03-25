# Databricks notebook source
# MAGIC %md
# MAGIC ## Call get disturbances API in order to retrieve disturbances

# COMMAND ----------

# Import packages
import http
import json
import pandas as pd
import requests
from pyspark.sql.types import *
import pytz
import datetime
from pyspark.sql.functions import *


# Save json response as variable

response = requests.post("https://api.irail.be/disturbances/?format=json&lineBreakCharacter=''&lang=nl").json()

# set mount location variable

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'

mount_location = "/mnt/"+storage_account+"/"+storage_container

# COMMAND ----------

# Create pandas dataframe from JSON response with collection reference .disturbance
pandas_df = pd.DataFrame.from_dict(response['disturbance'])

# COMMAND ----------

# Create Pyspark dataframe from JSON response with collection reference .station

spark_df = spark.createDataFrame(response['disturbance'], 
                      schema=StructType(fields=[
                          StructField("id", StringType()), 
                          StructField("title", StringType()),
                          StructField("description", StringType()), 
                          StructField("link", StringType()), 
                          StructField("type", StringType()), 
                          StructField("timestamp", StringType()),
                          StructField("richtext", StringType()), 
                          StructField("descriptionLinks", StringType())   
                      ]))

spark_df.show()

# COMMAND ----------

# Write pyspark df to Parquet file on bronze container with /disturbances/yyyymmddhhmm

current_timestamp = datetime.datetime.now(pytz.timezone('Europe/Brussels')).strftime("%Y%m%d%H%M")
spark_df.write.parquet(f"{mount_location}/disturbances/{current_timestamp}")

# COMMAND ----------

# DBTITLE 1,Convert string timestamp to timestamp type
#spark_df.withColumn("timestamped",from_unixtime(col('timestamp'))).show()


# COMMAND ----------


