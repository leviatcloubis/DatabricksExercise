# Databricks notebook source
# MAGIC %md
# MAGIC ## Call get stations API in order to retrieve a list of all stations

# COMMAND ----------

# Import packages
import http
import json
import pandas as pd
import requests
from pyspark.sql.types import *

# Save json response as variable

response = requests.post("https://api.irail.be/stations/?format=json&lang=en").json()

# COMMAND ----------

# Create pandas dataframe from JSON response with collection reference .station
pandas_df = pd.DataFrame.from_dict(response['station'])

# COMMAND ----------

# Create Pyspark dataframe from JSON response with collection reference .station

spark_df = spark.createDataFrame(response['station'], 
                      schema=StructType(fields=[
                          StructField("locationX", StringType()), 
                          StructField("locationY", StringType()),
                          StructField("id", StringType()), 
                          StructField("name", StringType()), 
                          StructField("@id", StringType()), 
                          StructField("standardname", StringType()) 
                      ]))
