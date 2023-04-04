# Databricks notebook source
# MAGIC %md
# MAGIC ## PART 1: Call get disturbances API in order to retrieve disturbances

# COMMAND ----------

# DBTITLE 1,Import packages & dependencies
# MAGIC %pip install xlrd==2.0.1

# COMMAND ----------

import http
import json
import pandas as pd
import json, requests, re
from pyspark.sql.types import *
import pytz
import datetime
from pyspark.sql.functions import *
import re
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

# DBTITLE 1,Retain disturbances API response and prepare mount location variable
# Save json response as variable

response = requests.post("https://api.irail.be/disturbances/?format=json&lineBreakCharacter=''&lang=nl").json()

# set mount location variable

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'

mount_location = "/mnt/"+storage_account+"/"+storage_container

# COMMAND ----------

# DBTITLE 1,Create pandas dataframe from JSON response with collection reference .disturbance
pandas_df = pd.DataFrame.from_dict(response['disturbance'])

# COMMAND ----------

# DBTITLE 1,# Create Pyspark dataframe from JSON response with collection reference .station
# Create Pyspark dataframe from JSON response with collection reference .station

disturbances_df = spark.createDataFrame(response['disturbance'], 
                      schema=StructType(fields=[
                          StructField("id", StringType()), 
                          StructField("title", StringType()),
                          StructField("description", StringType()), 
                          StructField("link", StringType()), 
                          StructField("type", StringType()), 
                          StructField("timestamp", StringType()),
                          StructField("richtext", StringType()), 
                          StructField("descriptionLinks", StringType())   
                      ])).withColumn('timestamp_from_unix', from_unixtime('timestamp'))\
                         .withColumn('import_timestamp', current_timestamp())\
                         .withColumn('created_by', lit('Levi Devos'))      

disturbances_df.show()

# COMMAND ----------

# DBTITLE 1,Write pyspark df to Parquet file on bronze container with /disturbances/yyyymmddhhmm
current_timestamp = datetime.datetime.now(pytz.timezone('Europe/Brussels')).strftime("%Y%m%d%H%M")
disturbances_df.write.parquet(f"{mount_location}/disturbances/{current_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 2: Preparing station locations for using in PowerBI
# MAGIC 
# MAGIC The goal is to create a heatmap of disturbances.
# MAGIC The railAPI provides us with disturbances which are being stored in the `abfss://bronze@[REDACTED].dfs.core.windows.net/disturbances/timestamp` location. Similarly, we can retrieve each station's details from the API. This notebook combines the two in an attempt to enricht the disturbances with location data.

# COMMAND ----------

# DBTITLE 1,Retrieve the list of stations from the API in four possible languages
# Getting the list of stations from the API
responses = list()
langs = ['en', 'nl', 'fr', 'de']
delay = 0.9 # in seconds
for lang in langs:
    stations_response = requests.post(f"https://api.irail.be/stations/?format=json&lang={lang}").json()
    responses.extend(stations_response['station'])

# COMMAND ----------

# DBTITLE 1,Read into Pandas df to flatten, then parallelize as Spark DF
stations_df = spark.createDataFrame(pd.json_normalize(responses))
stations_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Station name parsing
# MAGIC Stations have a 'standard name'. This is typically just the name of the stations in the relevant languages, e.g. "Mouterij/Germoir" or "Brussel-Kapellekerk/Bruxelles-Chapelle". Sometimes it's also a fixed choice of language, that may or may not correspond to the 'regular' (display?) name. 
# MAGIC 
# MAGIC We will use these names to match stations to disturbances. Both names will be extracted into columns.

# COMMAND ----------

def extract_names(title):
    """
        Returns array of names contained in the title.
        Titles are assumed to be either a single name, or of the format "<station> [ - <other-station>][: <description>]"
    """
    # Check if the title has a descriptor
    if ':' in title:
        title = title[:title.index(':')]
    parts = title.split(" - ") # Extract 
    return [p.strip() for p in parts]
extract_title_udf = udf(extract_names, ArrayType(StringType()))

# COMMAND ----------

# DBTITLE 1,Run above function on disturbances_df's title column
#display(disturbances_df.withColumn("stations", extract_title_udf(col('title')))
#                       .withColumn('station', explode(col("stations")))
#                       .select("id", "title", "station")
#                       .limit(50))

# COMMAND ----------

# DBTITLE 1,Construct a table of all names found in the station data
def collect_names(names):
    namelist = re.sub(" / ", "/", names).split('/')
    return [re.sub(" - ", '-', name) for name in namelist]
collect_names_udf = udf(collect_names, ArrayType(StringType()))

def dedup_names(names):
    return list(set(names))
dedup_names_udf = udf(dedup_names, ArrayType(StringType()))

all_names_df = stations_df.distinct()\
                           .withColumn('names', collect_names_udf(col('name')))\
                           .withColumn('standardnames', collect_names_udf(col('standardname')))\
                           .withColumn('allNames', dedup_names_udf(concat(col('names'), col('standardnames'))))
                           # \
                           #.withColumn('station', explode(col('allNames')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 3: Join station locations with disturbances data

# COMMAND ----------

# DBTITLE 1,Join disturbances pyspark dataframe with the list of stations from stations data API
disturbance_locations_df = disturbances_df\
                            .withColumn("stations", extract_title_udf(col('title')))\
                            .withColumn('name', explode(col("stations")))\
                            .withColumnRenamed("id", "disturbance_id")\
                            .join(all_names_df, on="name", how="left")\
                            .drop("@id","standardname","names","standardnames","allNames","descriptionLinks") 

# COMMAND ----------

# DBTITLE 1,Write pyspark df to Parquet file on bronze container with /disturbances_enriched/yyyymmddhhmm
current_timestamp = datetime.datetime.now(pytz.timezone('Europe/Brussels')).strftime("%Y%m%d%H%M")
disturbance_locations_df.write.parquet(f"{mount_location}/disturbances_enriched/{current_timestamp}")
