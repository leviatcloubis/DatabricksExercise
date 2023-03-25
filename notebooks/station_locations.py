# Databricks notebook source
# MAGIC %md
# MAGIC ## Preparing station locations for using in PowerBI
# MAGIC The goal is to create a heatmap of disturbances.
# MAGIC The railAPI provides us with a stream of disturbances which is being stored in the `stream_disturbances_bronze` table. Similarly, we can retrieve each station's details from the API. This notebook combines the two in an attempt to enricht the disturbances with location data.

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

import json, requests, re
import pandas as pd
from pyspark.sql.functions import from_unixtime, desc, col, udf, explode, isnan, isnull, count, concat
from pyspark.sql.types import StringType, ArrayType

# COMMAND ----------

# Getting the list of stations from the API
responses = list()
langs = ['en', 'nl', 'fr', 'de']
delay = 0.9 # in seconds
for lang in langs:
    stations_response = requests.post(f"https://api.irail.be/stations/?format=json&lang={lang}").json()
    responses.extend(stations_response['station'])

# COMMAND ----------

# Read into Pandas df to flatten, then parallelize as Spark DF
stations_df = spark.createDataFrame(pd.json_normalize(responses))

# COMMAND ----------

# Loading the raw disturbances data (from the streamed table)
disturbances_df = spark.read.table('stream_disturbances_bronze').withColumn('readable_timestamp', from_unixtime('timestamp'))

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

display(disturbances_df.withColumn("stations", extract_title_udf(col('title')))
                       .withColumn('station', explode(col("stations")))
                       .select("id", "title", "station")
                       .limit(50))

# COMMAND ----------

# Construct a table of all names found in the station data
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
                           .withColumn('allNames', dedup_names_udf(concat(col('names'), col('standardnames'))))\
                           .withColumn('station', explode(col('allNames')))

# COMMAND ----------

# MAGIC %md
# MAGIC **Note on station names**: The API does not return all station names in all languages. This can lead to failed matching with the disturbances. To avoid this, the translated station names should be retrieved and added to the stations dataframe.

# COMMAND ----------

disturbance_locations_df = disturbances_df\
                            .withColumn("stations", extract_title_udf(col('title')))\
                            .withColumn('station', explode(col("stations")))\
                            .withColumnRenamed("id", "disturbance_id")\
                            .join(all_names_df, on="station", how="left")

# COMMAND ----------

display(disturbance_locations_df.select("disturbance_id", "title", "station", "locationX", "locationY").dropDuplicates(["disturbance_id", "title", "station"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Enriching with zipcode data

# COMMAND ----------

zipcodes_df = spark.createDataFrame(pd.read_csv("./zipcodes_BE.csv", sep=";"))

# COMMAND ----------

disturbance_locations_df = disturbance_locations_df.join(zipcodes_df, on=zipcodes_df.Plaatsnaam == disturbance_locations_df.station, how="left")\
                                                   .withColumnRenamed("Postcode", "zipcode")

# COMMAND ----------

# MAGIC %md
# MAGIC Enriching with NIS codes

# COMMAND ----------

niscodes_df = spark.createDataFrame(pd.read_excel("./REFNIS_2019.xls"))\
              .withColumnRenamed('Entités administratives', "placename_FR")\
              .withColumnRenamed('Administratieve eenheden', "placename_NL")\
              .withColumnRenamed('Code NIS', 'niscode')

# COMMAND ----------

join_condition = [(disturbance_locations_df.station == niscodes_df.placename_FR) | (disturbance_locations_df.station == niscodes_df.placename_NL)]
disturbance_locations_df = disturbance_locations_df.join(niscodes_df, on=join_condition, how="left")

# COMMAND ----------

display(disturbance_locations_df.select('disturbance_id', 'title', 'station', 'zipcode', 'niscode'))

# COMMAND ----------

# MAGIC %md
# MAGIC Checking how which % of the stations we can place using zip/nis codes

# COMMAND ----------

nis_null = disturbance_locations_df.where(isnull(col('niscode'))).count()
zip_null = disturbance_locations_df.where(isnull(col('zipcode'))).count()
total_count = disturbance_locations_df.count()

# COMMAND ----------

print(f"Zip emptiness: {zip_null} null out of {total_count} ({100*zip_null/total_count:.1f}%)")
print(f"NIS emptiness: {nis_null} null out of {total_count} ({100*nis_null/total_count:.1f}%)")

# COMMAND ----------

responses

# COMMAND ----------

all_names_df.show()

# COMMAND ----------

# set mount location variable

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'

mount_location = "/mnt/"+storage_account+"/"+storage_container

# Write pyspark df to Parquet file on bronze container with /disturbances/yyyymmddhhmm

#current_timestamp = datetime.datetime.now(pytz.timezone('Europe/Brussels')).strftime("%Y%m%d%H%M")
all_names_df.write.parquet(f"{mount_location}/stations_geo_data")

# COMMAND ----------


