# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Building a Spark Data pipeline with Delta Lake
# MAGIC
# MAGIC With this notebook we are buidling an end-to-end pipeline consuming a railtime API and implementing a *medaillon / multi-hop* architecture going from bronze to gold (no landing for simplification reasons).
# MAGIC
# MAGIC With traditional systems this can be challenging due to:
# MAGIC  * data quality issues
# MAGIC  * running concurrent operations
# MAGIC  * running DELETE/UPDATE/MERGE operations on files
# MAGIC  * governance & schema evolution
# MAGIC  * poor performance from ingesting millions of small files on cloud blob storage
# MAGIC  * processing & analysing unstructured data (image, video...)
# MAGIC  * switching between batch or streaming depending of your requirements...
# MAGIC
# MAGIC ## Overcoming these challenges with Delta Lake
# MAGIC
# MAGIC <div style="float:left">
# MAGIC
# MAGIC **What's Delta Lake? It's a OSS standard that brings SQL Transactional database capabilities on top of parquet files!**
# MAGIC
# MAGIC Used as a Spark format, built on top of Spark API / SQL
# MAGIC
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (Expectations, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with Z-Order, data skipping and Caching, which solve the small files problem 
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="height: 200px"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Steps
# MAGIC
# MAGIC We will develop the following pipeline, introducing a couple of discussed concepts and functions:
# MAGIC
# MAGIC  * 1: API call --> Parse to parquet --> store on bronzen layer with .write.parquet
# MAGIC  * 2: Bronzen layer --> Autoloader --> store on silver layer with delta format
# MAGIC  * 3: Silver layer --> Read delta path into Structured Streaming --> Transformations --> store on golden layer with delta format

# COMMAND ----------

# DBTITLE 1,Let's start off with importing some usefull/needed packages
# Import packages

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

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Exploring the dataset
# MAGIC
# MAGIC Let's first query our railtime API, parse the JSON and store it on the bronze container of our storage account

# COMMAND ----------

# Save json response as variable

response = requests.post("https://api.irail.be/disturbances/?format=json&lineBreakCharacter=''&lang=nl").json()

# Write a statement to be able to inspect above JSON to validate whether it is looking like a valid JSON

'''''''''''''

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Display active mounts

# COMMAND ----------

# DBTITLE 1,Check whether our mount location is configured
dbutils.fs.mounts()

# find a more convenient way to show this 


# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Creating our mount variable
# MAGIC
# MAGIC Let's create a mount variable which we can use to refer to

# COMMAND ----------

# save storage account name and container name
# If you have trouble with retrieving the adlsname: 'adlsdbnotrialdev2023'

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'exercise'

# use both for the mount path

mount_location = f"/mnt/{storage_account}/{storage_container}"

# Print the above location. Is it what you expected? What could be going on?

'''''''''''''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Parse json and visualize it

# COMMAND ----------

# DBTITLE 1,# Create Pyspark dataframe from JSON response 
import http
import json, requests, re
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re

# Create Pyspark dataframe from JSON response 

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
                      ]))#.withColumn('schema_drift', lit('this_will_cause_schema_drift'))\
                         #.withColumn('import_timestamp', current_timestamp())\
                         #.withColumn('created_by', lit('Levi Devos'))      

display(disturbances_df)

##EXERCISE: insert a new column which you think is appropriate for the bronze layer

# COMMAND ----------

# DBTITLE 1,1: Save Pyspark dataframe into bronze layer with suffix /yourname/layer/disturbances/yyyymmddhhmm
# Save the pyspark dataframe by hard coding the appropriate values: yourname, medaillon layer, ....

current_timestamp = datetime.datetime.now(pytz.timezone('Europe/Brussels')).strftime("%Y%m%d%H%M")

disturbances_df.write.parquet(f"{mount_location}/yourname/bronze/disturbances/{current_timestamp}")

##EXERCISE: finish the path statement in order to write to mount/yourname/bronze/...

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Loading our bronze data using Databricks Autoloader (cloud_files) onto silver
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="files/tables/schema.png"/>
# MAGIC </div>
# MAGIC
# MAGIC The Autoloader allows us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC
# MAGIC Let's use it to ingest the bronze parquet data being delivered in our storage account
# MAGIC into the *silver* layer

# COMMAND ----------

# DBTITLE 1,Create a new function "ingest_folder" which contains the parameters for autoloader
def new_function(folder, data_format, landing, table):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                      .option("cloudFiles.inferColumnTypes", "true")
                      #.option("cloudFiles.schemaEvolutionMode", "rescue")
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/yourname/silver/disturbances/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                      .load(folder))
                      
  #bronze_products = bronze_products.withColumn('staging_name', lit('silver'))\
  #                                 .withColumn('timestamp_from_unix', from_unixtime('timestamp'))

  
  return (bronze_products.writeStream
            .option("checkpointLocation",
                    f"{mount_location}/yourname/silver/disturbances/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            .trigger(once = True) #Remove for real time streaming
            .start(landing)
  )


##EXERCISE: Add columns which are appropriate for the silver layer
##EXERCISE: rename function to ingest_folder


# COMMAND ----------

# DBTITLE 1,Call our new function: Storing the parquet data in "silver" Delta table, supporting schema evolution and incorrect data
ingest_folder(f"{mount_location}/yourname/bronze/disturbances", 'parquet', f"{mount_location}/yourname/silver/disturbances", "disturbances_silver")

##EXERCISE: Complete the function call with 1) folder with parquet files you want to read (via mount location) 2) Data type which you are trying to read 3) File path on adls where you want the parquet files to land 4) name of the subfolder underneath schema and checkpointlocation

# COMMAND ----------

# DBTITLE 1,We can read the delta table from its source
# Load the data from its source.

from delta.tables import *
from pyspark.sql.functions import *

df = spark.read.load(f"{mount_location}/yourname/silver/disturbances")

display(df)
## EXERCISE: Complete the path in order to read the Delta table from your stream path 

# COMMAND ----------

# DBTITLE 1,We just realised we have to delete records with type = "planned" for compliance; let's fix that
deltaTable = DeltaTable.forPath(spark, f"{mount_location}/yourname/silver/disturbances")

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("type = 'planned'")

# EXERCISE: Make a modification in your Delta Table: https://docs.delta.io/0.4.0/delta-update.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Configuring autoloader to switch between 'merge' mode and 'rescue' mode
# MAGIC
# MAGIC Autoloader offers several configuration options in order to deal with schema drifts. Rerun the step # 1: Create Pyspark dataframe from JSON response and add an additional column. See what happens with the additional column when running your structured streaming with merge modus enabled. Afterwards, rerun this sequence and comment out the merge statement and use the rescue mode. Validate what happens with your saved data.

# COMMAND ----------

# DBTITLE 1,Display the history of your Delta Table - do you notice the modification(s) you made?
display(deltaTable.history())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Loading our silver delta table into a Spark stream, transforming and storing it onto the gold layer
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="files/tables/schema.png"/>
# MAGIC </div>
# MAGIC
# MAGIC The Autoloader allows us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC
# MAGIC Let's use it to ingest the silver delta table data being delivered in our storage account
# MAGIC into the *gold* layer

# COMMAND ----------

# DBTITLE 1,Use an aggregationt to offer a view on the data and store this as a delta table to the golden layer
silver_stream = (spark.readStream \
        .load(f"{mount_location}/yourname/silver/disturbances") \
        .groupBy("title").count().sort(desc("count")) \

      .writeStream
        .option("checkpointLocation", f"{mount_location}/yourname/gold/disturbances/aggegration/checkpoint/disturbances")
        .outputMode("complete")## Append is the default, but then you need to configure watermarks
        .trigger(once=True)
        .start(f"{mount_location}/yourname/gold/disturbances/aggegration/")
)
        
    

# COMMAND ----------

# DBTITLE 1,Show the delta table as a dataframe to inspect the output
gold_stream = DeltaTable.forPath(spark, f"{mount_location}/yourname/gold/disturbances/aggegration/")   
display(gold_stream.toDF())


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4/ As a final step, you can expose your silver/golden data to a Power BI dashboard 
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="files/tables/schema.png"/>
# MAGIC </div>
# MAGIC
# MAGIC In order to read delta tables from the ADLS, we use a custom function for power query which is explained on the Wiki:
# MAGIC
# MAGIC https://dev.azure.com/cloubis/Azure%20Data%20Platform/_wiki/wikis/Azure%20Data%20Platform%20Knowledge%20Hub/167/Connector-Delta-lake-to-power-BI

# COMMAND ----------


