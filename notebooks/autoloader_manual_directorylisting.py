# Databricks notebook source
storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container_input = 'bronze'
storage_container_output = 'bronze'
mount_location = "/mnt/"+storage_account+"/"+storage_container_input
mount_location_output = "/mnt/"+storage_account+"/"+storage_container_output

from delta.tables import *

def updateDeltaTable(unprocessed_file, batchID): 
    current_delta = DeltaTable.forPath(spark, mount_location + '/stream/')
    current_delta.alias('t')\
    .merge(unprocessed_file.alias('s'),'t.id=s.id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

# introduceren referentie DF moet via DeltaTable instances formaat (https://docs.delta.io/latest/api/python/index.html)
# Ook nog te bekijken wat 'm doet met csv instead of delta table zoals in uitgewerkt voorbeeld hieronder

# COMMAND ----------

# Create ingest function (autoloader)

def ingest_folder(folder, data_format, landing, table, duplicates_array):
  bronze_products = (spark.readStream
                     .format("cloudFiles")
                    .option("cloudFiles.format", data_format)
                     .option("ignoreMissingFiles", "true")  
                     .option("ignoreCorruptFiles","true")
                     .option("cloudFiles.inferColumnTypes", "true")  #Only for json and csv
                     .option("cloudFiles.allowOverwrites", "true")
                      .option("cloudFiles.schemaEvolutionMode", "none")
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                     .load(folder))
  
  #bronze_products = bronze_products.orderBy("timestamp").dropDuplicates(duplicates_array) #Hier kan je geen sortby doen spijtig genoeg
                     
  return (bronze_products.writeStream
            .foreachBatch(updateDeltaTable)
            .format("delta")
            .option("checkpointLocation",
                    f"{mount_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            .outputMode("append")
            .trigger(once = True) #Remove for £ time streaming
            #.table("deltatable")
            .start(landing)
  )


# COMMAND ----------

ingest_folder(f"{mount_location}/staging", 'csv', mount_location + '/stream/', "sensordata_from_csv", ["id"])

# COMMAND ----------

printstring

# COMMAND ----------

batch_1 = spark.read.load(mount_location + '/stream/')
 

# COMMAND ----------

display(batch_1)

# COMMAND ----------

# File location and type
file_location = f"{mount_location}/staging/batch_2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
batch_2 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(batch_2)

# COMMAND ----------

display(batch_1)

# COMMAND ----------

def updateDeltaTable(batch_2, batchID): 
    batch_1.alias('t').merge(batch_2.alias('s'),'t.id=s.id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



# COMMAND ----------

display(batch_1)

# COMMAND ----------

updateDeltaTable(batch_2,1)

# COMMAND ----------

batch_1_path = DeltaTable.forPath(spark, mount_location + '/stream/')


batch_1_path.alias('t').merge(batch_2.alias('s'),'t.id=s.id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------


batch_1_path.toDF().show()


# COMMAND ----------

  bronze_products = (spark.readStream
                     .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                     .option("ignoreMissingFiles", "true")  
                     .option("ignoreCorruptFiles","true")
                     .option("cloudFiles.inferColumnTypes", "true")  #Only for json and csv
                     .option("cloudFiles.allowOverwrites", "true")
                      .option("cloudFiles.schemaEvolutionMode", "none")
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/sensordata_from_csv") #Autoloader will automatically infer all the schema & evolution
                     .load(f"{mount_location}/staging"))

# COMMAND ----------

display(bronze_products)

# COMMAND ----------


