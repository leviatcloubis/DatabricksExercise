# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Create two streams from the following parquet files
# MAGIC - disturbances 
# MAGIC - disturbances_enriched 

# COMMAND ----------

# DBTITLE 1,Set mount location variable
storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'
mount_location = "/mnt/"+storage_account+"/"+storage_container


# COMMAND ----------

# DBTITLE 1,Create ingestion function to configure Autoloader in order to store parquet bronze data in "bronze" Delta tables, supporting schema evolution and incorrect data
# Create ingest function (autoloader)

def ingest_folder(folder, data_format, landing, duplicates_array, table):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                     .option("ignoreMissingFiles", "true") # If checkpoints contains files which can not be found --> No error
                     .option("cloudFiles.inferColumnTypes", "true") # Only for json and csv
                      .option("cloudFiles.schemaEvolutionMode","addNewColumns") # Write new column to the schema after stopping stream
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                     .load(folder))
  
  bronze_products = bronze_products.dropDuplicates(duplicates_array)
                     
  return (bronze_products.writeStream
            .format("delta")
            .option("checkpointLocation",
                    f"{mount_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            .trigger(once = True) #Remove for real time streaming
            .start(landing)
  )


# COMMAND ----------

# DBTITLE 1,Call upon function to start both streams
ingest_folder(mount_location + '/disturbances', 'parquet', mount_location + '/stream/disturbances', ['title','timestamp'], "disturbances")

ingest_folder(mount_location + '/disturbances_enriched', 'parquet', mount_location + '/stream/disturbances_enriched', ['title','timestamp','name'], "disturbances_enriched")


# COMMAND ----------

# DBTITLE 1,Stream_disturbances_bronze Delta table is now ready for querying
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it will be stored here 
# MAGIC select 
# MAGIC --from_unixtime(timestamp),
# MAGIC *
# MAGIC from stream_disturbances_bronze
# MAGIC order by timestamp_from_unix desc
# MAGIC limit 20
# MAGIC ;

# COMMAND ----------

print(mount_location + '/disturbances')

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it will be stored here 
# MAGIC select 
# MAGIC --from_unixtime(timestamp),
# MAGIC *
# MAGIC from stream_disturbances_bronze
# MAGIC order by timestamp_from_unix desc
# MAGIC limit 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC id, title, description, link
# MAGIC --,
# MAGIC --RANK() OVER (PARTITION BY title ORDER BY id)
# MAGIC from stream_disturbances_bronze
# MAGIC QUALIFY RANK() OVER (PARTITION BY title ORDER BY id) = 1
# MAGIC order by timestamp_from_unix desc

# COMMAND ----------

