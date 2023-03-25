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

def ingest_folder(folder, data_format, table, duplicates_array):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaEvolutionMode","addNewColumns") # Add new columns to the schema
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                     .load(folder))
  
  bronze_products = bronze_products.dropDuplicates(duplicates_array)
                     
  return (bronze_products.writeStream
            .option("checkpointLocation",
                    f"{mount_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            #.trigger(once = True) #Remove for real time streaming
            .table(table)) #Table will be created if we haven't specified the schema first


# COMMAND ----------

# DBTITLE 1,Call upon function to start both streams
ingest_folder(mount_location + '/disturbances', 'parquet', 'stream_disturbances_bronze', ['title','timestamp'])

ingest_folder(mount_location + '/disturbances_enriched', 'parquet', 'stream_disturbances_bronze_enriched', ['title','timestamp','name'])


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

# Alternatively, read into Pyspark DF
#from pyspark.sql.functions import from_unixtime, desc, col
#disturbances_df = spark.read.table('stream_disturbances_bronze')

# COMMAND ----------

#display(disturbances_df.orderBy(col('timestamp').desc()).limit(20))

# COMMAND ----------

# DBTITLE 1,Stream_disturbances_bronze_enriched Delta table is now ready for querying
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it will be stored here 
# MAGIC select 
# MAGIC --from_unixtime(timestamp),
# MAGIC *
# MAGIC from stream_disturbances_bronze_enriched
# MAGIC order by import_timestamp desc
# MAGIC --limit 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it will be stored here 
# MAGIC select 
# MAGIC --from_unixtime(timestamp),
# MAGIC *
# MAGIC from stream_disturbances_bronze_enriched
# MAGIC order by import_timestamp desc
# MAGIC --limit 20
# MAGIC ;

# COMMAND ----------

