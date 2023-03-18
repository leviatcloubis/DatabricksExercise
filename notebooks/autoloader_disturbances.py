# Databricks notebook source
# DBTITLE 1,Storing the raw data in "bronze" Delta tables, supporting schema evolution and incorrect data
# set mount location variable

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'
mount_location = "/mnt/"+storage_account+"/"+storage_container

# Create ingest function (autoloader)

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                     .load(folder))
  
  bronze_products = bronze_products.dropDuplicates(['title','timestamp'])
                     
  return (bronze_products.writeStream
            .option("checkpointLocation",
                    f"{mount_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            .trigger(once = True) #Remove for real time streaming
            .table(table)) #Table will be created if we haven't specified the schema first
  
ingest_folder(mount_location + '/disturbances', 'parquet', 'stream_disturbances_bronze')



# COMMAND ----------

# DBTITLE 1,Stream_disturbances_bronze Delta table is now ready for querying
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it will be stored here 
# MAGIC select from_unixtime(timestamp),
# MAGIC *
# MAGIC from stream_disturbances_bronze
# MAGIC order by 1 desc
# MAGIC ;

# COMMAND ----------


