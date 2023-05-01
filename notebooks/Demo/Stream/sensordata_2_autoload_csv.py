# Databricks notebook source
# Create variable to retrieve mount value

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container_input = 'csv-staging'
storage_container_output = 'bronze'
mount_location = "/mnt/"+storage_account+"/"+storage_container_input
mount_location_output = "/mnt/"+storage_account+"/"+storage_container_output

# Retrieve secrets for Notifications

resource_group = dbutils.secrets.get(scope="sensordata", key = "resource_group")
subscriptionId = dbutils.secrets.get(scope="sensordata", key = "subscriptionId")
tenantId = dbutils.secrets.get(scope="sensordata", key = "tenantId")
clientId = dbutils.secrets.get(scope="sensordata", key = "clientId")
clientSecret = dbutils.secrets.get(scope="sensordata", key = "clientSecret")

# COMMAND ----------

# Create ingest function (autoloader)

def ingest_folder(folder, data_format, landing, table):
  bronze_products = (spark.readStream
                     .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                     .option("ignoreMissingFiles", "true") # If checkpoints contains files which can not be found --> No error
                     .option("ignoreCorruptFiles","true")
                     .option("cloudFiles.inferColumnTypes", "true")  #Only for json and csv
                     .option("cloudFiles.useNotifications", "true")
                    .option("cloudFiles.resourceGroup", resource_group)
                    .option("cloudFiles.subscriptionId", subscriptionId)
                    .option("cloudFiles.tenantId", tenantId)
                    .option("cloudFiles.clientId", clientId)
                    .option("cloudFiles.clientSecret", clientSecret)
                      .option("cloudFiles.schemaLocation",
                              f"{mount_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                     .load(folder))
  
  #bronze_products = bronze_products.dropDuplicates(duplicates_array)
                     
  return (bronze_products.writeStream
            .format("delta")
            .option("checkpointLocation",
                    f"{mount_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("mergeSchema", "true") #merge any new column dynamically
            #.trigger(once = True) #Remove for real time streaming
            .start(landing)
  )


# COMMAND ----------

ingest_folder(f"{mount_location}/staging", 'csv', mount_location + '/stream/', "sensordata_csv")

# COMMAND ----------


