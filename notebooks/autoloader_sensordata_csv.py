# Databricks notebook source
storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container_input = 'sensor-csvlanding'
storage_container_output = 'sensor-deltastream'
mount_location = "/mnt/"+storage_account+"/"+storage_container_input
mount_location_output = "/mnt/"+storage_account+"/"+storage_container_output


# COMMAND ----------

# Create ingest function (autoloader)

def ingest_folder(folder, data_format, landing, table):
  bronze_products = (spark.readStream
                     .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                     .option("ignoreMissingFiles", "true") # If checkpoints contains files which can not be found --> No error
                     #.option("cloudFiles.schemaHints", "id STRING")
                     .option("ignoreCorruptFiles","true")
                     .option("cloudFiles.inferColumnTypes", "true")  #Only for json and csv
                     .option("cloudFiles.useNotifications", "true")
                    #.option("cloudFiles.includeExistingFiles", "true")
                    #.option("ignoreExtension", "true")
                    .option("cloudFiles.resourceGroup", 'rg-cloubis-dbdemo-april2023-dev-westeu-1')
                    #.option("cloudFiles.region", omitted)
                    #.option("cloudFiles.connectionString", omitted)
                    .option("cloudFiles.subscriptionId", 'cc7a2c07-0b5a-428a-8c79-20197553677d')
                    .option("cloudFiles.tenantId", 'a082dbbc-d89d-4018-b336-7279f22177eb')
                    .option("cloudFiles.clientId", 'cddfbd1c-71d5-47d5-91f4-afb17a710fb6')
                    .option("cloudFiles.clientSecret", 'eed8Q~Cmzb~VHe4mmYVaTbAm2bL8ZpIIf8ClAbSc')
                      #.option("cloudFiles.schemaEvolutionMode","addNewColumns") # Write new column to the schema after stopping stream
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


