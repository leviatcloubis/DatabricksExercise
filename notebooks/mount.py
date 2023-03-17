# Databricks notebook source
# Get authentication variables and databricks secret scope

storage_account = 'adlsdbdemodevwesteu1'
storage_container = 'bronze'
servicePrincipalId = 'e325d73c-6f99-4a18-9806-6d9b1255b027'
ServicePrincipalKey = '8WL8Q~FgfsCrRs1g~DppuRBtfeqmmeM_x4zD3cKo'
tenantID = 'a082dbbc-d89d-4018-b336-7279f22177eb'
Directory = "https://login.microsoftonline.com/"+tenantID+"/oauth2/token"

configs = {"fs.azure.account.auth.type" : "OAuth",
           "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" : servicePrincipalId,
          "fs.azure.account.oauth2.client.secret" : ServicePrincipalKey,
           "fs.azure.account.oauth2.client.endpoint" : Directory
            }

#Unmount if needed
#dbutils.fs.unmount("/mnt/"+storage_account+"/"+storage_container)

# Mount data lake into DBFS at the mnt location

dbutils.fs.mount(
    source = "abfss://"+storage_container+"@"+storage_account+".dfs.core.windows.net/",
    mount_point = "/mnt/"+storage_account+"/"+storage_container,
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


