# Databricks notebook source
# Get authentication variables and databricks secret scope

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'
servicePrincipalId = dbutils.secrets.get(scope="principalid", key = "principalid") # Application (client) ID
ServicePrincipalKey = dbutils.secrets.get(scope="principalkey", key = "principalkey") # Generated client secret (value) for the application
tenantID = dbutils.secrets.get(scope="tenantid", key = "tenantid") # Directory (tenant) id of the application
Directory = "https://login.microsoftonline.com/"+tenantID+"/oauth2/token"

configs = {"fs.azure.account.auth.type" : "OAuth",
           "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" : servicePrincipalId,
          "fs.azure.account.oauth2.client.secret" : ServicePrincipalKey,
           "fs.azure.account.oauth2.client.endpoint" : Directory
            }

#Unmount if needed
dbutils.fs.unmount("/mnt/"+storage_account+"/"+storage_container)

# Mount data lake into DBFS at the mnt location

dbutils.fs.mount(
    source = "abfss://"+storage_container+"@"+storage_account+".dfs.core.windows.net/",
    mount_point = "/mnt/"+storage_account+"/"+storage_container,
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Get authentication variables and databricks secret scope

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'
servicePrincipalId = dbutils.secrets.get(scope="principalid", key = "principalid") # Application (client) ID
ServicePrincipalKey = dbutils.secrets.get(scope="principalkey", key = "principalkey") # Generated client secret (value) for the application
tenantID = dbutils.secrets.get(scope="tenantid", key = "tenantid") # Directory (tenant) id of the application
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

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'csv-staging'
servicePrincipalId = dbutils.secrets.get(scope="principalid", key = "principalid") # Application (client) ID
ServicePrincipalKey = dbutils.secrets.get(scope="principalkey", key = "principalkey") # Generated client secret (value) for the application
tenantID = dbutils.secrets.get(scope="tenantid", key = "tenantid") # Directory (tenant) id of the application
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

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'exercise'
servicePrincipalId = dbutils.secrets.get(scope="principalid", key = "principalid") # Application (client) ID
ServicePrincipalKey = dbutils.secrets.get(scope="principalkey", key = "principalkey") # Generated client secret (value) for the application
tenantID = dbutils.secrets.get(scope="tenantid", key = "tenantid") # Directory (tenant) id of the application
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

storage_account = dbutils.secrets.get(scope="adlsname", key = "adlsname") #Name of storage account
storage_container = 'bronze'
mount_location = f"/mnt/{storage_account}/{storage_container}"

dbutils.fs.ls(mount_location)


# COMMAND ----------

fileInfos = dbutils.fs.ls(f"{mount_location}/disturbances_enriched")  
monthPaths = []  
for fileinfo in fileInfos:  
    monthPaths.append(fileinfo.path)  
  
monthPaths.sort(reverse=True)  
monthPaths

# COMMAND ----------

import time

path = spark._jvm.org.apache.hadoop.fs.Path
fs = path(f"{mount_location}/stream/disturbances_enriched").getFileSystem(sc._jsc.hadoopConfiguration())

res = fs.listFiles(path(f"{mount_location}/stream/disturbances_enriched"), True)

while res.hasNext():
  file = res.next()
  localTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file.getModificationTime() / 1000))
  print(f"{file.getPath()}: {localTime}")


# COMMAND ----------


