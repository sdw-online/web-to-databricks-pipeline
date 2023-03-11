# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Mount Blob to DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  Create constants using secrets and other config details

# COMMAND ----------

client_id                      =    dbutils.secrets.get(scope="azure-02", key="client_id")
client_secret                  =    dbutils.secrets.get(scope="azure-02", key="client_secret")
tenant_id                      =    dbutils.secrets.get(scope="azure-02", key="tenant_id")
storage_account_name           =    dbutils.secrets.get(scope="azure-02", key="storage_account_name")
container_name                 =    dbutils.secrets.get(scope="azure-02", key="container_name")
sas_token                      =    dbutils.secrets.get(scope="azure-02", key="sas_token")
sas_connection_string          =    dbutils.secrets.get(scope="azure-02", key="connection_string")
# blob_service_sas_url           =    dbutils.secrets.get(scope="azure-02", key="blob_service_sas_url")


source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}-dbfs"
extra_configs = {
        f"fs.azure.account.auth.type.{storage_account_name}.blob.core.windows.net": "OAuth",
        f"fs.azure.account.oauth.provider.type.{storage_account_name}.blob.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{storage_account_name}.blob.core.windows.net": client_id,
        f"fs.azure.account.oauth2.client.secret.{storage_account_name}.blob.core.windows.net": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.blob.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token,
        f"fs.azure.account.key.<storage-account-name>.blob.core.windows.net": sas_connection_string
    
}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  Mount the Blob container into DBFS

# COMMAND ----------

dbutils.fs.mount(
    source         =   source_path ,
    mount_point    =   mount_point ,
    extra_configs  =   extra_configs 
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  List the objects in the DBFS mount point

# COMMAND ----------

dbutils.fs.ls(f"{mount_point}")

# COMMAND ----------

# dbutils.fs.unmount(f"{mount_point}")
