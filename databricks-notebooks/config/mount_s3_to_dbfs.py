# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Mount S3 to DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  Create constants using secrets and other config details

# COMMAND ----------

access_key = dbutils.secrets.get(scope="aws", key="aws_access_key")
secret_key = dbutils.secrets.get(scope="aws", key="aws_secret_key")
s3_bucket  = dbutils.secrets.get(scope="aws", key="s3_bucket")
mount_name = "football-data"

source_path = f"s3a://{s3_bucket}"
mount_point = f"/mnt/{mount_name}"
extra_configs = {
        "fs.s3a.aws.access.key": access_key,
        "fs.s3a.aws.secret.key": secret_key
    }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  Mount the S3 bucket into DBFS

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

dbutils.fs.ls(f"{mount_point}/s3/")
