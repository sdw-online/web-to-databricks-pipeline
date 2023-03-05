# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Web to Databricks pipeline
# MAGIC 
# MAGIC 
# MAGIC ## Approach
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Bronze zone
# MAGIC * Specify schema
# MAGIC * Ingest CSV file into dataframe
# MAGIC * Convert dataframe into delta file 
# MAGIC * Save delta file to folder in S3 bucket 
# MAGIC * Read delta file into structured streaming dataframe
# MAGIC * Trigger query to execute only when new files are dumped into the source directory (i.e. `trigger(once=True)`)
# MAGIC * Add checkpoint files to record last state of streaming query output before query shuts down (using `checkpointLocation`)
# MAGIC * Include schema enforcement (i.e. `enforceSchema=True`)
# MAGIC * Write streaming query output to `bronze_folder` in the `delta_folder` of the S3 bucket as a delta table ("bronze_table")
# MAGIC * Convert the bronze_table to temp view for data profiling analysis
# MAGIC 
# MAGIC 
# MAGIC ### Silver zone
# MAGIC * Transform delta table in silver zone 
# MAGIC 
# MAGIC 
# MAGIC ### Gold zone
# MAGIC * Aggregate results in gold zone 
# MAGIC * Visualize results in Power BI 

# COMMAND ----------

# Set up constants 

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

football_data_path = f"{mount_point}/s3/"

# COMMAND ----------

# List the objects in the DBFS mount point 
dbutils.fs.ls(f"{football_data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read data into Databricks

# COMMAND ----------

df = (spark.read
        .format("csv")
        .option("header", "true")
        .load(f"{football_data_path}/prem_league_table_2022-Nov-01.csv"))

# COMMAND ----------

display(df)

# COMMAND ----------

# dbutils.fs.unmount(f"/mnt/{mount_name}")
