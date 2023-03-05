# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Web to Databricks pipeline
# MAGIC 
# MAGIC 
# MAGIC ## Initial Approach
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
s3_bucket = dbutils.secrets.get(scope="aws", key="s3_bucket")
mount_name = "football-data"

source_path = f"s3a://{s3_bucket}"
mount_point = f"/mnt/{mount_name}"
extra_configs = {
    "fs.s3a.aws.access.key": access_key,
    "fs.s3a.aws.secret.key": secret_key,
}

football_data_path_src = f"{mount_point}/s3/"
football_data_path_dbfs_tgt = f"/mnt/delta/"
football_data_path_s3_tgt = f"{mount_point}/delta/"

# COMMAND ----------

# List the objects in the DBFS mount point 
# dbutils.fs.ls(f"{football_data_path_src}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Bronze zone
# MAGIC * Specify schema
# MAGIC * Ingest CSV file into dataframe
# MAGIC * Convert dataframe into delta file and write to DBFS 
# MAGIC * Read delta file into structured streaming dataframe
# MAGIC * Trigger query to execute only when new files are dumped into the source directory (i.e. `trigger(once=True)`)
# MAGIC * Add checkpoint files to record last state of streaming query output before query shuts down (using `checkpointLocation`)
# MAGIC * Include schema enforcement (i.e. `enforceSchema=True`)
# MAGIC * Write streaming query output to `bronze_folder` in the `delta_folder` of the S3 bucket as a delta table ("bronze_table")
# MAGIC * Convert the bronze_table to temp view for data profiling analysis
# MAGIC * Copy the delta files from DBFS to AWS S3 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Specify schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

league_table_schema = StructType([
    
    StructField("Pos", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("P", IntegerType(), True),
    StructField("W", IntegerType(), True),
    StructField("D", IntegerType(), True),
    StructField("L", IntegerType(), True),
    StructField("GF", IntegerType(), True),
    StructField("GA", IntegerType(), True),
    StructField("W.1", IntegerType(), True),
    StructField("D.1", IntegerType(), True),
    StructField("L.1", IntegerType(), True),
    StructField("GF.1", IntegerType(), True),
    StructField("GA.1", IntegerType(), True),
    StructField("GD", IntegerType(), True),
    StructField("Pts", IntegerType(), True)
    
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest CSV file into dataframe

# COMMAND ----------

df = (spark.read
        .format("csv")
        .option("header", "true")
        .schema(league_table_schema)
        .load(f"{football_data_path_src}/prem_league_table_2022-Nov-01.csv"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Convert dataframe into delta file and write to DBFS 

# COMMAND ----------

df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{football_data_path_dbfs_tgt}/prem_league_table_2022-Nov-01-delta_tbl")

# COMMAND ----------

# dbutils.fs.unmount(f"/mnt/{mount_name}")
