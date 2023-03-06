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
# MAGIC * Convert dataframe into delta file and write to DBFS 
# MAGIC * Read delta file into structured streaming dataframe
# MAGIC * Trigger query to execute only when new files are dumped into the source directory (i.e. `trigger(once=True)`)
# MAGIC * Add checkpoint files to record last state of streaming query output before query shuts down (using `checkpointLocation`)
# MAGIC * Include schema enforcement (i.e. `enforceSchema=True`)
# MAGIC * Write streaming query output to `bronze_folder` in the `delta_folder` of the Blob container as a delta table ("bronze_table")
# MAGIC * Convert the bronze_table to temp view for data profiling analysis
# MAGIC * Copy the delta files from DBFS to Blob Storage 
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

client_id                      =    dbutils.secrets.get(scope="azure", key="client_id")
client_secret                  =    dbutils.secrets.get(scope="azure", key="client_secret")
tenant_id                      =    dbutils.secrets.get(scope="azure", key="tenant_id")
storage_account_name           =    dbutils.secrets.get(scope="azure", key="storage_account_name")
container_name                 =    dbutils.secrets.get(scope="azure", key="container_name")
sas_token                      =    dbutils.secrets.get(scope="azure", key="sas_token")
sas_connection_string          =    dbutils.secrets.get(scope="azure", key="sas_connection_string")
blob_service_sas_url           =    dbutils.secrets.get(scope="azure", key="blob_service_sas_url")


source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}-dbfs"



football_data_path_src = f"{mount_point}"
football_data_path_dbfs_tgt = f"/mnt/delta/"
football_data_path_azure_tgt = f"{mount_point}/delta/"

checkpoint_location = f"{football_data_path_dbfs_tgt}/checkpoint"

# COMMAND ----------

# List the objects in the DBFS mount point 
# dbutils.fs.ls(f"{football_data_path_src}")

# COMMAND ----------

# Delete checkpoint location
# dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Bronze zone
# MAGIC * Specify schema --- [x]
# MAGIC * Ingest CSV file into dataframe  --- [x]
# MAGIC * Convert dataframe into delta file and write to DBFS  --- [x] 
# MAGIC * Read delta file into structured streaming dataframe  --- []
# MAGIC 
# MAGIC ### Streaming Query
# MAGIC * Trigger query to execute only when new files are dumped into the source directory (i.e. `trigger(once=True)`)  --- []
# MAGIC * Switch on the CDC mechanism (by setting `ignoreChanges`=`False`)  --- [x]
# MAGIC * Add checkpoint files to record last state of streaming query output before query shuts down (using `checkpointLocation`)  --- [x]
# MAGIC * Include schema enforcement (i.e. `enforceSchema=True`)  --- [x]
# MAGIC * Write streaming query output to `bronze_folder` in the `delta_folder` of the Blob container as a delta table ("bronze_table")  --- []
# MAGIC * Convert the bronze_table to temp view for data profiling analysis  --- []
# MAGIC * Copy the delta files from DBFS to Blob Storage  --- [] 

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
# MAGIC ### Ingest CSV file into streaming dataframe

# COMMAND ----------

src_query = (spark.readStream
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(league_table_schema)
        .option("maxFilesPerTrigger", 2)
        .load(football_data_path_src)
     )

# COMMAND ----------

bronze_streaming_query = (src_query
                          .writeStream
                          .format("delta") 
                          .option("checkpointLocation", checkpoint_location)
                          .option("enforceSchema", True)
                          .option("mergeSchema", False)
                          .option("ignoreChanges", False)
                          .queryName("BRONZE_QUERY_LEAGUE_STANDINGS_01")
                          .outputMode("append")
                          .trigger(once=True)
                          .toTable("bronze_table") 
                         )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List the objects in the Delta folder in DBFS

# COMMAND ----------

# List the objects in the DBFS mount point where the Delta files reside
dbutils.fs.ls(f"{football_data_path_dbfs_tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Silver zone
# MAGIC * xxxxxxxxx --- [ ]
# MAGIC * xxxxxxxxx --- [ ]
# MAGIC * xxxxxxxxx --- [ ]
