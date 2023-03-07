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

client_id = dbutils.secrets.get(scope="azure", key="client_id")
client_secret = dbutils.secrets.get(scope="azure", key="client_secret")
tenant_id = dbutils.secrets.get(scope="azure", key="tenant_id")
storage_account_name = dbutils.secrets.get(scope="azure", key="storage_account_name")
container_name = dbutils.secrets.get(scope="azure", key="container_name")
sas_token = dbutils.secrets.get(scope="azure", key="sas_token")
sas_connection_string = dbutils.secrets.get(scope="azure", key="sas_connection_string")
blob_service_sas_url = dbutils.secrets.get(scope="azure", key="blob_service_sas_url")


source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}-dbfs"


# Source locations
football_data_path_for_src_csv_files    = f"{mount_point}"
football_data_path_for_src_delta_files  = f"{mount_point}/src/delta"


# Target locations
football_data_path_for_tgt_delta_files = f"{mount_point}/tgt/delta/league_standings"

bronze_output_location  = f"{football_data_path_for_tgt_delta_files}/bronze"
silver_output_location  = f"{football_data_path_for_tgt_delta_files}/silver"
gold_output_location    = f"{football_data_path_for_tgt_delta_files}/gold"


# Checkpoint
bronze_checkpoint = f"{bronze_output_location}/_checkpoint"
silver_checkpoint = f"{silver_output_location}/_checkpoint"
gold_checkpoint = f"{gold_output_location}/_checkpoint"




# Standard Medallion Tables
bronze_table = bronze_output_location + "tables/"
silver_table = silver_output_location + "tables/"
gold_table = gold_output_location + "tables/base_file/"

# COMMAND ----------

# List the objects in the DBFS mount point 
# dbutils.fs.ls(f"{football_data_path_for_src_csv_files}")

# COMMAND ----------

# Delete checkpoint location
dbutils.fs.rm(bronze_checkpoint, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS football_db.bronze_tbl

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

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
    StructField("Pts", IntegerType(), True),
    StructField("match-date", DateType(), True)
    
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add sandbox database

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS football_db

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest CSV file into streaming dataframe

# COMMAND ----------

src_query = (spark.readStream
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("maxFilesPerTrigger", 2)
        .schema(league_table_schema)
        .load(football_data_path_for_src_csv_files)
     )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add unique ID column 

# COMMAND ----------

from pyspark.sql.functions import concat, lit, lower, regexp_replace

# src_query = src_query.withColumn("team_id", concat(lower(replace("src_query["team"]", " ", "")), lit("_123")))

src_query = src_query.withColumn("team_id", concat(lower(regexp_replace("team", "\s+", "")), lit("_123")))

# COMMAND ----------

bronze_streaming_query = (src_query
                          .writeStream
                          .format("delta") 
                          .option("checkpointLocation", bronze_checkpoint)
                          .option("enforceSchema", True)
                          .option("mergeSchema", False)
                          .option("ignoreChanges", False)
                          .queryName("BRONZE_QUERY_LEAGUE_STANDINGS_01")
                          .outputMode("append")
                          .trigger(once=True)
                          .toTable("football_db.bronze_tbl") 
                         )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List the objects in the Delta folder in DBFS

# COMMAND ----------

# List the objects in the DBFS mount point where the Delta files reside
dbutils.fs.ls(f"{football_data_path_for_tgt_delta_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics

# COMMAND ----------

# bronze_streaming_query.recentProgress

# COMMAND ----------

if len(bronze_streaming_query.recentProgress) > 0:
    no_of_incoming_rows = bronze_streaming_query.recentProgress[0]['numInputRows']
    query_name = bronze_streaming_query.recentProgress[0]['name']
    
    
    print(f'=================== DATA PROFILING METRICS ===================')
    print(f'==============================================================')
    print(f'')
    print(f'Bronze query name:                       {query_name}')
    print(f'New rows inserted into bronze table:     {no_of_incoming_rows}')
else:
    print('No changes appeared in the source directory')
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Analyze the streaing results in temp views  

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DESCRIBE HISTORY football_db.bronze_tbl
# MAGIC 
# MAGIC -- select * from football_db.bronze_tbl version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM football_db.bronze_tbl

# COMMAND ----------

# Convert Hive table into delta table 
bronze_tbl_df = spark.read.table("football_db.bronze_tbl")


# Save bronze_tbl_df to delta folder
(bronze_tbl_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", True)
     .save(bronze_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Silver zone
# MAGIC * Use bronze table as source table for silver table
# MAGIC * Perform MERGE operation between source and target tables --- [ ]
# MAGIC * Convert transformation intents into PySpark logic
# MAGIC * xxxxxxxxx --- [ ]

# COMMAND ----------

from delta.tables import *
import pyspark.sql.functions as func
from pyspark import StorageLevel 

# COMMAND ----------

# Load 
src_bronze_tbl_df = (spark
                 .readStream
                 .format("delta")
                 .load(bronze_table)
                )

# COMMAND ----------

# Set up target table 

target_delta_tbl = DeltaTable.forPath(spark, bronze_table)
target_delta_tbl.toDF().show()

# COMMAND ----------

# Set up source table 

source_delta_tbl = spark.read.table("football_db.bronze_tbl")
source_delta_tbl.show()

# COMMAND ----------

# Add column for adding flags to indentify updates or inserts  
source_delta_tbl = source_delta_tbl.withColumn("upsert_flag", lit("I"))

# COMMAND ----------

display(source_delta_tbl)

# COMMAND ----------

target_delta_tbl_df = target_delta_tbl.toDF()
source_delta_tbl = source_delta_tbl.join(target_delta_tbl_df, ["team_id"], "left").withColumn("upsert_flag", when(col("count") > 0, "U").otherwise("I"))

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

from pyspark.sql.functions import when, col


def mergeChangesToDF(microbatchDF, batchID):
    
    delta_df = DeltaTable.forPath(spark, bronze_table)
    
    # Set condition for MERGE 
    merge_condition = "target_tbl.team_id = source_tbl.team_id"
    
    
    # Drop duplicates from incoming source table  
    microbatchDF = microbatchDF.dropDuplicates(["team_id", "P"])
    
    
    # Perform the UPSERT 
    (delta_df.alias("target_tbl")
     .merge(microbatchDF
            .alias("source_tbl"), merge_condition)
     
     .whenMatchedUpdateAll("source_tbl.team_id  <>  target_tbl.team_id")
     .whenNotMatchedInsertAll()
     .execute()
    )
    
    
    return display(df)

# COMMAND ----------

silver_streaming_query = (src_bronze_tbl_df
                          .writeStream
                          .format("delta")
                          .outputMode("append")
                          .foreachBatch(mergeChangesToDF)
                          .option("checkpointLocation", silver_checkpoint)
                          .trigger(once=True)
                          .toTable("football_db.silver_tbl") 

)

# COMMAND ----------

mergeChangesToDF(src_bronze_tbl_df)
