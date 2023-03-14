# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Web to Databricks pipeline
# MAGIC 
# MAGIC 
# MAGIC ## Purpose
# MAGIC 
# MAGIC To compute the data collected from the Python API and scraping tools.
# MAGIC 
# MAGIC 
# MAGIC ## Architecture 
# MAGIC 
# MAGIC This architecure will adopt the Medallion architecture (a recommended model from Databricks that uses bronze-silver-gold zones to apply different computation methods at each layer). 
# MAGIC 
# MAGIC 
# MAGIC ## Initial Approach
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Bronze zone
# MAGIC * Specify schema --- [x]
# MAGIC * Create database objects --- [x]
# MAGIC * Ingest CSV file into dataframe  --- [x]
# MAGIC * Add unique ID column --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Ingest CSV files from source directory into streaming dataframe --- [x]
# MAGIC * Write data from source query into bronze delta table --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Display the data profiling metrics --- [x]
# MAGIC * Convert dataframe into delta file and write to DBFS  --- [x] 
# MAGIC * Read delta file into structured streaming dataframe  --- [x]
# MAGIC 
# MAGIC 
# MAGIC ### Silver zone
# MAGIC * Set up target and source tables to prepare for CDC operations --- [x]
# MAGIC * Create custom MERGE function to apply CDC to micro-batches dynamically --- [x]
# MAGIC * Specify transformation intents using functions --- [x]
# MAGIC * Begin silver streaming query using bronze delta table as the source --- [x]
# MAGIC * Apply transformation logic to streaming dataframes --- [x]
# MAGIC * Create silver table in Hive metastore --- [x]
# MAGIC * Write transformed data from source query into silver delta table  --- [x]
# MAGIC * Display the data profiling metrics --- [x]
# MAGIC * Analyze the silver streaming results --- [x]
# MAGIC * Save Hive tables as silver delta tables --- [x]
# MAGIC 
# MAGIC 
# MAGIC ### Gold zone
# MAGIC * Use silver delta table as source for data frame for gold zone --- [x]
# MAGIC * Convert gold data frame to temp view  --- [x] 
# MAGIC * Persist dataframe to cache  --- [x]
# MAGIC * Drop duplicates form previous append operations  --- [x]
# MAGIC * Use aggregate operations to summarize table standings data  --- [x]
# MAGIC * Create and visualize the aggregate tables  --- [x]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Specify the pipeline constants

# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="azure-02", key="storage_account_name")
container_name = dbutils.secrets.get(scope="azure-02", key="container_name")


source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}-dbfs"


# Source locations
football_data_path_for_src_csv_files    = f"{mount_point}/src/prem-league-raw-data/*.csv"
football_data_path_for_src_delta_files  = f"{mount_point}/src/delta"


# Target locations
football_data_path_for_tgt_delta_files = f"{mount_point}/tgt/delta/league_standings"
football_data_path_for_tgt_csv_files   = f"{mount_point}/tgt/csv/league_standings"

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


# Aggregations

premier_league_table_gold                  =   gold_table + 'premier_league_table/delta_file'
teams_with_most_wins_table_gold            =   gold_table + 'teams_with_most_wins/delta_file'
teams_with_most_goals_scored_table_gold    =   gold_table + 'teams_with_most_goals_scored/delta_file'

# COMMAND ----------

client_id                      =    dbutils.secrets.get(scope="azure-02", key="client_id")
client_secret                  =    dbutils.secrets.get(scope="azure-02", key="client_secret")
tenant_id                      =    dbutils.secrets.get(scope="azure-02", key="tenant_id")


subscription_id                =    dbutils.secrets.get(scope="azure-02", key="subscription_id")
connection_string              =    dbutils.secrets.get(scope="azure-02", key="connection_string")
resource_group                 =    dbutils.secrets.get(scope="azure-02", key="resource_group")

queue_connection_string       = dbutils.secrets.get(scope="azure-02", key="queue_service_sas_url")

schema_location                =    f"{mount_point}/src/_schema/prem_league_schema.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Clear objects for this session (only if you're restarting query operations)

# COMMAND ----------

# Delete objects for this session (checkpoint locations, tables etc)


# DELETE_SESSION_OBJECTS = True
DELETE_SESSION_OBJECTS = False


def drop_session_objects() -> None:
    if DELETE_SESSION_OBJECTS:
        try:

            # Drop directory
            dbutils.fs.rm(bronze_checkpoint, True)
            dbutils.fs.rm(silver_checkpoint, True)
            dbutils.fs.rm(f"{football_data_path_for_tgt_delta_files}", recurse=True)
            print(">>> 1. Deleted checkpoint locations successfully ")


            # Drop Hive tables

            # A. Bronze tables
            spark.sql(""" DROP TABLE IF EXISTS football_db.bronze_tbl; """)
            print(">>> 2. Deleted BRONZE TABLE successfully")

            # B. Silver tables
            spark.sql(""" DROP TABLE IF EXISTS football_db.silver_tbl_1; """)
            print(">>> 3. Deleted SILVER TABLE 1 successfully")


            spark.sql(""" DROP TABLE IF EXISTS football_db.silver_tbl_2; """)
            print(">>> 4. Deleted SILVER TABLE 2 successfully")

            # C. Gold tables
            spark.sql(""" DROP TABLE IF EXISTS football_db.gold_tbl; """)
            print(">>> 5. Deleted GOLD TABLE successfully")


            spark.sql(""" DROP TABLE IF EXISTS football_db.bronze_tbl_audit_log; """)
            print(">>> 6. Deleted audit log for BRONZE TABLE successfully")


            spark.sql(""" DROP TABLE IF EXISTS football_db.silver_tbl_audit_log; """)
            print(">>> 7. Deleted audit log for SILVER TABLE successfully")





            print('')
            print(">>>  Deleted all session objects successfully ")
            print("")
            print("")
            print("--------------------")
        except Exception as e:
            print(e)
            print("")
            print("")
            print("--------------------")

    else:
        print("No session objects deleted.")
        print("")
        print("")
        print("--------------------")
        
drop_session_objects()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Bronze zone
# MAGIC * Specify schema --- [x]
# MAGIC * Create database objects --- [x]
# MAGIC * Ingest CSV file into dataframe  --- [x]
# MAGIC * Add unique ID column --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Ingest CSV files from source directory into streaming dataframe --- [x]
# MAGIC * Write data from source query into bronze delta table --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Display the data profiling metrics --- [x]
# MAGIC * Convert dataframe into delta file and write to DBFS  --- [x] 
# MAGIC * Read delta file into structured streaming dataframe  --- [x]
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
    
    StructField("Pos", IntegerType(), False),
    StructField("Team", StringType(), False),
    StructField("P", IntegerType(), False),
    StructField("W", IntegerType(), False),
    StructField("D", IntegerType(), False),
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
    StructField("match_date", DateType(), True)
    
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create database objects

# COMMAND ----------

def create_football_database():
    db_name = 'football_db'
    spark.sql(f""" CREATE DATABASE IF NOT EXISTS {db_name}; """)
    if spark.catalog.databaseExists(db_name):
        print(f"The '{db_name}' database exists in the Spark catalogue. ")
        print("")
        print("")
        print("--------------------")
    else:
        print(f""" ERROR: Database {db_name} does not exist... """)
        print("")
        print("")
        print("--------------------")
    
create_football_database()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest CSV files from source directory into streaming dataframe

# COMMAND ----------

# Read cloud-sourced CSV data into bronze streaming dataframe

print('Reading CSV data into bronze streaming dataframe...')
src_query = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.clientId", client_id)
             .option("cloudFiles.clientSecret", client_secret)
             .option("cloudFiles.tenantId", tenant_id)
             .option("cloudFiles.subscriptionId", subscription_id)
             .option("cloudFiles.connectionString", connection_string)
             .option("cloudFiles.resourceGroup", resource_group)
             .option("cloudFiles.useNotifications", False)
             .option("cloudFiles.validateOptions", False)
             .option("header", True)
             .schema(league_table_schema)
             .load(football_data_path_for_src_csv_files)
     )
print('Completed')
print("")
print("")
print("--------------------")

# COMMAND ----------

from pyspark.sql.functions import concat, lit, lower, regexp_replace
print('Adding unique IDs to records...')
    
# Add unique IDs to each record 
src_query = src_query.withColumn("team_id", concat(lower(regexp_replace("team", "\s+", "")), lit("_123")))
print('Completed')
print("")
print("")
print("--------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write data from source query into bronze delta table

# COMMAND ----------

# Write bronze streaming content into Hive metatable

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
# MAGIC ### Stop notebook execution once streaming job is complete

# COMMAND ----------

dbutils.notebook.exit("Terminating the bronze query")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save Hive tables as bronze delta tables

# COMMAND ----------

# Convert Hive table into data frame 

def write_bronze_tbl_to_delta() -> None:
    print("Creating delta table...")
    print("")
    print("")
    bronze_tbl_df = spark.read.table("football_db.bronze_tbl")


    # Write bronze table data frame to delta table
    (bronze_tbl_df
         .write
         .format("delta")
         .mode("append")
         .option("mergeSchema", True)
         .option("checkpointLocation", bronze_checkpoint)
         .save(bronze_table)
    )
    print("Successfully created new Delta table.")
    print("")
    print("")
    print("")
    print("--------------------")

write_bronze_tbl_to_delta()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics
# MAGIC 
# MAGIC Create custom audit logs to analyse streaming job performance 

# COMMAND ----------

from pyspark.sql.streaming import DataStreamWriter

def render_bronze_query_metrics(bronze_streaming_query: DataStreamWriter):
    if len(bronze_streaming_query.recentProgress) > 0:
        bronze_query_id                        =    bronze_streaming_query.recentProgress[0]['id']
        bronze_run_id                          =    bronze_streaming_query.recentProgress[0]['runId']
        bronze_batch_id                        =    bronze_streaming_query.recentProgress[0]['batchId']
        bronze_query_name                      =    bronze_streaming_query.recentProgress[0]['name']
        bronze_query_execution_timestamp       =    bronze_streaming_query.recentProgress[0]['timestamp']
        bronze_sources                         =    bronze_streaming_query.recentProgress[0]['sources'][0]['description']
        bronze_sink                            =    bronze_streaming_query.recentProgress[0]['sink']['description']


        print(f'=================== DATA PROFILING METRICS: BRONZE ===================')
        print(f'======================================================================')
        print(f'')
        print(f'Query name:                              "{bronze_query_name}"')
        print(f'Query ID:                                "{bronze_query_id}"')
        print(f'Run ID:                                  "{bronze_run_id}" ')
        print(f'Batch ID:                                "{bronze_batch_id}" ')
        print(f'Execution Timestamp:                     "{bronze_query_execution_timestamp}" ')

        print('')
        print(f'Source:                                  "{bronze_sources}"  ')
        print(f'Sink:                                    "{bronze_sink}" ')
        print("")
        print("")
        print("")
        print("--------------------")
        
    else:
        print('No changes appeared in the source directory')
        print("")
        print("")
        print("")
        print("--------------------")

render_bronze_query_metrics(bronze_streaming_query)

# COMMAND ----------

from pyspark.sql.functions import explode 
from delta import *

def create_bronze_audit_log_tbl(bronze_table_path: str) -> None:
    # Read delta file into Delta table instance
    bronze_delta_df       = DeltaTable.forPath(spark, bronze_table)
    
    # Read the standard audit log into a dataframe instance
    bronze_audit_log_df   = bronze_delta_df.history() 
    
    # Explode the operationMetrics column
    exploded_bronze_audit_log_df = bronze_audit_log_df.select(
                    bronze_audit_log_df.version,
                    bronze_audit_log_df.timestamp,
                    bronze_audit_log_df.userId,
                    bronze_audit_log_df.userName,
                    bronze_audit_log_df.operation,
                    explode(bronze_audit_log_df.operationMetrics)
                    )
    
    # Select relevant columns and cast value column to integer 
    final_bronze_audit_log_df = exploded_bronze_audit_log_df.select(
                    bronze_audit_log_df.version,
                    bronze_audit_log_df.timestamp,
                    bronze_audit_log_df.userId,
                    bronze_audit_log_df.userName,
                    exploded_bronze_audit_log_df.operation,
                    exploded_bronze_audit_log_df.key,
                    exploded_bronze_audit_log_df.value.cast('int')
                    )
    
    
    # Create pivot table to convert audit log records to fields
    pivot_bronze_audit_log_df = final_bronze_audit_log_df.groupBy("operation", "version", "timestamp", "userId", "userName").pivot("key").sum("value")
    pivot_bronze_audit_log_df.createOrReplaceTempView("load_last_operation_to_bronze_audit_tbl")
    pivot_bronze_audit_log_df = pivot_bronze_audit_log_df.orderBy("version", ascending=False)
    
    # Create audit log table for bronze_tbl
    spark.sql(""" CREATE TABLE IF NOT EXISTS football_db.bronze_tbl_audit_log (
                    operation STRING,
                    version STRING,
                    timestamp TIMESTAMP,
                    userID DOUBLE,
                    userName STRING,
                    numFiles INT,
                    numOutputRows INT,
                    numOutputBytes INT                 
                    )
    ; """)

    # Log operations history of last streaming query to audit log 
    spark.sql("""  INSERT INTO football_db.bronze_tbl_audit_log 
                        SELECT * FROM load_last_operation_to_bronze_audit_tbl;
                    """)
    
    # Display the audit log  
    print("Audit log table for bronze delta table successfully created")
    display(pivot_bronze_audit_log_df)
    

create_bronze_audit_log_tbl(bronze_table)

# COMMAND ----------

dbutils.notebook.exit("Stop at the bronze audit log table.")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Analyze the bronze streaming results 

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
