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

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Silver zone
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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Set up target and source tables to prepare for CDC operations 

# COMMAND ----------

from delta.tables import *
import pyspark.sql.functions as func
from pyspark import StorageLevel 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### A. Target table

# COMMAND ----------

# Set up target table 

target_delta_tbl = DeltaTable.forPath(spark, bronze_table)
target_delta_tbl.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### B. Source table

# COMMAND ----------

# Set up source table 

source_delta_tbl = spark.read.table("football_db.bronze_tbl")
source_delta_tbl.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create custom MERGE function to apply CDC to micro-batches dynamically  

# COMMAND ----------

from pyspark.sql.functions import when, col


def mergeChangesToDF(df, batchID):
    
    # Create Delta table if it doesnt exist
    DeltaTable.createIfNotExists(spark, "football_db.bronze_tbl")
    
    # Specify the source and target tables
    target_tbl = DeltaTable.forPath(spark, bronze_table)
    source_tbl = df.alias("source_tbl")
    

    # Set condition for joining the tables 
    join_condition = target_tbl.team_id == source_tbl.team_id
    
    
    # Define the update statement for when the rows match
    update_statement = {
        "Pts": source_tbl.Pts,
        "W": source_tbl.W,
        "D": source_tbl.D,
        "L": source_tbl.L,
        "GF": source_tbl.GF,
        "GA": source_tbl.GA,
        "W.1": source_tbl["W.1"],
        "D.1": source_tbl["D.1"],
        "L.1": source_tbl["L.1"],
        "GF.1": source_tbl["GF.1"],
        "GA.1": source_tbl["GA.1"],
        "GD": source_tbl.GD,
        "match_date": source_tbl["match_date"],
        "update_flag": lit("U")  # Add a flag column for updates
    }

    # Define the insert statement for when the rows don't match
    insert_statement = {
        "Pos": source_tbl.Pos,
        "Team": source_tbl.Team,
        "P": source_tbl.P,
        "W": source_tbl.W,
        "D": source_tbl.D,
        "L": source_tbl.L,
        "GF": source_tbl.GF,
        "GA": source_tbl.GA,
        "W.1": source_tbl["W.1"],
        "D.1": source_tbl["D.1"],
        "L.1": source_tbl["L.1"],
        "GF.1": source_tbl["GF.1"],
        "GA.1": source_tbl["GA.1"],
        "GD": source_tbl.GD,
        "Pts": source_tbl.Pts,
        "match_date": source_tbl["match_date"],
        "team_id": source_tbl.team_id,
        "update_flag": lit("I")  # Add a flag column for inserts
    }
    
    
    # Perform the Delta merge operation
    (target_tbl
     .merge(
         source_tbl, join_condition)
     .whenMatchedUpdate(
         condition=join_condition,
         set=update_statement)
     .whenNotMatchedInsertAll(
         values=insert_statement)
     .orderBy("Pos", "P")
     .execute() 
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Specify transformation intents using functions

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

# Rename columns 

def rename_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("Pos", "ranking")
            .withColumnRenamed("Team", "team")
            .withColumnRenamed("P", "matches_played")
            .withColumnRenamed("Pts", "points")
            .withColumnRenamed("W", "win_home")
            .withColumnRenamed("D", "draw_home")
            .withColumnRenamed("L", "loss_home")
            .withColumnRenamed("GF", "goals_for_home")
            .withColumnRenamed("GA", "goals_against_home")
            .withColumnRenamed("W.1", "win_away")
            .withColumnRenamed("D.1", "draw_away")
            .withColumnRenamed("L.1", "loss_away")
            .withColumnRenamed("GF.1", "goals_for_away")
            .withColumnRenamed("GA.1", "goals_against_away")
            .withColumnRenamed("GD", "goal_difference")
         )

# COMMAND ----------

# Create new calculated columns for wins, draws, losses, goals_for, goals_against by combining the home & away columns 

def add_calculated_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("wins", col("win_home") + col("win_away"))
          .withColumn("draws", col("draw_home") + col("draw_away"))
          .withColumn("losses", col("loss_home") + col("loss_away"))
          .withColumn("goals_for", col("goals_for_home") + col("goals_for_away"))
          .withColumn("goals_against", col("goals_against_home") + col("goals_against_away")
                     )
)

# COMMAND ----------

# Re-organise the columns in a set order

def reorganize_columns(df: DataFrame) -> DataFrame:
    return (
        df.select(
            ["ranking", 
             "team", 
             "matches_played", 
             "wins", 
             "draws", 
             "losses", 
             "goals_for", 
             "goals_against", 
             "goal_difference", 
             "points"
            ]
        )
    )

# COMMAND ----------

# Deduplicate the records in the dataframe

def drop_duplicates(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(
            ["ranking", 
             "team", 
             "matches_played", 
             "wins", 
             "draws", 
             "losses", 
             "goals_for", 
             "goals_against", 
             "goal_difference", 
             "points"
            ]
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Begin silver streaming query using bronze delta table as the source

# COMMAND ----------

silver_streaming_df_1 = (spark
                             .readStream
                             .format("delta")
                             .load(bronze_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Apply transformation logic to streaming dataframes

# COMMAND ----------

# Apply transformations

silver_streaming_df_1 =  (silver_streaming_df_1.transform(rename_columns)
                                              .transform(add_calculated_columns)
                                              .transform(reorganize_columns)
                                              .transform(drop_duplicates)
                         )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create 1st silver table in Hive metastore

# COMMAND ----------

# spark.sql(""" DROP TABLE IF EXISTS football_db.silver_tbl_1; """)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM football_db.silver_tbl_1

# COMMAND ----------

def create_1st_stage_silver_tbl():
    silver_tbl_stage_1 = 'football_db.silver_tbl_1'
    spark.sql(""" CREATE TABLE IF NOT EXISTS football_db.silver_tbl_1;  """)
    if spark.catalog.tableExists(silver_tbl_stage_1):

        print(f"Successfully created '{silver_tbl_stage_1}' table . ")
        print("")
        print("")
        print("--------------------")
        
    else:
        print(f"The '{silver_tbl_stage_1}' table doesn't exist in the Spark catalogue... ")
        print("")
        print("")
        print("--------------------")

create_1st_stage_silver_tbl()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write transformed data from source query into silver delta table 

# COMMAND ----------

silver_streaming_df_2 = (silver_streaming_df_1
                          .writeStream
                          .format("delta")
                          .outputMode("append")
                          .foreachBatch(mergeChangesToDF)
                          .queryName("SILVER_QUERY_LEAGUE_STANDINGS_01")
                          .option("checkpointLocation", silver_checkpoint)
                          .option("mergeSchema", True)
                          .option("ignoreChanges", False)
                          .trigger(once=True)
                          .toTable("football_db.silver_tbl_1") 
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop notebook execution once streaming job is complete

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics for silver zone outputs

# COMMAND ----------

from pyspark.sql.streaming import DataStreamWriter

def render_silver_query_metrics(silver_streaming_query: DataStreamWriter):
    if len(silver_streaming_query.recentProgress) > 0:
        silver_query_id = silver_streaming_query.recentProgress[0]['id']
        silver_run_id = silver_streaming_query.recentProgress[0]['runId']
        silver_batch_id = silver_streaming_query.recentProgress[0]['batchId']
        silver_query_name = silver_streaming_query.recentProgress[0]['name']
        silver_query_execution_timestamp = silver_streaming_query.recentProgress[0]['timestamp']
        silver_sources = silver_streaming_query.recentProgress[0]['sources'][0]['description']
        silver_sink = silver_streaming_query.recentProgress[0]['sink']['description']

        
        
        print(f'=================== DATA PROFILING METRICS: SILVER ===================')
        print(f'======================================================================')
        print(f'')
        print(f'Query name:                              "{silver_query_name}"')
        print(f'Query ID:                                "{silver_query_id}"')
        print(f'Run ID:                                  "{silver_run_id}" ')
        print(f'Batch ID:                                "{silver_batch_id}" ')
        print(f'Execution Timestamp:                     "{silver_query_execution_timestamp}" ')

        print('')
        print(f'Source:                                  "{silver_sources}"  ')
        print(f'Sink:                                    "{silver_sink}" ')
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

        
render_silver_query_metrics(silver_streaming_df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Analyze the silver streaming results

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DESCRIBE HISTORY football_db.silver_tbl_1
# MAGIC 
# MAGIC -- select * from football_db.bronze_tbl version as of 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create 2nd silver table in Hive metastore + Drop duplicates form previous append operations

# COMMAND ----------

def create_2nd_stage_silver_tbl():
    silver_tbl_stage_2 = 'football_db.silver_tbl_2'
    spark.sql(f""" DROP TABLE IF EXISTS {silver_tbl_stage_2}; """)
    if not spark.catalog.tableExists(silver_tbl_stage_2):
        
        spark.sql(f""" CREATE TABLE IF NOT EXISTS football_db.silver_tbl_2 as 
                          SELECT DISTINCT * 
                          FROM football_db.silver_tbl_1
                          ;  
""")
        print(f"Successfully created '{silver_tbl_stage_2}' table . ")
        print("")
        print("")
        print("--------------------")
        display(spark.sql(f""" SELECT * FROM {silver_tbl_stage_2} ORDER BY ranking;   """))
        
    else:
        print(f"The '{silver_tbl_stage_2}' table still exists in the Spark catalogue... ")
        print("")
        print("")
        print("--------------------")
    
create_2nd_stage_silver_tbl()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save Hive tables as silver delta tables

# COMMAND ----------

def write_silver_tbl_to_delta() -> None:

    # Convert Hive table into data frame 
    silver_tbl_df = spark.read.table("football_db.silver_tbl_2")
    
    # Write silver table data frame to delta table
    (silver_tbl_df
         .write
         .format("delta")
         .mode("append")
         .option("mergeSchema", True)
         .option("checkpointLocation", silver_checkpoint)
         .save(silver_table)
    )
    print("Successfully created new Delta table.")
    print("")
    print("")
    print("")
    print("--------------------")

write_silver_tbl_to_delta()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics for silver streaming query

# COMMAND ----------

from pyspark.sql.functions import explode 
from delta import *

def create_silver_audit_log_tbl(silver_table_path: str) -> None:
    # Read delta file into Delta table instance
    silver_delta_df = DeltaTable.forPath(spark, silver_table_path)

    # Read the standard audit log into a dataframe instance
    silver_tbl_audit_log_df = silver_delta_df.history() 

    # Explode the required columns 
    explode_silver_tbl_audit_log_df_1 = silver_tbl_audit_log_df.select(
        silver_tbl_audit_log_df.version,
        silver_tbl_audit_log_df.timestamp,
        silver_tbl_audit_log_df.userId,
        silver_tbl_audit_log_df.userName,
        silver_tbl_audit_log_df.operation, 
        explode(silver_tbl_audit_log_df.operationMetrics)
    )

    explode_silver_tbl_audit_log_df_2 = explode_silver_tbl_audit_log_df_1.select(
        silver_tbl_audit_log_df.version,
        silver_tbl_audit_log_df.timestamp,
        silver_tbl_audit_log_df.userId,
        silver_tbl_audit_log_df.userName,
        explode_silver_tbl_audit_log_df_1.operation,
        explode_silver_tbl_audit_log_df_1.key,
        explode_silver_tbl_audit_log_df_1.value.cast('int')
    )


    # Create a pivot table to convert the audit log records to fields
    final_audit_log_silver_df = explode_silver_tbl_audit_log_df_2.groupBy(
        "operation", "version", "timestamp", "userId", "userName").pivot("key").sum("value")
    
    final_audit_log_silver_df = final_audit_log_silver_df.orderBy("version", ascending=False)
    final_audit_log_silver_df.createOrReplaceTempView("load_last_operation_to_silver_audit_tbl")
    display(final_audit_log_silver_df)

    # Create audit log table for silver_tbl
    spark.sql("""CREATE TABLE IF NOT EXISTS football_db.silver_tbl_audit_log (
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
    spark.sql("""INSERT INTO football_db.silver_tbl_audit_log 
                SELECT * FROM load_last_operation_to_silver_audit_tbl;
                """)

    # Display the audit log  
    print("Audit log table for silver delta table successfully created")

create_silver_audit_log_tbl(silver_table)


# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC # END OF NOTEBOOK
