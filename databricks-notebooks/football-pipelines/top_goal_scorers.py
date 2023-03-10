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
# MAGIC * Ingest JSON file into dataframe  --- [x]
# MAGIC * Add unique ID column --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Ingest JSON files from source directory into streaming dataframe --- [x]
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
football_data_path_for_src_json_files    = f"{mount_point}/src/top-goal-scorers-raw-data"
football_data_path_for_src_delta_files  = f"{mount_point}/src/delta"


# Target locations
football_data_path_for_tgt_delta_files = f"{mount_point}/tgt/delta/top_goal_scorers_delta_folder"

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




youngest_goal_scorers_gold    = gold_table + 'youngest_goal_scorers/delta_file'
top_goal_contributions_gold   = gold_table + 'top_goal_contributions/delta_file'



# COMMAND ----------

# List the objects in the DBFS mount point 
# dbutils.fs.ls(f"{football_data_path_for_src_JSON_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Clear objects for this session (only if you're restarting query operations)

# COMMAND ----------

# Delete objects for this session (checkpoint locations, tables etc)


DELETE_SESSION_OBJECTS = True
# DELETE_SESSION_OBJECTS = False


if DELETE_SESSION_OBJECTS:
    try:
        dbutils.fs.rm(bronze_checkpoint, True)
        dbutils.fs.rm(silver_checkpoint, True)

        # Drop directory
        dbutils.fs.rm(f"{football_data_path_for_tgt_delta_files}", recurse=True)
        print(">>> 1. Deleted checkpoint locations successfully ")
        
        
        # Drop Hive tables
        
        spark.sql(""" DROP TABLE IF EXISTS football_db.bronze_tbl; """)
        print(">>> 2. Deleted BRONZE TABLE successfully")
        
        
        spark.sql(""" DROP TABLE IF EXISTS football_db.silver_tbl; """)
        print(">>> 3. Deleted SILVER TABLE successfully")
        
        print('')
        print(">>>  Deleted all session objects successfully ")
    except Exception as e:
        print(e)
    
else:
    print("No session objects deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Bronze zone
# MAGIC * Specify schema --- [x]
# MAGIC * Create database objects --- [x]
# MAGIC * Ingest JSON file into dataframe  --- [x]
# MAGIC * Add unique ID column --- [x]
# MAGIC * List the objects in the Delta folder in DBFS --- [x]
# MAGIC * Ingest JSON files from source directory into streaming dataframe --- [x]
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, ArrayType, MapType



top_goal_scorers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("birth", StructType([
        StructField("date", StringType(), True),
        StructField("place", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("nationality", StringType(), True),
    StructField("height", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("injured", BooleanType(), True),
    StructField("photo", StringType(), True),
    StructField("0", StructType([
        StructField("team", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("logo", StringType(), True)
        ]), True),
        StructField("league", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("logo", StringType(), True),
            StructField("flag", StringType(), True),
            StructField("season", IntegerType(), True)
        ]), True),
        StructField("games", StructType([
            StructField("appearences", IntegerType(), True),
            StructField("lineups", IntegerType(), True),
            StructField("minutes", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("position", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("captain", BooleanType(), True)
        ]), True),
        StructField("substitutes", StructType([
            StructField("in", IntegerType(), True),
            StructField("out", IntegerType(), True),
            StructField("bench", IntegerType(), True)
        ]), True),
        StructField("shots", StructType([
            StructField("total", IntegerType(), True),
            StructField("on", IntegerType(), True)
        ]), True),
        StructField("goals", StructType([
            StructField("total", IntegerType(), True),
            StructField("conceded", IntegerType(), True),
            StructField("assists", IntegerType(), True),
            StructField("saves", IntegerType(), True)
        ]), True),
        StructField("passes", StructType([
            StructField("total", IntegerType(), True),
            StructField("key", IntegerType(), True),
            StructField("accuracy", IntegerType(), True)
        ]), True),
        StructField("tackles", StructType([
            StructField("total", IntegerType(), True),
            StructField("blocks", IntegerType(), True),
            StructField("interceptions", IntegerType(), True)
        ]), True),
        StructField("duels", StructType([
            StructField("total", IntegerType(), True),
            StructField("won", IntegerType(), True)
        ]), True),
        StructField("dribbles", StructType([
            StructField("attempts", IntegerType(), True),
            StructField("success", IntegerType(), True),
            StructField("past", IntegerType(), True)
        ]), True),
        StructField("fouls", StructType([
            StructField("drawn", IntegerType(), True),
            StructField("commited", IntegerType(), True)
        ]), True),
        StructField("cards", StructType([
            StructField("yellow", IntegerType(), True),
            StructField("yellowred", IntegerType(), True),
            StructField("red", IntegerType(), True)
        ]), True),
        StructField("penalty", StructType([
            StructField("won", IntegerType(), True),
            StructField("committed", IntegerType(), True),
            StructField("scored", IntegerType(), True),
            StructField("missed", IntegerType(), True),
            StructField("saved", IntegerType(), True)
        ]), True)
    
])
                
               )])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create database objects

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS football_db

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest JSON files from source directory into streaming dataframe

# COMMAND ----------

src_query = (spark.readStream
        .format("json")
        .option("inferSchema", "false")
        .option("maxFilesPerTrigger", 2)
        .schema(top_goal_scorers_schema)
        .load(football_data_path_for_src_json_files)
     )

# COMMAND ----------

display(src_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add unique ID column 

# COMMAND ----------

# from pyspark.sql.functions import concat, lit, lower, regexp_replace

# src_query = src_query.withColumn("team_id", concat(lower(regexp_replace("team", "\s+", "")), lit("_123")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write data from source query into bronze delta table 

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
# dbutils.fs.ls(f"{football_data_path_for_tgt_delta_files}")

# COMMAND ----------

# stop_here_and_re_execute_script

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics

# COMMAND ----------

if len(bronze_streaming_query.recentProgress) > 0:
    no_of_incoming_rows = bronze_streaming_query.recentProgress[0]['numInputRows']
    query_name = bronze_streaming_query.recentProgress[0]['name']
    query_execution_timestamp = bronze_streaming_query.recentProgress[0]['timestamp']
    bronze_sources = bronze_streaming_query.recentProgress[0]['sources'][0]['description']
    bronze_sink = bronze_streaming_query.recentProgress[0]['sink']['description']
    
    
    print(f'=================== DATA PROFILING METRICS: BRONZE ===================')
    print(f'======================================================================')
    print(f'')
    print(f'Bronze query name:                       {query_name}')
    print(f'New rows inserted into bronze table:     {no_of_incoming_rows}')
    print(f'Source:                                  "{bronze_sources}"  ')
    print(f'Sink:                                    "{bronze_sink}" ')
else:
    print('No changes appeared in the source directory')
    

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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save Hive tables as bronze delta tables

# COMMAND ----------

# Convert Hive table into data frame 
bronze_tbl_df = spark.read.table("football_db.bronze_tbl")


# Write bronze table data frame to delta table
(bronze_tbl_df
     .write
     .format("delta")
     .mode("append")
     .option("mergeSchema", True)
     .save(bronze_table)
)

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
# MAGIC ### Create silver table in Hive metastore

# COMMAND ----------

spark.sql(""" CREATE TABLE IF NOT EXISTS football_db.silver_tbl;  """)

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
                          .option("checkpointLocation", silver_checkpoint)
                          .option("mergeSchema", True)
                          .option("ignoreChanges", False)
                          .trigger(once=True)
                          .toTable("football_db.silver_tbl") 
)

# COMMAND ----------

# stop_here_and_re_execute_script

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics

# COMMAND ----------

if len(silver_streaming_df_2.recentProgress) > 0:
    no_of_incoming_rows = silver_streaming_df_2.recentProgress[0]['numInputRows']
    query_execution_timestamp = silver_streaming_df_2.recentProgress[0]['timestamp']
    silver_sources = silver_streaming_df_2.recentProgress[0]['sources'][0]['description']
    silver_sink = silver_streaming_df_2.recentProgress[0]['sink']['description']
    
    
    print(f'=================== DATA PROFILING METRICS: SILVER ===================')
    print(f'======================================================================')
    print(f'')
    print(f'New rows inserted into silver table:     {no_of_incoming_rows}')
    print(f'Source:                                  "{silver_sources}"  ')
    print(f'Sink:                                    "{silver_sink}" ')
else:
    print('No changes appeared in the source directory')
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Analyze the silver streaming results

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DESCRIBE HISTORY football_db.silver_tbl
# MAGIC 
# MAGIC -- select * from football_db.bronze_tbl version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM football_db.silver_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save Hive tables as silver delta tables

# COMMAND ----------

# Convert Hive table into data frame 
silver_tbl_df = spark.read.table("football_db.silver_tbl")


# Write silver table data frame to delta table
(silver_tbl_df
     .write
     .format("delta")
     .mode("append")
     .option("mergeSchema", True)
     .save(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Gold zone
# MAGIC * Use silver delta table as source for data frame for gold zone --- [x]
# MAGIC * Convert gold data frame to temp view  --- [x] 
# MAGIC * Persist dataframe to cache  --- [x]
# MAGIC * Drop duplicates form previous append operations  --- [x]
# MAGIC * Use aggregate operations to summarize table standings data  --- [x]
# MAGIC * Create and visualize the aggregate tables  --- [x]
# MAGIC 
# MAGIC 
# MAGIC **Create the following tables:**
# MAGIC * 1. Premier League table
# MAGIC * 2. Teams with Most Wins
# MAGIC * 3. Teams with Most Goals Scored

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Use silver delta table as source for gold data frame

# COMMAND ----------

gold_tbl_df = (spark
.read
.format("delta")
.load(silver_table)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Convert gold data frame to temp view

# COMMAND ----------

gold_tbl_df.createOrReplaceTempView("gold_df_sql")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Persist dataframe to cache

# COMMAND ----------

gold_tbl_df.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Drop duplicates form previous append operations

# COMMAND ----------

gold_tbl_df = gold_tbl_df.dropDuplicates()
display(gold_tbl_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use aggregate operations to summarize table standings data

# COMMAND ----------

import plotly.express as px
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1. Premier League Table 

# COMMAND ----------

def create_premier_league_table(df):
    try:
        print('Using the source gold data frame to create the Premier League delta table ... ')
        print('')
        df.createOrReplaceTempView("premier_league_tbl_sql")

        df = spark.sql("""
                            SELECT DISTINCT     ranking
                                            , team
                                            , matches_played
                                            , wins
                                            , draws
                                            , losses
                                            , goals_for
                                            , goals_against
                                            , goal_difference
                                            , points 
                        FROM (
                                SELECT ranking
                                       , team
                                       , matches_played
                                       , wins
                                       , draws
                                       , losses
                                       , goals_for
                                       , goals_against
                                       , goal_difference
                                       , points 
                                       , RANK() OVER (PARTITION BY team 
                                                           ORDER BY matches_played 
                                                                 DESC) as rank 
                                 FROM gold_df_sql
                        )
                        WHERE      rank = 1
                        ORDER BY   ranking ASC

        """)
        df.write.format("delta").mode("overwrite").save(premier_league_table_gold)
        print(f'Successfully created the Premier League delta table in the "{premier_league_table_gold}" location ')
    
    except Exception as e:
        print(e)


# COMMAND ----------

def plot_premier_league_table(df):
    try:
        print('Plotting the Premier League table using Databricks ... ')
        print('')
        df.createOrReplaceTempView("premier_league_tbl_sql")

        df = spark.sql("""

            SELECT  ranking
                   , team
                   , matches_played
                   , wins
                   , draws
                   , losses
                   , goals_for
                   , goals_against
                   , goal_difference
                   , points  
             FROM premier_league_tbl_sql    

        """)
    
    except Exception as e:
        print(e)
    
    
    return display(df)
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. Teams with Most Wins

# COMMAND ----------

def create_teams_with_most_wins_table(df):
    try:
        print('Using the source gold data frame to create the table for teams with most wins ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_wins_tbl_sql")

        df = spark.sql("""
                            SELECT DISTINCT     ranking
                                                , team
                                                , matches_played
                                                , wins
                                                , draws
                                                , losses
                                                , goals_for
                                                , goals_against
                                                , goal_difference
                                                , points 
                        FROM (
                                SELECT ranking
                                       , team
                                       , matches_played
                                       , wins
                                       , draws
                                       , losses
                                       , goals_for
                                       , goals_against
                                       , goal_difference
                                       , points 
                                       , RANK() OVER (PARTITION BY team 
                                                           ORDER BY matches_played 
                                                                 DESC) as rank 
                                 FROM gold_df_sql
                        )
                        WHERE      rank = 1
                        ORDER BY   ranking ASC

        """)
        df.write.format("delta").mode("overwrite").save(teams_with_most_wins_table_gold)
        print(f'Successfully created the delta table for teams with most wins in "{teams_with_most_wins_table_gold}" location... ')
    
    except Exception as e:
        print(e)


# COMMAND ----------

def plot_teams_with_most_wins_table(df):
    try:
        print('Plotting the table for teams with most wins using Plotly ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_wins_tbl_sql")

        # Use the Premier League temp view 
        df = spark.sql("""

            SELECT * FROM premier_league_tbl_sql    

        """)

        df = df.toPandas()
        
        fig = px.bar(df,
                    x="team",
                    y="wins",
                    color="team",
                     title="Teams with most wins"
                    )
    
    except Exception as e:
        print(e)
    
    
    return fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3. Teams with Most Goals Scored

# COMMAND ----------

def create_teams_with_most_goals_scored_table(df):
    try:
        print('Using the source gold data frame to create the table for teams with most goals scored ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_goals_scored_tbl_sql")

        df = spark.sql("""
                            SELECT DISTINCT     ranking
                                                , team
                                                , matches_played
                                                , wins
                                                , draws
                                                , losses
                                                , goals_for
                                                , goals_against
                                                , goal_difference
                                                , points 
                        FROM (
                                SELECT ranking
                                       , team
                                       , matches_played
                                       , wins
                                       , draws
                                       , losses
                                       , goals_for
                                       , goals_against
                                       , goal_difference
                                       , points 
                                       , RANK() OVER (PARTITION BY team 
                                                           ORDER BY matches_played 
                                                                 DESC) as rank 
                                 FROM gold_df_sql
                        )
                        WHERE      rank = 1
                        ORDER BY   ranking ASC

        """)
        df.write.format("delta").mode("overwrite").save(teams_with_most_goals_scored_table_gold)
        print(f'Successfully created the delta table for teams with most goals scored in "{teams_with_most_goals_scored_table_gold}" location ... ')
    
    except Exception as e:
        print(e)


# COMMAND ----------

def plot_teams_with_most_goals_scored_table(df):
    try:
        print('Plotting the table for teams with most goals scored using Plotly ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_goals_scored_tbl_sql")
        
        
        # Use the Premier League temp view 
        df = spark.sql("""

            SELECT * FROM premier_league_tbl_sql    

        """)

        df = df.toPandas()
        
        fig = px.bar(df,
                    x="team",
                    y="goals_for",
                    color="team",
                     title="Teams with most goals scored"
                    )
    
    except Exception as e:
        print(e)
    
    
    return fig.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM teams_with_most_goals_scored_tbl_sql

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create and visualize the aggregated tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1. Premier League Table 

# COMMAND ----------

# 1 - Create the delta table for the aggregate

create_premier_league_table(gold_tbl_df)

# COMMAND ----------

# 2 - Read the delta table into a data frame to create an aggregate table

premier_league_df = (spark
.read
.format("delta")
.load(premier_league_table_gold)
)

# COMMAND ----------

# 3 - Visualize the aggregate table

plot_premier_league_table(premier_league_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. Teams with Most Wins

# COMMAND ----------

# 1 - Create the delta table for the aggregate

create_teams_with_most_wins_table(gold_tbl_df)

# COMMAND ----------

# 2 - Read the delta table into a data frame to create an aggregate table

teams_with_most_wins_df = (spark
.read
.format("delta")
.load(teams_with_most_wins_table_gold)
)

# COMMAND ----------

# 3 - Visualize the aggregate table

plot_teams_with_most_wins_table(teams_with_most_wins_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3. Teams with Most Goals Scored

# COMMAND ----------

# 1 - Create the delta table for the aggregate

create_teams_with_most_goals_scored_table(gold_tbl_df)

# COMMAND ----------

# 2 - Read the delta table into a data frame to create an aggregate table

teams_with_most_goals_scored_df = (spark
.read
.format("delta")
.load(teams_with_most_goals_scored_table_gold)
)

# COMMAND ----------

# 3 - Visualize the aggregate table

plot_teams_with_most_goals_scored_table(gold_tbl_df)
