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


# Aggregations

premier_league_table_gold                               =   gold_table + 'premier_league_table/delta_file'
teams_with_most_wins_and_losses_table_gold              =   gold_table + 'teams_with_most_wins_and_losses/delta_file'
teams_with_most_goals_scored_and_conceded_table_gold    =   gold_table + 'teams_with_most_goals_scored_and_conceded/delta_file'



# COMMAND ----------

# List the objects in the DBFS mount point 
# dbutils.fs.ls(f"{football_data_path_for_src_csv_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Clear objects for this session

# COMMAND ----------

# Delete checkpoint locations


# DELETE_CHECKPOINT = True
DELETE_CHECKPOINT = False


if DELETE_CHECKPOINT:
    try:
        dbutils.fs.rm(bronze_checkpoint, True)
        dbutils.fs.rm(silver_checkpoint, True)

        # Drop directory
        dbutils.fs.rm(f"{football_data_path_for_tgt_delta_files}", recurse=True)
        print("Deleted checkpoints and directories successfully ")
    except Exception as e:
        print(e)
    
else:
    print("No checkpoints deleted.")

# COMMAND ----------

# %sql

# DROP TABLE IF EXISTS football_db.bronze_tbl;
# DROP TABLE IF EXISTS football_db.silver_tbl;

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

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS football_db

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS football_db.silver_tbl

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

from time import sleep


# Add simulated delay to process incoming rows into tables 
sleep(3)

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
# MAGIC ### Analyze the bronze streaming results in temp views 

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
     .mode("append")
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

# Set up target table 

target_delta_tbl = DeltaTable.forPath(spark, bronze_table)
target_delta_tbl.toDF().show()

# COMMAND ----------

# Set up source table 

source_delta_tbl = spark.read.table("football_db.bronze_tbl")
source_delta_tbl.show()

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
# MAGIC ### Start silver streaming query

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

# Rename columns 

silver_streaming_df_1 =  (
        silver_streaming_df_1.withColumnRenamed("Pos", "ranking")
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

silver_streaming_df_1 = silver_streaming_df_1.withColumn("wins", col("win_home") + col("win_away"))
silver_streaming_df_1 = silver_streaming_df_1.withColumn("draws", col("draw_home") + col("draw_away"))
silver_streaming_df_1 = silver_streaming_df_1.withColumn("losses", col("loss_home") + col("loss_away"))
silver_streaming_df_1 = silver_streaming_df_1.withColumn("goals_for", col("goals_for_home") + col("goals_for_away"))
silver_streaming_df_1 = silver_streaming_df_1.withColumn("goals_against", col("goals_against_home") + col("goals_against_away"))


# COMMAND ----------

# Organise the columns in a set order

silver_streaming_df_1 = silver_streaming_df_1.select(["ranking", "team", "matches_played", "wins", "draws", "losses", "goals_for", "goals_against", "goal_difference", "points"])

# COMMAND ----------

sleep(3)

# COMMAND ----------

silver_streaming_query = (silver_streaming_df_1
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

# MAGIC %md
# MAGIC 
# MAGIC ### Display the data profiling metrics

# COMMAND ----------

if len(silver_streaming_query.recentProgress) > 0:
    no_of_incoming_rows = silver_streaming_query.recentProgress[0]['numInputRows']
    query_execution_timestamp = silver_streaming_query.recentProgress[0]['timestamp']
    silver_sources = silver_streaming_query.recentProgress[0]['sources'][0]['description']
    silver_sink = silver_streaming_query.recentProgress[0]['sink']['description']
    
    
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
# MAGIC ### Analyze the silver streaming results in temp views 

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

# Convert Hive table into delta table 
silver_tbl_df = spark.read.table("football_db.silver_tbl")


# Save silver_tbl_df to delta folder
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
# MAGIC * Drop duplicates from data frame  --- [x]
# MAGIC * Use aggregate operations to summarize table standings data  --- [x]
# MAGIC * Create and visualize the aggregate tables  --- [x]
# MAGIC 
# MAGIC 
# MAGIC **Create the following tables:**
# MAGIC * 1. Premier League table
# MAGIC * 2. Teams with Most Wins/Losses
# MAGIC * 3. Teams with Most Goals Scored/Conceded

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Use silver delta table as source for gold delta table

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
# MAGIC ### Drop duplicates from data frame

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
        print('Plotting the Premier League table using Plotly ... ')
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

#         df = df.toPandas()
        
#         fig = px.bar(df,
#                     x="team",
#                     y="wins",
#                      title="Premier League Table"
#                     )
    
    
    except Exception as e:
        print(e)
    
    
#     return fig.show()
    return display(df)
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. Teams with Most Wins/Losses

# COMMAND ----------

def create_teams_with_most_wins_and_losses_table(df):
    try:
        print('Using the source gold data frame to create the table for teams with most wins/losses ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_wins_and_losses_tbl_sql")

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
        df.write.format("delta").mode("overwrite").save(teams_with_most_wins_and_losses_table_gold)
        print(f'Successfully created the delta table for teams with most wins/losses in "{teams_with_most_wins_and_losses_table_gold}" location... ')
    
    except Exception as e:
        print(e)


# COMMAND ----------

def plot_teams_with_most_wins_and_losses_table(df):
    try:
        print('Plotting the table for teams with most wins and losses using Plotly ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_wins_and_losses_tbl_sql")

        df = spark.sql("""

            SELECT * FROM teams_with_most_wins_and_losses_tbl_sql    

        """)

        df = df.toPandas()
        
        fig = px.bar(df,
                    x="team",
                    y="wins",
                    color="team",
                     title="Teams with most wins and losses"
                    )
    
    except Exception as e:
        print(e)
    
    
    return fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3. Teams with Most Goals Scored/Conceded

# COMMAND ----------

def create_teams_with_most_goals_scored_and_conceded_table(df):
    try:
        print('Using the source gold data frame to create the table for teams with most goals scored/conceded ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_goals_scored_and_conceded_tbl_sql")

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
        df.write.format("delta").mode("overwrite").save(teams_with_most_goals_scored_and_conceded_table_gold)
        print(f'Successfully created the delta table for teams with most goals scored/conceded in "{teams_with_most_goals_scored_and_conceded_table_gold}" location ... ')
    
    except Exception as e:
        print(e)


# COMMAND ----------

def plot_teams_with_most_goals_scored_and_conceded_table(df):
    try:
        print('Plotting the table for teams with most goals scored and conceded using Plotly ... ')
        print('')
        df.createOrReplaceTempView("teams_with_most_goals_scored_and_conceded_tbl_sql")

        df = spark.sql("""

            SELECT * FROM teams_with_most_goals_scored_and_conceded_tbl_sql    

        """)

        df = df.toPandas()
        
        fig = px.bar(df,
                    x="team",
                    y="goals_for",
                    color="team",
                     title="Teams with most goals scored and conceded"
                    )
    
    except Exception as e:
        print(e)
    
    
    return fig.show()

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
# MAGIC #### 2. Teams with Most Wins/Losses

# COMMAND ----------

# 1 - Create the delta table for the aggregate

create_teams_with_most_wins_and_losses_table(gold_tbl_df)

# COMMAND ----------

# 2 - Read the delta table into a data frame to create an aggregate table

teams_with_most_wins_and_losses_df = (spark
.read
.format("delta")
.load(teams_with_most_wins_and_losses_table_gold)
)

# COMMAND ----------

# 3 - Visualize the aggregate table

plot_teams_with_most_wins_and_losses_table(teams_with_most_wins_and_losses_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3. Teams with Most Goals Scored/Conceded

# COMMAND ----------

# 1 - Create the delta table for the aggregate

create_teams_with_most_goals_scored_and_conceded_table(gold_tbl_df)

# COMMAND ----------

# 2 - Read the delta table into a data frame to create an aggregate table

teams_with_most_goals_scored_and_conceded_df = (spark
.read
.format("delta")
.load(teams_with_most_goals_scored_and_conceded_table_gold)
)

# COMMAND ----------

# 3 - Visualize the aggregate table

plot_teams_with_most_goals_scored_and_conceded_table(gold_tbl_df)
