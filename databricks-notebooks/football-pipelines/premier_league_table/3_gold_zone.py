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

from pyspark import StorageLevel
gold_tbl_df.persist(StorageLevel.MEMORY_ONLY)

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
        spark.sql(""" DROP TABLE IF EXISTS football_db.gold_tbl """)
        df.write.saveAsTable("football_db.gold_tbl")
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

            SELECT  DISTINCT ranking
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
             ORDER BY ranking

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

            SELECT DISTINCT * FROM premier_league_tbl_sql    

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

            SELECT DISTINCT * FROM premier_league_tbl_sql    

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

# COMMAND ----------

# premier_league_df.orderBy("points", ascending=True)#.first()
display(premier_league_df.orderBy(["points", "goal_difference"], ascending=False))

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC # END OF NOTEBOOK
