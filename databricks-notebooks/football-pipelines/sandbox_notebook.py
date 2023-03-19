# Databricks notebook source
import pyspark
from pyspark.sql.functions import *



# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="azure-02", key="storage_account_name")
container_name = dbutils.secrets.get(scope="azure-02", key="container_name")


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

df_1 = spark.read.option("multiline", True).json(football_data_path_for_src_json_files)

# COMMAND ----------

display(df_1)

# COMMAND ----------

df_1.printSchema()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col

df_2 = (df_1.select(
                col("0.*")).alias("raw_profile_data")
       )
display(df_2)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_1 = df_1.withColumn("row_id", monotonically_increasing_id())
df_2 = df_2.withColumn("row_id", monotonically_increasing_id())

merged_df = df_1.join(df_2, on="row_id", how="outer").drop("row_id")
display(merged_df)

# COMMAND ----------

merged_df = merged_df.drop("0")
display(merged_df)

# COMMAND ----------

transformed_df = (merged_df.select(
                            col("firstname"),
                            col("lastname"),
    col("age"),
    col("birth.date").alias("date_of_birth"),
    col("birth.country").alias("country_of_birth"),
    col("birth.place").alias("place_of_birth"),
    col("height"),
    col("id").alias("player_id"),
    col("injured"),
    col("cards.red").alias("red_cards"),
    col("cards.yellow").alias("yellow_cards"),
    col("cards.yellowred").alias("yellow_to_red_cards"),
    col("dribbles.attempts").alias("dribbles_attempts"),
    col("dribbles.past").alias("dribbles_past"),
    col("dribbles.success").alias("dribbles_success"),
    col("duels.total").alias("duels_total"),
    col("duels.won").alias("duels_won"),
    col("fouls.committed").alias("fouls_committed"),
    col("fouls.drawn").alias("fouls_drawn"),
    col("games.appearences").alias("games_appearences"),
    col("games.captain").alias("games_captain"),
    col("games.minutes").alias("games_minutes"),
    col("games.lineups").alias("games_lineups"),
    col("games.position").alias("games_position"),
    col("games.rating").alias("games_rating"),
    col("goals.total").alias("goals_total"),
    col("goals.assists").alias("goals_assists"),
    col("league.country").alias("league_country"),
    col("league.id").alias("league_id"),
    col("league.name").alias("league_name"),
    col("league.logo").alias("league_logo"),
    col("passes.total").alias("passes_total"),
    col("passes.key").alias("passes_key"),
    col("passes.accuracy").alias("passes_accuracy"),
    col("penalty.scored").alias("penalty_scored"),
    col("penalty.missed").alias("penalty_missed"),
    col("shots.total").alias("shots_total"),
    col("shots.on").alias("shots_on_target"),
    col("substitutes.in").alias("substitutes_in"),
    col("substitutes.out").alias("substitutes_out"),
    col("substitutes.bench").alias("substitutes_bench"),
    col("tackles.blocks").alias("tackles_blocks"),
    col("tackles.interceptions").alias("tackles_interceptions"),
    col("tackles.total").alias("tackles_total"),
    col("team.id").alias("team_id"),
    col("team.name").alias("team_name"),
    col("team.logo").alias("team_logo")
    
    
))

    
#     ,
    

display(transformed_df)

# COMMAND ----------

merged_df.columns
