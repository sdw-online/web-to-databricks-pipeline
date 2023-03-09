import pytest 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functions_for_dq_tests import check_if_table_exists


# Create a Spark Session for building the DQ tests 
spark = SparkSession.builder.appName("test-premier-league-table-app").getOrCreate()



# Create constants for tests 
database_name = "football_db"
table_name = "gold_tbl"
premier_league_table = spark.read.table(f"{database_name}.{table_name}")



# ================================== TABLE EXISTENCE CHECK ==================================

# Test the table exists in the appropriate database

def test_table_existence():
    assert check_if_table_exists(table_name, database_name) is True, f"INVALID TABLE: The '{table_name}' table does not exist in the '{database_name}' database ...   "




# ================================== ROW COUNT CHECK ==================================

# Test that the number of rows is equal to 20

def test_row_count():
    
    expected_row_count   =  20
    actual_row_count     =  premier_league_table.count()
    
    
    assert actual_row_count == expected_row_count, f"INVALID ROW COUNT: Premier League table contains {actual_row_count} rows instead of {expected_row_count} rows... "





# ================================== UNIQUENESS CHECK ==================================

# Test that the names for each club is unique across the Team column
def test_unique_names():
    
    expected_unique_names_count    =    20
    actual_unique_names_count      =    premier_league_table.select("team").distinct().count()
    
    
    assert actual_unique_names_count == expected_unique_names_count, f"INVALID UNIQUE NAMES COUNT: Premier League table contains is expected to have {expected_unique_names_count} unique names but has {actual_unique_names_count} instead ...  "



# ================================== TOTAL MATCHES PLAYED CALCULATION CHECK ==================================

# Test the total matches played for each team is calculated correctly - the matches played is the sum of each team's wins, draws and losses i.e. (W + D + L) = matches_played

def test_total_matches_played_calculation():
    
    expected_matches_played_error_count    =    0
    actual_matches_played_error_count      =    premier_league_table.filter(col("wins") + col("draws") + col("losses") != col("matches_played")).count()
    
    
    assert actual_matches_played_error_count == expected_matches_played_error_count, f"INVALID MATCHES PLAYED: There are {actual_matches_played_error_count} records that contain an incorrect value for the 'matches_played' field ... "



# ================================== POINTS CALCULATION CHECK ==================================

# Test the points accumulated by each team is calculated correctly - for each win 3 points are earned, and each draw earns 1 point i.e. (W x 3) + (D x 1) = Pts

def test_points_calculation():
    
    expected_points_error_count      =     0
    actual_points_error_count        =     premier_league_table.filter(col("points") !=  3 * col("wins")  + col("draws")).count()
    
    
    assert actual_points_error_count == expected_points_error_count, f"INVALID POINTS: There are {actual_points_error_count} records that contain an incorrect value for the 'points' field ... "




# ================================== RANKING ORDER CHECK 1 ==================================

# Test the team with the highest number of points is first and the team with the lowest is last

def test_ranking_order_1():
    top_team = premier_league_table.orderBy("points", ascending=False).first()
    
    expected_ranking_for_top_team = 1
    actual_ranking_for_top_team = top_team.ranking
    assert actual_ranking_for_top_team == expected_ranking_for_top_team, f"INVALID RANKING: The Premier league ranking contains the incorrect team at the top of the table. "


    
    
    
# ================================== RANKING ORDER CHECK 2 ==================================

# Test the team with the highest number of points is first and the team with the lowest is last

def test_ranking_order_2():
    bottom_team = premier_league_table.orderBy("points", ascending=True).first()
    
    expected_ranking_for_bottom_team = 20
    actual_ranking_for_bottom_team = bottom_team.ranking
    assert actual_ranking_for_bottom_team == expected_ranking_for_bottom_team, f"INVALID RANKING: The Premier league ranking contains the incorrect team at the bottom of the table ... "
    
    
    
    
    
    
    


# ================================== XXXXXXXXXXXXXXXXX ==================================



