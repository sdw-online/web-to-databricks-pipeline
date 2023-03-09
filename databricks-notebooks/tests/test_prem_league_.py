import pytest 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



# Create a Spark Session for tests 
spark = SparkSession.builder.appName("test-premier-league-table-app").getOrCreate()





"""

Data Quality Tests

Here are the data quality tests that need to be conducted on the output:


- Row count
- Unique club names
- Total matches played = W + D + L
- GD = GF - GA 
- Points = (W x 3) + (D x 1)
- Ranking order (1=first, 20=last)
- GD order - if two teams share the same points, the team with the higher goal difference should be first



"""






# ================================== ROW COUNT CHECK ==================================

# Test that the number of rows is equal to 20

def test_row_count():
    
    
    expected_row_count = 20
    actual_row_count = spark.read.table("football_db.gold_tbl").count()
    
    
    assert actual_row_count == expected_row_count, f"INVALID ROW COUNT: Premier League table containts {actual_row_count} rows instead of {expected_row_count} rows... "





# ================================== UNIQUENESS CHECK ==================================

# Test that the names for each club is unique across the Team column
def test_unique_names():
    pass



# ================================== POINTS CALCULATION CHECK ==================================

# Test the points accumulated by each team is calculated correctly - for each win 3 points are earned, and each draw earns 1 point i.e. (W x 3) + (D x 1) = Pts

def test_points_calculation():
    pass


# ================================== TOTAL MATCHES PLAYED CALCULATION CHECK ==================================

# Test the total matches played for each team is calculated correctly - the matches played is the sum of each team's wins, draws and losses i.e. (W + D + L) = matches_played

def test_total_matches_played_calculation():
    pass




# ================================== RANKING ORDER CHECK ==================================

# Test the team with the highest number of points is first and the team with the lowest is last

def test_ranking_order():
    pass


# ================================== GOAL DIFFERENCE ORDER CHECK ==================================

# Test the teams with the same number of points are ranked by their goal differences i.e. if more than one team share the same number of points, they ranking should be based on their GD values where the team with the highest GD go first and the team with the lowest go last

def test_goal_difference_order():
    pass


# ================================== XXXXXXXXXXXXXXXXX ==================================



