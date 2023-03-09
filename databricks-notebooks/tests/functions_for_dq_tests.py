import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Create Spark Session for call functions 

spark = SparkSession.builder.appName("data-quality-support-functions").getOrCreate()


# Create external functions to support data quality checks


# 1. Table existence checks

def check_if_table_exists(table_name, db_name):
    return spark.catalog.tableExists(f'{db_name}.{table_name}')


# 2. Columns existence checks

def check_if_columns_exist(dataframe, column_name):
    if column_name in dataframe.columns:
        return True
    else:
        return False

