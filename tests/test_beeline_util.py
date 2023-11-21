from delta_hive_connector_utility.beeline_utils.drop_table import drop_table
from delta_hive_connector_utility.beeline_utils.drop_table import drop_table
from delta_hive_connector_utility.beeline_utils.external_table_gen import external_table_gen
from delta_hive_connector_utility.beeline_utils.external_to_dataframe import delta_external_to_dataframe
from delta_hive_connector_utility.config_spark import configure_spark
from pyhive import hive
from TCLIService.ttypes import TOperationState
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from delta import *

import pytest

database = "lab_stanley_db"
source_table = "transaction_record_partitioned"
sink_table = "beeline_test_table"
delta_path = "s3://labstanleybucket/spark_sql_warehouse/lab_stanley_db.db"

spark = configure_spark()

spark.sql(f"DROP TABLE IF EXISTS {database}.{sink_table}") 
df = spark.sql(f"select * from {database}.{source_table}")
df.write.format("delta").mode("overwrite").partitionBy('event_date').saveAsTable(f'{database}.{sink_table}')

def test_external_table_gen():
    df = spark.sql(f"select * from {database}.{sink_table}")
    external_table_gen(database, delta_path, sink_table, df.schema)
    assert spark.catalog.tableExists(f"{database}.{sink_table}_external") == True

def test_delta_external_to_dataframe():
    df_data, df_schema = delta_external_to_dataframe(database, sink_table, set_hive_connector=True)
    assert len(df_data) == 1000
    assert len(df_schema) == 3

def test_beeline_cmds():
    expect_df1 = [tuple(row) for row in spark.sql(f"select * from {database}.{sink_table} where event_date = '2023-06' limit 5").collect()]
    expect_df2 = [tuple(row) for row in spark.sql(f"select amount, count(*) as count from {database}.{sink_table} where amount = 6 group by amount").collect()]  
    expect_df3 = [tuple(row) for row in spark.sql(f"select event_date, sum(amount) as total_sales from {database}.{sink_table} where event_date = '2023-06' group by event_date").collect()]

    cursor = hive.Connection(host="localhost", port=10000).cursor()
    
    cursor.execute("ADD JAR /home/hadoop/delta-hive-assembly_2.12-3.1.0-SNAPSHOT.jar")
    cursor.execute("SET hive.input.format=io.delta.hive.HiveInputFormat")
    cursor.execute("SET hive.tez.input.format=io.delta.hive.HiveInputFormat")

    cursor.execute(f"SELECT * FROM {database}.{sink_table}_external WHERE event_date = '2023-06' LIMIT 5")
    actual_df1 = cursor.fetchall()
    cursor.execute(f"SELECT amount, COUNT(*) AS count FROM {database}.{sink_table}_external WHERE amount = 6 GROUP BY amount")
    actual_df2 = cursor.fetchall()
    cursor.execute(f"SELECT event_date, SUM(amount) AS total_sales FROM {database}.{sink_table}_external WHERE event_date = '2023-06' GROUP BY event_date")
    actual_df3 = cursor.fetchall()

    assert expect_df1 == actual_df1
    assert expect_df2 == actual_df2
    assert expect_df3 == actual_df3

    cursor.close()

def test_drop_table():
    spark.sql(f"DROP TABLE IF EXISTS {database}.{sink_table}")
    drop_table(database, sink_table+"_external")
    assert spark.catalog.tableExists(f"{database}.{sink_table}_external") == False

    spark.stop()