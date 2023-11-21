import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType
from delta import *
from pyspark.sql.functions import col, lit
import os
import subprocess

from pyspark.sql.functions import expr

from pyhive import hive
from TCLIService.ttypes import TOperationState

from delta_hive_connector_utility.config_spark import configure_spark
from delta_hive_connector_utility.beeline_utils.update_schema import update_external_schema
import pytest

database = "lab_stanley_db"
source_table = "transaction_record_partitioned"
sink_table = "delta_test_table"
delta_path = "s3://labstanleybucket/spark_sql_warehouse/lab_stanley_db.db"

spark = configure_spark()
spark.sql(f"DROP TABLE IF EXISTS {database}.{sink_table}") 
df = spark.sql(f"select * from {database}.{source_table}")
df.write.format("delta").mode("overwrite").partitionBy('event_date').saveAsTable(f'{database}.{sink_table}')

conn = hive.Connection(host="localhost", port=10000)
cursor = conn.cursor()

def test_add_column_to_internal():
    df = spark.sql(f"select * from {database}.{sink_table}")
    df = df.withColumn("location", lit('loc'))
    df.write.format("delta").option("mergeSchema", "true").mode("overwrite").partitionBy('event_date').saveAsTable(f'{database}.{sink_table}')
    # update external table schema
    update_external_schema(database, delta_path, sink_table, df.schema)

    # test query
    expected_df = [tuple(row) for row in df.collect()]
    
    cursor.execute("ADD JAR /home/hadoop/delta-hive-assembly_2.12-3.1.0-SNAPSHOT.jar")
    cursor.execute("SET hive.input.format=io.delta.hive.HiveInputFormat")
    cursor.execute("SET hive.tez.input.format=io.delta.hive.HiveInputFormat")

    cursor.execute(f"SELECT * FROM {database}.{sink_table}_external")
    actual_df1 = spark.createDataFrame(cursor.fetchall(), df.schema)

    assert expected_df == actual_df1
    

def test_remove_column_from_internal():
    # with pytest.raises(Exception):
    spark.sql(f"ALTER TABLE {database}.{sink_table} DROP COLUMN location")
    df = spark.sql(f"select * from {database}.{sink_table}")
    # update external table schema
    update_external_schema(database, delta_path, sink_table, df.schema)

    # test query
    cursor.execute("ADD JAR /home/hadoop/delta-hive-assembly_2.12-3.1.0-SNAPSHOT.jar")
    cursor.execute("SET hive.input.format=io.delta.hive.HiveInputFormat")
    cursor.execute("SET hive.tez.input.format=io.delta.hive.HiveInputFormat")

    cursor.execute(f"SELECT * FROM {database}.{sink_table}_external")

    cursor.close()
    conn.close()