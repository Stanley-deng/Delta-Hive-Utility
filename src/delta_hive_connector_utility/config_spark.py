import pyspark
from pyspark.conf import SparkConf
from delta import *
import os

def configure_spark():
    os.environ["SPARK_HOME"] = "/usr/lib/spark" 
    os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"

    conf = SparkConf().setAppName("app").setMaster("yarn") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .set("spark.hive.exec.dynamic.partition", "true") \
        .set("spark.hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.hive.exec.max.dynamic.partitions.pernode", "300") 

    builder = pyspark.sql.SparkSession.builder \
        .config(conf=conf) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.delta.logStore.class", "io.delta.storage.S3SingleDriverLogStore") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0,org.apache.hadoop:hadoop-aws:3.3.3") \
        .enableHiveSupport() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark