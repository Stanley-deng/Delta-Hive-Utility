"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -m delta_hive_connector_utility` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``delta_hive_connector_utility.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``delta_hive_connector_utility.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from delta import *
from typing import List, Optional
from beeline_utils.drop_table import drop_table
from beeline_utils.external_table_gen import external_table_gen
from beeline_utils.select_table import select_table
# import typer
# from typing_extensions import Annotated

# main = typer.Typer()


# @main.command()
#names: Annotated[Optional[List[str]], typer.Argument()] = None
def main():
    database = "lab_stanley_db"
    source_table = "transaction_record_partitioned"
    sink_table = "delta_auto_demo"
    delta_path = "s3://labstanleybucket/spark_sql_warehouse/lab_stanley_db.db"

    # os.environ["SPARK_HOME"] = "/usr/lib/spark" 
    # os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"

    spark = configure_spark()

    spark.sql(f"DROP TABLE IF EXISTS {database}.{sink_table}")
    drop_table(database, sink_table+"_external")

    df = spark.sql(f"select * from {database}.{source_table}")

    df.write.format("delta").mode("overwrite").partitionBy('event_date').saveAsTable(f'{database}.{sink_table}')

    dst = spark.sql(f"select * from {database}.{sink_table}")
    print(f"\n {sink_table} write result \n")
    dst.show()

    # Generate external table
    external_table_gen(delta_path, sink_table, df.schema)

    # beeline commands
    sql_commands = [
        f"SELECT * FROM {database}.{sink_table}_external LIMIT 10;",
        f"SELECT COUNT(*) FROM {database}.{sink_table}_external WHERE event_date = '2023-06';",
        f"SELECT amount, COUNT(*) AS count FROM {database}.{sink_table}_external GROUP BY amount;",
        f"SELECT event_date, SUM(amount) AS total_sales FROM {database}.{sink_table}_external GROUP BY event_date;"
    ]
    select_table(database, set_hive_connector=True, sql_cmd=sql_commands)

    drop_table(database, sink_table)

def configure_spark():
    conf = SparkConf().setAppName("app").setMaster("yarn") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .set("spark.hive.exec.dynamic.partition", "true") \
        .set("spark.hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.hive.exec.max.dynamic.partitions.pernode", "300") \
        .set("spark.hive.metastore.warehouse.dir.", "s3://labstanleybucket/spark_sql_warehouse/") 

    builder = pyspark.sql.SparkSession.builder \
        .config(conf=conf) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .enableHiveSupport() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark