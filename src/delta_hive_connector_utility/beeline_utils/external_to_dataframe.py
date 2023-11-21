from pyhive import hive
from TCLIService.ttypes import TOperationState
from pyspark.sql.types import *
from delta import *

def delta_external_to_dataframe(database, table, set_hive_connector=False):
    conn = hive.Connection(host="localhost", port=10000, \
                           username="hadoop@ec2-57-180-27-135.ap-northeast-1.compute.amazonaws.com", \
                           database="lab_stanley_db") 
    
    cursor = conn.cursor()

    if set_hive_connector:
        cursor.execute("ADD JAR /home/hadoop/delta-hive-assembly_2.12-3.1.0-SNAPSHOT.jar")
        cursor.execute("SET hive.input.format=io.delta.hive.HiveInputFormat")
        cursor.execute("SET hive.tez.input.format=io.delta.hive.HiveInputFormat")

    # Read table head
    cursor.execute(f'SELECT * FROM {database}.{table}_external')
    raw_data = cursor.fetchall()

    # Read table schema
    cursor.execute(f'DESCRIBE {database}.{table}_external')
    schema_list = []
    for field_name in cursor.fetchall():
        if field_name[1] == 'int':
            schema_list.append(StructField(field_name[0], IntegerType(), True))
        elif field_name[1] == 'bigint':
            schema_list.append(StructField(field_name[0], LongType(), True))
        else:
            schema_list.append(StructField(field_name[0], StringType(), True))

    df_schema = StructType(schema_list)

    df_data = []
    for row in raw_data:
        processed_row = []
        for i, value in enumerate(row):
            field_type = df_schema[i].dataType
            if field_type == IntegerType():
                processed_row.append(int(value))
            elif field_type == LongType():
                processed_row.append(int(value))
            else:
                processed_row.append(str(value))
        df_data.append(tuple(processed_row))

    conn.close()
    cursor.close()
    
    return df_data, df_schema