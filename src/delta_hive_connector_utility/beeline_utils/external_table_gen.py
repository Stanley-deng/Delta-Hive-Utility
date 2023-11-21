import subprocess

def external_table_gen(database, delta_path, sink_table, schema):
    # Parse schema
    schema_str = ",".join([f"{field.name} {field.dataType}" for field in schema.fields])
    schema_str = schema_str.replace("StringType()", "STRING")
    schema_str = schema_str.replace("IntegerType()", "INT")
    schema_str = schema_str.replace("LongType()", "BIGINT")

    # Define the Hive SQL statement
    command = f"beeline -u jdbc:hive2://localhost:10000/{database} -n hive -p hive -e \"\
        ADD JAR /home/hadoop/delta-hive-assembly_2.12-3.1.0-SNAPSHOT.jar; \
        CREATE EXTERNAL TABLE {database}.{sink_table}_external ({schema_str}) \
        STORED BY 'io.delta.hive.DeltaStorageHandler' \
        LOCATION '{delta_path}/{sink_table}/';\""
    
    # Execute the Hive SQL statement
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Check if the command succeeded or failed
    if process.returncode != 0:
        print(f"Failed to generate external table for {sink_table}. Error: {stderr.decode('utf-8')}")
    else:
        print(f"Successfully generated external table for {sink_table}\n")