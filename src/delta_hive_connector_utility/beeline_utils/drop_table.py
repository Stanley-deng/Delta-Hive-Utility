import subprocess

def drop_table(database, sink_table):
    command = f"beeline -u jdbc:hive2://localhost:10000 -n hive -p hive -e 'DROP TABLE {database}.{sink_table};'"

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f"Failed to drop table {database}.{sink_table}. Error: {stderr.decode('utf-8')}")
    else:
        print(f"Successfully dropped table {database}.{sink_table}")