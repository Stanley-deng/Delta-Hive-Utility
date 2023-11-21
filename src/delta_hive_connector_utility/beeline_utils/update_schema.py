from delta_hive_connector_utility.beeline_utils.drop_table import drop_table
from delta_hive_connector_utility.beeline_utils.external_table_gen import external_table_gen

def update_external_schema(database, delta_path, sink_table, new_schema):
    drop_table(database, sink_table+"_external")
    external_table_gen(database, delta_path, sink_table, new_schema)