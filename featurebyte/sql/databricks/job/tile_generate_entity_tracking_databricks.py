# """
# Databricks Tile Generate Job Script
# """
#
# import argparse
#
# from pyspark.sql import SparkSession
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TileGenerateEntityTracking").getOrCreate()
#
#     parser = argparse.ArgumentParser()
#     parser.add_argument("featurebyte_database", type=str)
#     parser.add_argument("tile_last_start_date_coulmn", type=str)
#     parser.add_argument("entity_column_names", type=str)
#     parser.add_argument("tile_id", type=str)
#     parser.add_argument("entity_table", type=str)
#
#     args = parser.parse_args()
#
#     featurebyte_database = args.featurebyte_database
#     tile_last_start_date_coulmn = args.tile_last_start_date_coulmn
#     entity_column_names = args.entity_column_names
#     tile_id = args.tile_id
#     entity_table = args.entity_table
#
#     print("featurebyte_database: ", featurebyte_database)
#     print("tile_last_start_date_coulmn: ", tile_last_start_date_coulmn)
#     print("entity_column_names: ", entity_column_names)
#     print("tile_id: ", tile_id)
#     print("entity_table: ", entity_table)
#
#     spark.sql(f"USE DATABASE {featurebyte_database}")
#
#     tracking_table_name = tile_id + "_ENTITY_TRACKER"
#     tracking_table_exist = spark.catalog.tableExists(tracking_table_name)
#
#     entity_insert_cols = []
#     entity_filter_cols = []
#     for element in entity_column_names.split(","):
#         element = element.strip()
#         entity_insert_cols.append("b." + element)
#         entity_filter_cols.append("a." + element + " = b." + element)
#
#     entity_insert_cols_str = ",".join(entity_insert_cols)
#     entity_filter_cols_str = " AND ".join(entity_filter_cols)
#     print("entity_insert_cols_str: ", entity_insert_cols_str)
#     print("entity_filter_cols_str: ", entity_filter_cols_str)
#
#     merge_sql = f"""
#         merge into {tracking_table_name} a using {entity_table} b
#             on {entity_filter_cols_str}
#             when matched then
#                 update set a.{tile_last_start_date_coulmn} = b.{tile_last_start_date_coulmn}
#             when not matched then
#                 insert ({entity_column_names}, {tile_last_start_date_coulmn})
#                     values ({entity_insert_cols_str}, b.{tile_last_start_date_coulmn})
#     """
#     print("merge_sql:", merge_sql)
#
#     # create table or insert new records or update existing records
#     if not tracking_table_exist:
#         spark.sql(f"create table {tracking_table_name} as SELECT * FROM {entity_table}")
#     else:
#         spark.sql(merge_sql)
