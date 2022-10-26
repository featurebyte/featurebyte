dbutils.widgets.text("TILE_ID", "")
dbutils.widgets.text("ENTITY_COLUMN_NAMES", "")
dbutils.widgets.text("ENTITY_TABLE", "")
dbutils.widgets.text("TILE_LAST_START_DATE_COLUMN", "")

tile_last_start_date_coulmn = dbutils.widgets.get("TILE_LAST_START_DATE_COLUMN")
entity_column_names = dbutils.widgets.get("ENTITY_COLUMN_NAMES")
tile_id = dbutils.widgets.get("TILE_ID")
entity_table = dbutils.widgets.get("ENTITY_TABLE")

print("tile_id: ", tile_id)
print("tile_last_start_date_coulmn: ", tile_last_start_date_coulmn)
print("entity_column_names: ", entity_column_names)
print("entity_table: ", entity_table)


tracking_table_name = tile_id + "_ENTITY_TRACKER"
tracking_table_exist = spark.catalog.tableExists(tracking_table_name)


entity_insert_cols = []
entity_filter_cols = []
for element in entity_column_names.split(","):
    element = element.strip()
    entity_insert_cols.append("b." + element)
    entity_filter_cols.append("a." + element + " = b." + element)

entity_insert_cols_str = ",".join(entity_insert_cols)
entity_filter_cols_str = " AND ".join(entity_filter_cols)

print("entity_insert_cols_str: ", entity_insert_cols_str)
print("entity_filter_cols_str: ", entity_filter_cols_str)


merge_sql = f"""
    merge into {tracking_table_name} a using ({entity_table}) b
        on {entity_filter_cols_str}
        when matched then
            update set a.{tile_last_start_date_coulmn} = b.{tile_last_start_date_coulmn}
        when not matched then
            insert ({entity_column_names}, {tile_last_start_date_coulmn})
                values ({entity_insert_cols_str}, b.{tile_last_start_date_coulmn})
"""

print("merge_sql:", merge_sql)

# create table or insert new records or update existing records
if not tracking_table_exist:
    spark.sql(f"create table {tracking_table_name} as SELECT * FROM {entity_table}")
else:
    spark.sql(merge_sql)
