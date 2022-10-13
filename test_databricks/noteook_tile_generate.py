# Define Parameters

dbutils.widgets.text("SQL", "")
dbutils.widgets.text("TILE_START_DATE_COLUMN", "")
dbutils.widgets.text("TILE_LAST_START_DATE_COLUMN", "")
dbutils.widgets.text("TIME_MODULO_FREQUENCY_SECOND", "")
dbutils.widgets.text("BLIND_SPOT_SECOND", "")
dbutils.widgets.text("FREQUENCY_MINUTE", "")
dbutils.widgets.text("ENTITY_COLUMN_NAMES", "")
dbutils.widgets.text("VALUE_COLUMN_NAMES", "")
dbutils.widgets.text("TILE_ID", "")
dbutils.widgets.text("TILE_TYPE", "")
dbutils.widgets.text("LAST_TILE_START_STR", "")

# Get Parameter Values
sql = dbutils.widgets.get("SQL")
tile_start_date_coulmn = dbutils.widgets.get("TILE_START_DATE_COLUMN")
tile_last_start_date_coulmn = dbutils.widgets.get("TILE_LAST_START_DATE_COLUMN")
tile_modulo_frequency_second = dbutils.widgets.get("TIME_MODULO_FREQUENCY_SECOND")
blind_spot_second = dbutils.widgets.get("BLIND_SPOT_SECOND")
frequency_minute = dbutils.widgets.get("FREQUENCY_MINUTE")
entity_column_names = dbutils.widgets.get("ENTITY_COLUMN_NAMES")
value_column_names = dbutils.widgets.get("VALUE_COLUMN_NAMES")
tile_id = dbutils.widgets.get("TILE_ID")
tile_type = dbutils.widgets.get("TILE_TYPE")
last_tile_start_str = dbutils.widgets.get("LAST_TILE_START_STR")

print("sql: ", sql)
print("tile_start_date_coulmn: ", tile_start_date_coulmn)
print("tile_last_start_date_coulmn: ", tile_last_start_date_coulmn)
print("tile_modulo_frequency_second: ", tile_modulo_frequency_second)
print("blind_spot_second: ", blind_spot_second)
print("frequency_minute: ", frequency_minute)
print("entity_column_names: ", entity_column_names)
print("value_column_names: ", value_column_names)
print("tile_id: ", tile_id)
print("tile_type: ", tile_type)
print("last_tile_start_str: ", last_tile_start_str)


tile_table_exist = spark.catalog.tableExists(tile_id)

tile_sql = f"""
select
            F_TIMESTAMP_TO_INDEX({tile_start_date_coulmn}, {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as index,
            {entity_column_names}, {value_column_names},
            current_timestamp() as created_at
        from ({sql})
"""

print("tile_sql:", tile_sql)

if not tile_table_exist:
    spark.sql(f"create table {tile_id} as {tile_sql}")


# add new columns to the tile table when there is new column
if tile_table_exist:
    existing_cols = {c.name for c in spark.catalog.listColumns(tile_id)}
    input_cols = set(value_column_names.split(","))
    new_cols = input_cols.difference(existing_cols)

    for new_c in new_cols:
        spark.sql(f"alter table {tile_id} add column {new_c} double")


entity_insert_cols = []
entity_filter_cols = []
for element in entity_column_names.split(","):
    element = element.strip()
    entity_insert_cols.append("b." + element)
    entity_filter_cols.append("a." + element + " = b." + element)

entity_insert_cols_str = ",".join(entity_insert_cols)
entity_filter_cols_str = " AND ".join(entity_filter_cols)

value_insert_cols = []
value_update_cols = []
for element in value_column_names.split(","):
    element = element.strip()
    value_insert_cols.append("b." + element)
    value_update_cols.append("a." + element + " = b." + element)

value_insert_cols_str = ",".join(value_insert_cols)
value_update_cols_str = ",".join(value_update_cols)

print("entity_insert_cols_str: ", entity_insert_cols_str)
print("entity_filter_cols_str: ", entity_filter_cols_str)
print("value_insert_cols_str: ", value_insert_cols_str)
print("value_update_cols_str: ", value_update_cols_str)


merge_sql = f"""
    merge into {tile_id} a using ({tile_sql}) b
        on a.index = b.index AND {entity_filter_cols_str}
        when matched then
            update set a.created_at = current_timestamp(), {value_update_cols_str}
        when not matched then
            insert (index, {entity_column_names}, {value_column_names}, created_at)
                values (b.index, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp())
"""

print("merge_sql:", merge_sql)


# insert new records and update existing records
if tile_table_exist:
    spark.sql(merge_sql)
